from __future__ import annotations

from dataclasses import dataclass
import imaplib
from typing import Iterable

from .config import ImapConfig


@dataclass
class ImapMessage:
    uid: str
    raw_rfc822: bytes


class ImapClient:
    def __init__(self, config: ImapConfig) -> None:
        self._config = config
        self._conn: imaplib.IMAP4 | imaplib.IMAP4_SSL | None = None
        self._current_uid_validity: str | None = None
        self._selected_folder: str | None = None

    def __enter__(self) -> "ImapClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def connect(self) -> None:
        if self._config.use_ssl:
            conn: imaplib.IMAP4 | imaplib.IMAP4_SSL = imaplib.IMAP4_SSL(
                self._config.host, self._config.port
            )
        else:
            conn = imaplib.IMAP4(self._config.host, self._config.port)

        conn.login(self._config.username, self._config.password)
        self._conn = conn

    def close(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.logout()
        except (imaplib.IMAP4.error, OSError):
            # Connection may already be closed/reset by the server.
            pass
        finally:
            self._conn = None
            self._current_uid_validity = None
            self._selected_folder = None

    @property
    def conn(self) -> imaplib.IMAP4 | imaplib.IMAP4_SSL:
        if self._conn is None:
            raise RuntimeError("IMAP connection is not open")
        return self._conn

    def ensure_folder_exists(self, folder_name: str) -> None:
        status, _ = self.conn.create(folder_name)
        if status == "OK":
            return

        # Many servers return NO when folder already exists. Validate via LIST.
        status, data = self.conn.list(pattern=f'"{folder_name}"')
        if status != "OK" or not data:
            raise RuntimeError(f"Could not create or verify folder '{folder_name}'")

    def select_folder(self, folder_name: str) -> int:
        status, data = self.conn.select(f'"{folder_name}"')
        if status != "OK":
            raise RuntimeError(f"Failed to select folder: {folder_name}")
        self._selected_folder = folder_name
        self._current_uid_validity = self._read_uid_validity()
        return int(data[0].decode() if data and data[0] else 0)

    @property
    def current_uid_validity(self) -> str | None:
        return self._current_uid_validity

    def _read_uid_validity(self) -> str | None:
        _, data = self.conn.response("UIDVALIDITY")
        if not data:
            return None

        raw = data[0]
        if isinstance(raw, bytes):
            value = raw.decode(errors="ignore").strip()
        elif isinstance(raw, str):
            value = raw.strip()
        else:
            value = str(raw).strip()

        return value or None

    def list_message_uids(self, *criteria: str) -> list[str]:
        if not criteria:
            criteria = ("ALL",)
        status, data = self.conn.uid("SEARCH", None, *criteria)
        if status != "OK":
            raise RuntimeError(f"UID SEARCH failed for criteria: {' '.join(criteria)}")
        if not data or not data[0]:
            return []
        return data[0].decode().split()

    def fetch_messages(self, uids: Iterable[str]) -> list[ImapMessage]:
        result: list[ImapMessage] = []
        for uid in uids:
            status, data = self.conn.uid("FETCH", uid, "(RFC822)")
            if status != "OK" or not data or not data[0]:
                continue
            payload = data[0]
            if isinstance(payload, tuple) and len(payload) >= 2:
                raw_bytes = payload[1]
                if isinstance(raw_bytes, bytes):
                    result.append(ImapMessage(uid=uid, raw_rfc822=raw_bytes))
        return result

    def move_uids(self, uids: Iterable[str], target_folder: str) -> int:
        moved = 0
        for uid in uids:
            status, _ = self.conn.uid("COPY", uid, f'"{target_folder}"')
            if status != "OK":
                continue
            status, _ = self.conn.uid("STORE", uid, "+FLAGS.SILENT", "(\\Deleted)")
            if status == "OK":
                moved += 1
        if moved > 0:
            self.conn.expunge()
        return moved

    def reconnect_if_needed(self) -> None:
        """Re-open IMAP connection when server closed an idle session."""
        try:
            self.conn.noop()
            return
        except (imaplib.IMAP4.abort, imaplib.IMAP4.error, OSError):
            pass

        selected_folder = self._selected_folder
        self.close()
        self.connect()
        if selected_folder:
            self.select_folder(selected_folder)
