from __future__ import annotations

import base64
from pathlib import Path
from typing import Sequence

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


SCOPES = ["https://mail.google.com/"]
SYSTEM_LABELS = {
    "inbox": "INBOX",
    "sent": "SENT",
    "draft": "DRAFT",
    "spam": "SPAM",
    "trash": "TRASH",
    "important": "IMPORTANT",
    "starred": "STARRED",
    "unread": "UNREAD",
    "category_personal": "CATEGORY_PERSONAL",
    "category_social": "CATEGORY_SOCIAL",
    "category_promotions": "CATEGORY_PROMOTIONS",
    "category_updates": "CATEGORY_UPDATES",
    "category_forums": "CATEGORY_FORUMS",
}


class GmailImporter:
    def __init__(self, credentials_file: Path, token_file: Path) -> None:
        self._credentials_file = credentials_file
        self._token_file = token_file
        self._service = None
        self._label_cache: dict[str, str] = {}

    def connect(self) -> None:
        creds: Credentials | None = None
        if self._token_file.exists():
            creds = Credentials.from_authorized_user_file(str(self._token_file), SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(self._credentials_file), SCOPES
                )
                creds = flow.run_local_server(port=0)
            self._token_file.write_text(creds.to_json(), encoding="utf-8")

        self._service = build("gmail", "v1", credentials=creds, cache_discovery=False)

    @property
    def service(self):
        if self._service is None:
            raise RuntimeError("Gmail service is not connected")
        return self._service

    def _load_labels(self) -> dict[str, str]:
        labels = (
            self.service.users()
            .labels()
            .list(userId="me")
            .execute()
            .get("labels", [])
        )
        self._label_cache = {label["name"]: label["id"] for label in labels}
        return self._label_cache

    def resolve_label_ids(self, label_names: Sequence[str]) -> list[str]:
        if not label_names:
            return []

        if not self._label_cache:
            self._load_labels()

        ids: list[str] = []
        for name in label_names:
            normalized_name = SYSTEM_LABELS.get(name.strip().lower(), name)
            if normalized_name in self._label_cache:
                ids.append(self._label_cache[normalized_name])
                continue

            created = (
                self.service.users()
                .labels()
                .create(
                    userId="me",
                    body={
                        "name": normalized_name,
                        "labelListVisibility": "labelShow",
                        "messageListVisibility": "show",
                    },
                )
                .execute()
            )
            label_id = created["id"]
            self._label_cache[normalized_name] = label_id
            ids.append(label_id)
        return ids

    def import_rfc822(self, raw_rfc822: bytes, label_ids: Sequence[str]) -> str:
        encoded = base64.urlsafe_b64encode(raw_rfc822).decode("utf-8")
        body = {"raw": encoded}
        if label_ids:
            body["labelIds"] = list(label_ids)

        try:
            result = (
                self.service.users()
                .messages()
                .import_(userId="me", body=body, internalDateSource="dateHeader")
                .execute()
            )
            return result["id"]
        except HttpError as exc:
            raise RuntimeError(f"Gmail import failed: {exc}") from exc
