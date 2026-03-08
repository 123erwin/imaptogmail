from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import os

from dotenv import load_dotenv


@dataclass(frozen=True)
class ImapConfig:
    host: str
    port: int
    username: str
    password: str
    use_ssl: bool
    source_folder: str
    move_to_folder: str | None
    create_target_folder: bool
    enable_move: bool
    date_from: date | None
    date_to: date | None


@dataclass(frozen=True)
class GmailConfig:
    credentials_file: Path
    token_file: Path
    import_source_folder: str
    labels: list[str]
    label_strategy: str
    label_mapping_file: Path
    enable_import: bool
    state_file: Path
    move_imported: bool
    imported_move_to_folder: str | None


@dataclass(frozen=True)
class LoggingConfig:
    log_file: Path
    log_level: str


@dataclass(frozen=True)
class AppConfig:
    imap: ImapConfig
    gmail: GmailConfig
    logging: LoggingConfig


def _get_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _split_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _get_optional_date(name: str) -> date | None:
    raw = os.getenv(name)
    if not raw:
        return None
    return date.fromisoformat(raw.strip())


def load_config() -> AppConfig:
    load_dotenv()

    imap = ImapConfig(
        host=_get_required("IMAP_HOST"),
        port=int(os.getenv("IMAP_PORT", "993")),
        username=_get_required("IMAP_USERNAME"),
        password=_get_required("IMAP_PASSWORD"),
        use_ssl=_get_bool("IMAP_USE_SSL", True),
        source_folder=os.getenv("IMAP_SOURCE_FOLDER", "INBOX"),
        move_to_folder=os.getenv("IMAP_MOVE_TO_FOLDER"),
        create_target_folder=_get_bool("IMAP_CREATE_TARGET_FOLDER", True),
        enable_move=_get_bool("STEP1_ENABLE_MOVE", True),
        date_from=_get_optional_date("IMAP_DATE_FROM"),
        date_to=_get_optional_date("IMAP_DATE_TO"),
    )

    gmail = GmailConfig(
        credentials_file=Path(os.getenv("GMAIL_CREDENTIALS_FILE", "credentials.json")),
        token_file=Path(os.getenv("GMAIL_TOKEN_FILE", "token.json")),
        import_source_folder=os.getenv("GMAIL_IMPORT_SOURCE_FOLDER", "INBOX"),
        labels=_split_csv(os.getenv("GMAIL_LABELS")),
        label_strategy=os.getenv("GMAIL_LABEL_STRATEGY", "env").strip().lower(),
        label_mapping_file=Path(os.getenv("LABEL_MAPPING_FILE", "label_mapping.json")),
        enable_import=_get_bool("GMAIL_ENABLE_IMPORT", True),
        state_file=Path(os.getenv("GMAIL_STATE_FILE", "state/imported_uids.json")),
        move_imported=_get_bool("GMAIL_MOVE_IMPORTED", True),
        imported_move_to_folder=os.getenv("GMAIL_IMPORTED_MOVE_TO_FOLDER"),
    )

    if gmail.label_strategy not in {"env", "folder_mapping"}:
        raise ValueError("GMAIL_LABEL_STRATEGY must be 'env' or 'folder_mapping'")

    if imap.date_from and imap.date_to and imap.date_from > imap.date_to:
        raise ValueError("IMAP_DATE_FROM cannot be later than IMAP_DATE_TO")

    logging_config = LoggingConfig(
        log_file=Path(os.getenv("LOG_FILE", "logs/imaptogmail.log")),
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
    )

    return AppConfig(imap=imap, gmail=gmail, logging=logging_config)
