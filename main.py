from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
import logging
from threading import Lock
import time
from typing import Callable, Iterable, Sequence, TypeVar

from imap_to_gmail.config import AppConfig, load_config
from imap_to_gmail.gmail_importer import GmailImporter
from imap_to_gmail.imap_client import ImapClient, ImapMessage
from imap_to_gmail.logging_setup import setup_logging
from imap_to_gmail.mapping import load_label_mapping
from imap_to_gmail.state_tracker import ImportStateTracker

logger = logging.getLogger("imap_to_gmail")

T = TypeVar("T")


def _labels_for_folder(config: AppConfig, folder_name: str) -> list[str]:
    if config.gmail.label_strategy == "env":
        return config.gmail.labels

    mapping = load_label_mapping(config.gmail.label_mapping_file)
    if folder_name in mapping:
        return mapping[folder_name]

    # Common IMAP folder formats: "INBOX/Sub" or "INBOX.Sub"
    tail = folder_name.split("/")[-1].split(".")[-1]
    return mapping.get(tail, [])


def _folder_separator(source_folder: str) -> str:
    # Reuse the source folder style so subfolders match server hierarchy style.
    if "/" in source_folder:
        return "/"
    if "." in source_folder:
        return "."
    return "."


def _build_imported_target_folder(source_folder: str, subfolder_name: str) -> str:
    separator = _folder_separator(source_folder)
    cleaned_subfolder = subfolder_name.strip().strip("./")
    return f"{source_folder}{separator}{cleaned_subfolder}"


def _build_date_search_criteria(config: AppConfig) -> list[str]:
    criteria: list[str] = ["ALL"]
    if config.imap.date_from:
        criteria.extend(["SINCE", config.imap.date_from.strftime("%d-%b-%Y")])
    if config.imap.date_to:
        # IMAP BEFORE is exclusive, so we add 1 day to include date_to itself.
        day_after = config.imap.date_to + timedelta(days=1)
        criteria.extend(["BEFORE", day_after.strftime("%d-%b-%Y")])
    return criteria


def _chunked(items: Sequence[str], chunk_size: int) -> Iterable[list[str]]:
    if chunk_size <= 0:
        chunk_size = 1
    for start in range(0, len(items), chunk_size):
        yield list(items[start : start + chunk_size])


def _run_with_retries(
    action: Callable[[], T],
    description: str,
    retries: int,
    retry_delay_seconds: float,
    reconnect: Callable[[], None] | None = None,
) -> T:
    for attempt in range(1, retries + 1):
        try:
            return action()
        except Exception as exc:
            if attempt == retries:
                raise RuntimeError(
                    f"{description} failed after {retries} attempts: {exc}"
                ) from exc
            logger.warning(
                "%s failed (attempt %s/%s): %s. Retrying in %.1fs.",
                description,
                attempt,
                retries,
                exc,
                retry_delay_seconds,
            )
            if reconnect is not None:
                try:
                    reconnect()
                except Exception as reconnect_exc:
                    logger.warning("Reconnect attempt failed: %s", reconnect_exc)
            time.sleep(retry_delay_seconds)


def run_step1(config: AppConfig) -> None:
    if not config.imap.enable_move:
        logger.info("Step 1 skipped: STEP1_ENABLE_MOVE=false.")
        return

    target_folder = config.imap.move_to_folder
    if not target_folder:
        logger.info("Step 1 skipped: IMAP_MOVE_TO_FOLDER not set.")
        return

    with ImapClient(config.imap) as client:
        source_count = client.select_folder(config.imap.source_folder)
        logger.info(
            "Selected source folder '%s' (%s messages).",
            config.imap.source_folder,
            source_count,
        )

        if config.imap.create_target_folder:
            client.ensure_folder_exists(target_folder)
            logger.info("Ensured target folder exists: '%s'.", target_folder)

        search_criteria = _build_date_search_criteria(config)
        logger.info("Step 1 using IMAP search criteria: %s", " ".join(search_criteria))
        uids = client.list_message_uids(*search_criteria)
        moved = client.move_uids(uids, target_folder)
        logger.info(
            "Moved %s message(s) from '%s' to '%s'.",
            moved,
            config.imap.source_folder,
            target_folder,
        )


def _import_chunk(
    credentials_file,
    token_file,
    chunk: list[ImapMessage],
    label_ids: list[str],
    retries: int,
    retry_delay_seconds: float,
) -> tuple[list[str], list[tuple[str, str]]]:
    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []

    importer = GmailImporter(credentials_file, token_file)
    try:
        _run_with_retries(
            importer.connect,
            "Gmail connect",
            retries,
            retry_delay_seconds,
        )
    except RuntimeError as exc:
        message = str(exc)
        return [], [(item.uid, message) for item in chunk]

    for item in chunk:
        try:
            _run_with_retries(
                lambda: importer.import_rfc822(item.raw_rfc822, label_ids),
                f"Gmail import UID {item.uid}",
                retries,
                retry_delay_seconds,
                reconnect=importer.connect,
            )
        except RuntimeError as exc:
            failed.append((item.uid, str(exc)))
            continue
        succeeded.append(item.uid)

    return succeeded, failed


def run_step2(config: AppConfig) -> None:
    if not config.gmail.enable_import:
        logger.info("Step 2 skipped: GMAIL_ENABLE_IMPORT=false.")
        return

    importer = GmailImporter(config.gmail.credentials_file, config.gmail.token_file)
    importer.connect()

    tracker = ImportStateTracker(config.gmail.state_file)
    tracker_lock = Lock()
    total_imported = 0
    total_skipped = 0
    total_moved = 0
    total_failed = 0

    with ImapClient(config.imap) as client:
        for source_folder in config.gmail.import_source_folders:
            try:
                labels = _labels_for_folder(config, source_folder)
                label_ids = importer.resolve_label_ids(labels)

                count = _run_with_retries(
                    lambda: client.select_folder(source_folder),
                    f"Select IMAP folder '{source_folder}'",
                    config.gmail.operation_retries,
                    config.gmail.retry_delay_seconds,
                    reconnect=client.reconnect_if_needed,
                )
                uid_validity = client.current_uid_validity
                logger.info(
                    "Selected import folder '%s' (%s messages).",
                    source_folder,
                    count,
                )
                if uid_validity:
                    logger.info(
                        "Using IMAP UIDVALIDITY '%s' for state tracking.", uid_validity
                    )
                else:
                    logger.warning(
                        "Could not read IMAP UIDVALIDITY; state tracking will fall back to plain UIDs."
                    )

                search_criteria = _build_date_search_criteria(config)
                logger.info(
                    "Step 2 using IMAP search criteria for '%s': %s",
                    source_folder,
                    " ".join(search_criteria),
                )
                uids = _run_with_retries(
                    lambda: client.list_message_uids(*search_criteria),
                    f"List UIDs in folder '{source_folder}'",
                    config.gmail.operation_retries,
                    config.gmail.retry_delay_seconds,
                    reconnect=client.reconnect_if_needed,
                )
                pending_uids = [
                    uid
                    for uid in uids
                    if not tracker.is_imported(source_folder, uid, uid_validity)
                ]
                pending_uid_set = set(pending_uids)
                already_imported_uids = [uid for uid in uids if uid not in pending_uid_set]
                skipped = len(already_imported_uids)

                logger.info(
                    "Folder '%s': %s pending import, %s already imported.",
                    source_folder,
                    len(pending_uids),
                    skipped,
                )

                imported = 0
                failed = 0
                moved = 0

                target_folder: str | None = None
                if config.gmail.move_imported and config.gmail.imported_move_to_folder:
                    target_folder = _build_imported_target_folder(
                        source_folder, config.gmail.imported_move_to_folder
                    )
                    _run_with_retries(
                        lambda: client.ensure_folder_exists(target_folder),
                        f"Ensure target folder '{target_folder}'",
                        config.gmail.operation_retries,
                        config.gmail.retry_delay_seconds,
                        reconnect=client.reconnect_if_needed,
                    )

                processed = 0
                for uid_batch in _chunked(pending_uids, config.gmail.fetch_batch_size):
                    messages = _run_with_retries(
                        lambda: client.fetch_messages(uid_batch),
                        f"Fetch IMAP batch in folder '{source_folder}'",
                        config.gmail.operation_retries,
                        config.gmail.retry_delay_seconds,
                        reconnect=client.reconnect_if_needed,
                    )

                    if not messages:
                        processed += len(uid_batch)
                        continue

                    if config.gmail.import_workers <= 1 or len(messages) <= 1:
                        succeeded_uids: list[str] = []
                        failed_items: list[tuple[str, str]] = []
                        for item in messages:
                            try:
                                _run_with_retries(
                                    lambda: importer.import_rfc822(item.raw_rfc822, label_ids),
                                    f"Gmail import UID {item.uid}",
                                    config.gmail.operation_retries,
                                    config.gmail.retry_delay_seconds,
                                    reconnect=importer.connect,
                                )
                            except RuntimeError as exc:
                                failed_items.append((item.uid, str(exc)))
                                continue
                            succeeded_uids.append(item.uid)
                    else:
                        worker_count = min(config.gmail.import_workers, len(messages))
                        chunk_size = max(1, (len(messages) + worker_count - 1) // worker_count)
                        message_chunks = [
                            messages[i : i + chunk_size]
                            for i in range(0, len(messages), chunk_size)
                        ]
                        succeeded_uids = []
                        failed_items = []
                        with ThreadPoolExecutor(max_workers=worker_count) as executor:
                            futures = [
                                executor.submit(
                                    _import_chunk,
                                    config.gmail.credentials_file,
                                    config.gmail.token_file,
                                    message_chunk,
                                    label_ids,
                                    config.gmail.operation_retries,
                                    config.gmail.retry_delay_seconds,
                                )
                                for message_chunk in message_chunks
                            ]
                            for future in as_completed(futures):
                                try:
                                    success_part, failed_part = future.result()
                                except Exception as exc:
                                    logger.warning(
                                        "Folder '%s': worker crashed during import: %s",
                                        source_folder,
                                        exc,
                                    )
                                    continue
                                succeeded_uids.extend(success_part)
                                failed_items.extend(failed_part)

                    for uid in succeeded_uids:
                        with tracker_lock:
                            tracker.mark_imported(source_folder, uid, uid_validity)
                    imported += len(succeeded_uids)

                    for uid, error_message in failed_items:
                        failed += 1
                        logger.warning(
                            "Folder '%s': failed to import UID %s; skipping this message. Error: %s",
                            source_folder,
                            uid,
                            error_message,
                        )

                    if target_folder and succeeded_uids:
                        moved += _run_with_retries(
                            lambda: client.move_uids(succeeded_uids, target_folder),
                            f"Move imported messages to '{target_folder}'",
                            config.gmail.operation_retries,
                            config.gmail.retry_delay_seconds,
                            reconnect=client.reconnect_if_needed,
                        )

                    processed += len(uid_batch)
                    logger.info(
                        "Folder '%s': progress %s/%s messages processed (ok=%s, failed=%s, moved=%s).",
                        source_folder,
                        processed,
                        len(pending_uids),
                        imported,
                        failed,
                        moved,
                    )

                if target_folder and already_imported_uids:
                    for uid_batch in _chunked(
                        already_imported_uids, config.gmail.fetch_batch_size
                    ):
                        moved += _run_with_retries(
                            lambda uids=uid_batch: client.move_uids(uids, target_folder),
                            f"Move already-imported messages to '{target_folder}'",
                            config.gmail.operation_retries,
                            config.gmail.retry_delay_seconds,
                            reconnect=client.reconnect_if_needed,
                        )

                total_imported += imported
                total_skipped += skipped
                total_moved += moved
                total_failed += failed

                logger.info("Folder '%s': imported %s message(s).", source_folder, imported)
                if skipped:
                    logger.info(
                        "Folder '%s': skipped %s already imported message(s).",
                        source_folder,
                        skipped,
                    )
                if failed:
                    logger.warning(
                        "Folder '%s': failed to import %s message(s).",
                        source_folder,
                        failed,
                    )
                if target_folder:
                    logger.info(
                        "Folder '%s': moved %s message(s) to '%s'.",
                        source_folder,
                        moved,
                        target_folder,
                    )
                elif config.gmail.move_imported and not config.gmail.imported_move_to_folder:
                    logger.info(
                        "Folder '%s': move-on-success is enabled, but no target folder is configured.",
                        source_folder,
                    )

                if labels:
                    logger.info(
                        "Folder '%s': applied labels: %s",
                        source_folder,
                        ", ".join(labels),
                    )
                else:
                    logger.info("Folder '%s': no labels applied.", source_folder)
            except Exception as exc:
                total_failed += 1
                logger.exception(
                    "Folder '%s': unexpected error, continuing with next folder. Error: %s",
                    source_folder,
                    exc,
                )

    logger.info("Imported %s message(s) into Gmail.", total_imported)
    if total_skipped:
        logger.info(
            "Skipped %s already imported message(s) based on state file.", total_skipped
        )
    if total_failed:
        logger.warning(
            "Failed to import %s message(s). See warnings above for UIDs/errors.",
            total_failed,
        )
    logger.info("State file: %s", config.gmail.state_file)
    if config.gmail.move_imported and config.gmail.imported_move_to_folder:
        logger.info(
            "Moved %s imported message(s) to '%s'.",
            total_moved,
            config.gmail.imported_move_to_folder,
        )
    elif config.gmail.move_imported and not config.gmail.imported_move_to_folder:
        logger.info("Move-on-success is enabled, but no target folder is configured.")
    else:
        logger.info("Move-on-success is disabled.")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Migrate email from IMAP to Gmail API import")
    parser.add_argument(
        "mode",
        choices=["step1", "step2", "all"],
        help="step1: move IMAP messages, step2: import to Gmail, all: run both in order",
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    config = load_config()
    setup_logging(config.logging.log_file, config.logging.log_level)
    logger.info("Started mode '%s'. Log file: %s", args.mode, config.logging.log_file)

    if args.mode == "step1":
        run_step1(config)
    elif args.mode == "step2":
        run_step2(config)
    else:
        run_step1(config)
        run_step2(config)

    logger.info("Completed mode '%s'.", args.mode)


if __name__ == "__main__":
    main()
