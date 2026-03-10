from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
import logging
from threading import Lock

from imap_to_gmail.config import AppConfig, load_config
from imap_to_gmail.gmail_importer import GmailImporter
from imap_to_gmail.imap_client import ImapClient
from imap_to_gmail.logging_setup import setup_logging
from imap_to_gmail.mapping import load_label_mapping
from imap_to_gmail.state_tracker import ImportStateTracker

logger = logging.getLogger("imap_to_gmail")


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
    chunk,
    label_ids: list[str],
) -> tuple[list[str], list[tuple[str, str]]]:
    importer = GmailImporter(credentials_file, token_file)
    importer.connect()
    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []
    for item in chunk:
        try:
            importer.import_rfc822(item.raw_rfc822, label_ids)
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
            labels = _labels_for_folder(config, source_folder)
            label_ids = importer.resolve_label_ids(labels)

            count = client.select_folder(source_folder)
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
            uids = client.list_message_uids(*search_criteria)
            pending_uids = [
                uid
                for uid in uids
                if not tracker.is_imported(source_folder, uid, uid_validity)
            ]
            pending_uid_set = set(pending_uids)
            already_imported_uids = [uid for uid in uids if uid not in pending_uid_set]
            skipped = len(already_imported_uids)
            messages = client.fetch_messages(pending_uids)
            total_to_import = len(messages)
            if total_to_import:
                logger.info(
                    "Folder '%s': starting Gmail import for %s message(s).",
                    source_folder,
                    total_to_import,
                )
            else:
                logger.info(
                    "Folder '%s': no new messages to import after state filtering.",
                    source_folder,
                )

            imported = 0
            failed = 0
            successful_uids: list[str] = []

            if config.gmail.import_workers <= 1 or total_to_import <= 1:
                for index, item in enumerate(messages, start=1):
                    try:
                        importer.import_rfc822(item.raw_rfc822, label_ids)
                    except RuntimeError as exc:
                        failed += 1
                        logger.warning(
                            "Folder '%s': failed to import UID %s; skipping this message. Error: %s",
                            source_folder,
                            item.uid,
                            exc,
                        )
                        continue

                    tracker.mark_imported(source_folder, item.uid, uid_validity)
                    successful_uids.append(item.uid)
                    imported += 1
                    if index % 10 == 0 or index == total_to_import:
                        logger.info(
                            "Folder '%s': Gmail import progress %s/%s (ok=%s, failed=%s).",
                            source_folder,
                            index,
                            total_to_import,
                            imported,
                            failed,
                        )
            else:
                logger.info(
                    "Folder '%s': importing with %s parallel worker(s).",
                    source_folder,
                    config.gmail.import_workers,
                )
                worker_count = min(config.gmail.import_workers, total_to_import)
                chunk_size = max(1, (total_to_import + worker_count - 1) // worker_count)
                chunks = [messages[i : i + chunk_size] for i in range(0, total_to_import, chunk_size)]

                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    futures = [
                        executor.submit(
                            _import_chunk,
                            config.gmail.credentials_file,
                            config.gmail.token_file,
                            chunk,
                            label_ids,
                        )
                        for chunk in chunks
                    ]

                    processed = 0
                    for future in as_completed(futures):
                        succeeded_uids, failed_items = future.result()
                        processed += len(succeeded_uids) + len(failed_items)

                        for uid in succeeded_uids:
                            with tracker_lock:
                                tracker.mark_imported(source_folder, uid, uid_validity)
                            successful_uids.append(uid)
                            imported += 1

                        for uid, error_message in failed_items:
                            failed += 1
                            logger.warning(
                                "Folder '%s': failed to import UID %s; skipping this message. Error: %s",
                                source_folder,
                                uid,
                                error_message,
                            )

                        if processed % 10 == 0 or processed == total_to_import:
                            logger.info(
                                "Folder '%s': Gmail import progress %s/%s (ok=%s, failed=%s).",
                                source_folder,
                                processed,
                                total_to_import,
                                imported,
                                failed,
                            )

            moved = 0
            if config.gmail.move_imported and config.gmail.imported_move_to_folder:
                target_folder = _build_imported_target_folder(
                    source_folder, config.gmail.imported_move_to_folder
                )
                # Gmail import can take long enough for some IMAP servers to drop idle sessions.
                client.reconnect_if_needed()
                client.ensure_folder_exists(target_folder)
                move_candidates = [*successful_uids, *already_imported_uids]
                moved = client.move_uids(move_candidates, target_folder)

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
            if config.gmail.move_imported and config.gmail.imported_move_to_folder:
                logger.info(
                    "Folder '%s': moved %s imported message(s) to '%s'.",
                    source_folder,
                    moved,
                    _build_imported_target_folder(
                        source_folder, config.gmail.imported_move_to_folder
                    ),
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
