from __future__ import annotations

import argparse
from datetime import timedelta
import logging

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


def run_step2(config: AppConfig) -> None:
    if not config.gmail.enable_import:
        logger.info("Step 2 skipped: GMAIL_ENABLE_IMPORT=false.")
        return

    importer = GmailImporter(config.gmail.credentials_file, config.gmail.token_file)
    importer.connect()

    labels = _labels_for_folder(config, config.gmail.import_source_folder)
    label_ids = importer.resolve_label_ids(labels)
    tracker = ImportStateTracker(config.gmail.state_file)

    with ImapClient(config.imap) as client:
        count = client.select_folder(config.gmail.import_source_folder)
        uid_validity = client.current_uid_validity
        logger.info(
            "Selected import folder '%s' (%s messages).",
            config.gmail.import_source_folder,
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
        logger.info("Step 2 using IMAP search criteria: %s", " ".join(search_criteria))
        uids = client.list_message_uids(*search_criteria)
        pending_uids = [
            uid
            for uid in uids
            if not tracker.is_imported(
                config.gmail.import_source_folder, uid, uid_validity
            )
        ]
        pending_uid_set = set(pending_uids)
        already_imported_uids = [uid for uid in uids if uid not in pending_uid_set]
        skipped = len(already_imported_uids)
        messages = client.fetch_messages(pending_uids)

        imported = 0
        successful_uids: list[str] = []
        for item in messages:
            importer.import_rfc822(item.raw_rfc822, label_ids)
            tracker.mark_imported(
                config.gmail.import_source_folder, item.uid, uid_validity
            )
            successful_uids.append(item.uid)
            imported += 1

        moved = 0
        if config.gmail.move_imported and config.gmail.imported_move_to_folder:
            if config.gmail.imported_move_to_folder != config.gmail.import_source_folder:
                # Gmail import can take long enough for some IMAP servers to drop idle sessions.
                client.reconnect_if_needed()
                client.ensure_folder_exists(config.gmail.imported_move_to_folder)
                move_candidates = [*successful_uids, *already_imported_uids]
                moved = client.move_uids(
                    move_candidates, config.gmail.imported_move_to_folder
                )
            else:
                logger.warning(
                    "GMAIL_IMPORTED_MOVE_TO_FOLDER equals source folder; skipping move."
                )

    logger.info("Imported %s message(s) into Gmail.", imported)
    if skipped:
        logger.info("Skipped %s already imported message(s) based on state file.", skipped)
    logger.info("State file: %s", config.gmail.state_file)
    if config.gmail.move_imported and config.gmail.imported_move_to_folder:
        logger.info(
            "Moved %s imported message(s) to '%s'.",
            moved,
            config.gmail.imported_move_to_folder,
        )
    elif config.gmail.move_imported and not config.gmail.imported_move_to_folder:
        logger.info("Move-on-success is enabled, but no target folder is configured.")
    else:
        logger.info("Move-on-success is disabled.")

    if labels:
        logger.info("Applied labels: %s", ", ".join(labels))
    else:
        logger.info("No labels applied.")


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
