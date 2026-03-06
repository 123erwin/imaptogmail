from __future__ import annotations

import json
from pathlib import Path


class ImportStateTracker:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: dict[str, list[str]] = self._load()

    def _load(self) -> dict[str, list[str]]:
        if not self._path.exists():
            return {}
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        if not isinstance(raw, dict):
            return {}

        data: dict[str, list[str]] = {}
        for folder, uids in raw.items():
            if isinstance(folder, str) and isinstance(uids, list):
                clean_uids = [uid for uid in uids if isinstance(uid, str)]
                data[folder] = clean_uids
        return data

    def is_imported(self, folder: str, uid: str) -> bool:
        return uid in set(self._data.get(folder, []))

    def mark_imported(self, folder: str, uid: str) -> None:
        folder_uids = self._data.setdefault(folder, [])
        if uid in folder_uids:
            return
        folder_uids.append(uid)
        self._save()

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(self._data, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
