from __future__ import annotations

from pathlib import Path
import json


def load_label_mapping(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    mapping: dict[str, list[str]] = {}
    for folder_name, labels in data.items():
        if isinstance(labels, str):
            mapping[folder_name] = [labels]
        else:
            mapping[folder_name] = [str(item).strip() for item in labels if str(item).strip()]
    return mapping

