"""Input/output helpers."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


def list_input_files(folder_path: Path) -> List[Path]:
    files = list(folder_path.glob('*.csv')) + list(folder_path.glob('*.xlsx'))
    logger.info("Found %s input file(s) in %s", len(files), folder_path)
    return files


def ensure_output_dirs(paths: List[Path]) -> None:
    for folder in paths:
        folder.mkdir(parents=True, exist_ok=True)
