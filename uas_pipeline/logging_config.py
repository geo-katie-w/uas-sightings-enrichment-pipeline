"""Logging setup utilities."""
from __future__ import annotations

import logging
import os

def configure_logging() -> logging.Logger:
    debug_mode = os.getenv('FAA_PIPELINE_DEBUG', 'false').lower() == 'true'
    logging.basicConfig(
        level=logging.DEBUG if debug_mode else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)
