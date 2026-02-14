"""Configuration models and defaults."""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

@dataclass(frozen=True)
class Config:
    data_path: Path
    run_date: str
    max_file_size_mb: int = 100
    max_text_length: int = 50000
    regex_timeout_seconds: int = 2
    rows_per_split: int = 250
    max_retry_attempts: int = 3
    retry_delay_base_seconds: int = 30

    @property
    def max_file_size_bytes(self) -> int:
        return self.max_file_size_mb * 1024 * 1024

    @staticmethod
    def default_data_path() -> Path:
        return Path("C:/Documents/FAA_UAS_Sightings")

    @classmethod
    def from_env(cls, run_date: str) -> "Config":
        data_path = Path(os.getenv("FAA_DATA_PATH") or cls.default_data_path())
        return cls(data_path=data_path, run_date=run_date)
