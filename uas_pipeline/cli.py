"""CLI entry point."""
from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime
from .config import Config
from .pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="UAS Sighting Enrichment Pipeline")
    parser.add_argument("--data-path", dest="data_path", help="Folder containing FAA files")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    run_date = datetime.now().strftime("%Y-%m-%d")
    config = Config.from_env(run_date)
    if args.data_path:
        config = Config(data_path=Path(args.data_path), run_date=run_date)
    run_pipeline(config.data_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
