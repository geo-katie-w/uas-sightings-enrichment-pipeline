"""Pipeline orchestration."""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
from geopy.geocoders import Nominatim

from .airports import load_airports
from .config import Config
from .extractors import (
    DEFAULT_AIRPORT_BLACKLIST,
    DEFAULT_STATE_ABBREV,
    extract_airport_code,
    extract_details,
    extract_leo_agency,
    get_best_col,
    normalize_state,
    standardize_value,
)
from .geocode import find_nearest_airport, load_cache_safely, save_cache_with_validation
from .io_utils import ensure_output_dirs, list_input_files
from .logging_config import configure_logging
from .security import SecurityError, validate_folder_path


def run_pipeline(data_path: Path) -> None:
    """Run the FAA UAS pipeline using the refactored module structure."""
    configure_logging()
    logger = logging.getLogger(__name__)

    run_date = datetime.now().strftime("%Y-%m-%d")
    config = Config(data_path=data_path, run_date=run_date)

    split_folder = config.data_path / "Split_Chunks" / config.run_date
    output_folder = config.data_path / "Processed_Files" / config.run_date
    yearly_folder = config.data_path / "Yearly_Masters"
    cache_file = config.data_path / "geocoding_cache.json"

    lon_min, lon_max = -125, -65
    lat_min, lat_max = 25, 50

    try:
        validate_folder_path(config.data_path)
    except SecurityError as exc:
        logger.error("Security validation failed: %s", exc)
        raise

    ensure_output_dirs([split_folder, output_folder, yearly_folder])

    geolocator = Nominatim(user_agent="faa_uas_precision_v8")
    city_cache: Dict[str, str] = load_cache_safely(cache_file)

    us_airports, icao_airports, icao_to_iata = load_airports()

    if us_airports:
        sample_code = list(us_airports.keys())[0]
        logger.debug("Sample IATA entry (%s): %s", sample_code, us_airports[sample_code])
    if icao_airports:
        sample_icao = [k for k in icao_airports.keys() if k.startswith('K')][0]
        logger.debug("Sample ICAO entry (%s): %s", sample_icao, icao_airports[sample_icao])
    logger.info("Loaded %s US IATA airports and %s ICAO mappings", len(us_airports), len(icao_to_iata))

    def get_airport_coords(code: str, coord_type: str):
        if not code or code in ['UNKNOWN', 'GEO_TIMEOUT']:
            return None

        if code in us_airports:
            coords = us_airports[code]
            return coords.get(coord_type)

        k_code = f'K{code}'
        if k_code in icao_airports and icao_airports[k_code].get('country') == 'US':
            coords = icao_airports[k_code]
            return coords.get(coord_type)

        return None

    def phase_1_split() -> None:
        all_files = list_input_files(config.data_path)
        for file_path in all_files:
            if "Enriched_" in file_path.name or "Split_Chunks" in str(file_path):
                continue

            try:
                file_size = file_path.stat().st_size
                if file_size > config.max_file_size_bytes:
                    logger.error(
                        "File %s exceeds size limit (%.1fMB > %sMB), skipping",
                        file_path.name,
                        file_size / 1024 / 1024,
                        config.max_file_size_mb,
                    )
                    continue

                logger.info("Loading: %s (%.1fMB)", file_path.name, file_size / 1024 / 1024)
                df = pd.read_excel(file_path) if file_path.suffix == '.xlsx' else pd.read_csv(
                    file_path, encoding='utf-8', on_bad_lines='warn'
                )

                df = df.dropna(axis=1, how='all')
                df = df.loc[:, ~df.columns.str.contains('^Unnamed|^Column', case=False, na=False)]

                sum_c = get_best_col(df, ['summary', 'narrative', 'description', 'remarks', 'event'])
                logger.info("  Target Column identified as: [%s]", sum_c)

                for i in range(0, len(df), config.rows_per_split):
                    chunk = df.iloc[i: i + config.rows_per_split]
                    chunk.to_csv(split_folder / f"{file_path.stem}_part_{i // config.rows_per_split + 1}.csv", index=False)
            except Exception as exc:
                logger.error("Error processing %s: %s", file_path.name, exc)
                continue

    def phase_2_enrich() -> None:
        split_files = sorted(list(split_folder.glob('*.csv')))
        total_files = len(split_files)

        for idx, file_path in enumerate(split_files, 1):
            out_file = output_folder / f"Enriched_{file_path.name}"
            if out_file.exists():
                logger.info("[%s/%s] Skipping %s (already processed)", idx, total_files, file_path.name)
                continue

            logger.info("[%s/%s] Processing: %s", idx, total_files, file_path.name)
            df = pd.read_csv(file_path)
            sum_c = get_best_col(df, ['summary', 'narrative', 'description', 'remarks', 'event'])
            city_c = get_best_col(df, ['city', 'location', 'town'])
            stat_c = get_best_col(df, ['state', 'province'])

            if not sum_c:
                logger.warning("  No summary column found, skipping file")
                continue
            if not city_c or not stat_c:
                logger.warning("  Missing city/state columns - geocoding may fail")

            df[['Acft_Type', 'UAS_Color', 'Alt_Ft', 'Evasive']] = df[sum_c].apply(
                lambda text: extract_details(text, config.max_text_length, config.regex_timeout_seconds)
            )
            df['LEO_Agency'] = df[sum_c].apply(lambda text: extract_leo_agency(text, config.max_text_length))

            df['Assigned_Airport'] = df[sum_c].apply(
                lambda text: extract_airport_code(
                    text,
                    config.max_text_length,
                    us_airports,
                    icao_to_iata,
                    DEFAULT_AIRPORT_BLACKLIST,
                )
            )

            missing_mask = df['Assigned_Airport'].isna()
            if missing_mask.any():
                logger.info(
                    "  Extracted %s airport codes from text, geocoding %s remaining...",
                    (~missing_mask).sum(),
                    missing_mask.sum(),
                )
                df.loc[missing_mask, 'Assigned_Airport'] = df[missing_mask].apply(
                    lambda r: find_nearest_airport(
                        r.get(city_c),
                        r.get(stat_c),
                        lambda s: normalize_state(s, DEFAULT_STATE_ABBREV),
                        city_cache,
                        geolocator,
                        us_airports,
                        config.max_retry_attempts,
                        config.retry_delay_base_seconds,
                    ),
                    axis=1,
                )
                save_cache_with_validation(cache_file, city_cache)
            else:
                logger.info("  Extracted all %s airport codes from text (no geocoding needed)", len(df))

            df['Airport_Longitude'] = df['Assigned_Airport'].apply(lambda code: get_airport_coords(code, 'lon'))
            df['Airport_Latitude'] = df['Assigned_Airport'].apply(lambda code: get_airport_coords(code, 'lat'))

            coords_populated = df['Airport_Longitude'].notna().sum()
            logger.debug("  Coordinates populated for %s/%s records", coords_populated, len(df))

            valid_coords = df[
                (df['Airport_Longitude'].notna())
                & (df['Airport_Latitude'].notna())
                & (df['Airport_Longitude'].between(lon_min, lon_max))
                & (df['Airport_Latitude'].between(lat_min, lat_max))
            ]
            logger.info("  Valid coordinates: %s/%s records", len(valid_coords), len(df))

            df.to_csv(out_file, index=False)
            time.sleep(5)

    def phase_3_consolidate_by_year() -> None:
        processed_parent = config.data_path / "Processed_Files"
        all_parts = list(processed_parent.glob("**/Enriched_*.csv"))

        if not all_parts:
            logger.warning("No enriched files found to consolidate")
            return

        years = set(
            re.search(r'20\d{2}', f.name).group()
            for f in all_parts
            if re.search(r'20\d{2}', f.name)
        )
        for year in years:
            year_parts = [f for f in all_parts if year in f.name]
            logger.info("Consolidating %s files for %s...", len(year_parts), year)

            combined = pd.concat([pd.read_csv(f) for f in year_parts], ignore_index=True)

            for col in combined.columns:
                combined[col] = combined[col].apply(standardize_value)

            original_count = len(combined)
            combined = combined.drop_duplicates()
            exact_dupes = original_count - len(combined)

            date_col = get_best_col(combined, ['date', 'event_date', 'sighting_date', 'occurred'])
            city_col = get_best_col(combined, ['city', 'location', 'town'])

            if date_col and city_col and 'Alt_Ft' in combined.columns:
                pre_smart_count = len(combined)
                combined = combined.drop_duplicates(subset=[date_col, city_col, 'Alt_Ft'], keep='first')
                smart_dupes = pre_smart_count - len(combined)
                logger.info("  Removed %s exact duplicates + %s likely duplicates", exact_dupes, smart_dupes)
            else:
                logger.info("  Removed %s exact duplicate records", exact_dupes)

            output_file = yearly_folder / f"FAA_{year}.csv"
            combined.to_csv(output_file, index=False)
            logger.info("  Saved %s records to %s", len(combined), output_file.name)

    try:
        logger.info("=== Starting FAA UAS Sighting Enrichment Pipeline ===")
        logger.info("Debug mode: %s", "ENABLED" if logger.isEnabledFor(logging.DEBUG) else "DISABLED")
        phase_1_split()
        phase_2_enrich()
        phase_3_consolidate_by_year()
    except SecurityError as exc:
        logger.critical("Security validation failed: %s", exc)
        raise
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        save_cache_with_validation(cache_file, city_cache)
        logger.info("=== Processing Complete ===")
