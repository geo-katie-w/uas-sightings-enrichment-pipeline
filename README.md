# UAS Sighting Enrichment Pipeline

## Overview

The **UAS Sighting Enrichment Pipeline** is an automated data processing tool designed to transform raw FAA Unmanned Aircraft System (UAS) sighting reports into clean, structured, GIS-ready datasets.

The pipeline extracts critical information from unstructured text summaries and enriches each record with:

- **Airport Codes** (IATA/ICAO) with geographic coordinates
- **Aircraft Type** identification
- **UAS/Drone Color** detection
- **Altitude** in feet
- **Evasive Action** flags
- **Law Enforcement Agency** notifications

Designed to handle large datasets efficiently while respecting API rate limits, the pipeline uses a three-phase architecture: Split → Enrich → Consolidate.

---

## Key Features

### **Phase 1: File Splitting & Cleaning**

- Auto-detects CSV and Excel files in the source folder
- **Ghost Buster Logic**: Removes empty columns and "Unnamed" artifacts from Excel exports
- Splits large files into 250-row chunks for memory efficiency and API safety
- Creates dated subfolders for organized processing runs

### **Phase 2: Intelligent Enrichment**

- **Smart Column Detection**: Automatically identifies Summary, City, and State columns using keyword matching
- **Priority-Based Airport Extraction**:
  - **Critical Priority**: Distance+direction patterns ("5 NW LAX", "12 ENE ATL")
  - **High Priority**: Runway references, ICAO codes, explicit mentions
  - **Fallback**: Geocoding via Nominatim API (with persistent caching)
- **Advanced Pattern Recognition**:
  - Aircraft types (multiple patterns including manufacturer names)
  - 16 color variations (RED, BLACK, GREY, MULTI-COLORED, etc.)
  - Multiple altitude formats (1,500 FEET, 500 FT, FL250)
  - Law Enforcement agencies (with FAA facility filtering)
- **Coordinate Population**: Lon/Lat values for all valid US airports (IATA + ICAO fallback)
- **Progress Tracking**: Real-time file counters and validation summaries

### **Phase 3: Automated Consolidation**

- Searches across all dated processing runs
- Merges records by year into master files (FAA_Master_2023.csv, etc.)
- **Smart Deduplication**:
  - Removes exact duplicates
  - Identifies likely duplicates by date+city+altitude
- **Value Standardization**: Consolidates N/A, UNKNOWN, NOT REPORTED → None
- Outputs GIS-ready CSV files for mapping platforms

### **Robustness Features**

- **Checkpoint Resuming**: Skips already-processed files on re-runs
- **Persistent Geocoding Cache**: Saves API lookups to disk, reloads on restart
- **Error Handling**: Graceful failure with detailed logging
- **Configurable Retry Logic**: API timeout handling with exponential backoff

---

## Output Structure

The pipeline creates the following folder hierarchy:

```
FOLDER_PATH/
├── your_source_files.csv           # Original raw data
├── Split_Chunks/
│   └── 2026-02-07/                 # Dated processing runs
│       ├── file_part_1.csv
│       ├── file_part_2.csv
│       └── ...
├── Processed_Files/
│   └── 2026-02-07/                 # Enriched chunks
│       ├── Enriched_file_part_1.csv
│       ├── Enriched_file_part_2.csv
│       └── ...
├── Yearly_Masters/                  # FINAL OUTPUT
│   ├── FAA_Master_2023.csv
│   ├── FAA_Master_2024.csv
│   └── FAA_Master_2025.csv
└── geocoding_cache.json            # Persistent API cache
```

### **Output Columns**

Enriched files contain all original columns plus:

- `Acft_Type` - Aircraft type identifier
- `UAS_Color` - Drone/UAS color
- `Alt_Ft` - Altitude in feet
- `Evasive` - YES/NO evasive action flag
- `LEO_Agency` - Law enforcement agency notified
- `Assigned_Airport` - 3-letter IATA airport code
- `Airport_Longitude` - Decimal degrees
- `Airport_Latitude` - Decimal degrees

---

## Requirements

### **System Requirements**

- Python 3.8 or higher
- 4GB+ RAM recommended for large datasets
- Internet connection (for geocoding API)

### **Python Dependencies**

```bash
pip install pandas airportsdata geopy openpyxl
```

**Package Descriptions:**

- `pandas` - Data manipulation and CSV/Excel processing
- `airportsdata` - IATA/ICAO airport database with coordinates
- `geopy` - Geocoding library (Nominatim integration)
- `openpyxl` - Excel file format support

### **Input Data Requirements**

- Source files: CSV or Excel (.xlsx)
- Required columns: Summary/Narrative text field, City, State
- File naming: Should include year (e.g., "Jan2023_March2023.csv")
- Encoding: UTF-8 preferred

---

## Configuration

### **1. Set Your Data Folder Path**

Edit line 14 in `UAS_Sighting_Enrichment_Pipeline.py`:

```python
FOLDER_PATH = r"C:/Documents/FAA_UAS_Sightings"  # Update this path
```

### **2. Optional: Adjust Processing Parameters**

```python
ROWS_PER_SPLIT = 250          # Chunk size (smaller = more stable)
MAX_RETRY_ATTEMPTS = 3        # API retry limit
RETRY_DELAY_BASE = 30         # Seconds between retries
```

### **3. Coordinate Validation Bounds**

US territory boundaries (default):

```python
LON_MIN, LON_MAX = -125, -65  # West to East
LAT_MIN, LAT_MAX = 25, 50     # South to North
```

---

## Usage

### **Basic Execution**

```bash
python UAS_Sighting_Enrichment_Pipeline.py
```

### **Expected Workflow**

1. Place raw CSV/Excel files in your configured `FOLDER_PATH`
2. Run the script
3. Monitor console output for progress
4. Find enriched yearly master files in `Yearly_Masters/` folder

### **Re-running After Interruption**

The script automatically:

- Skips already-processed chunks
- Resumes from the last incomplete file
- Preserves geocoding cache for faster restarts

### **Processing a New Dataset**

To reprocess with updated logic:

```powershell
# Delete today's processed files
Remove-Item "C:\Documents\FAA_UAS_Sightings\Processed_Files\2026-02-07" -Recurse -Force
```

Then rerun the script.

---

## Troubleshooting

### **No Coordinates Populating**

- Check if files are being skipped (already processed)
- Delete processed files folder and rerun
- Verify airport codes are extracting correctly in console output

### **Wrong Airport Codes**

- Pattern priority may need adjustment
- Check debug output to see which pattern matched
- Verify distance+direction format in Summary text

### **Geocoding Timeouts**

- Script automatically retries with exponential backoff
- Nominatim free tier has rate limits (1 request/second)
- Cache reduces repeat queries significantly

### **Missing LEO Agency**

- Verify text contains "XXX NOTIFIED" pattern
- LEO agencies must appear after "NO EVASIVE ACTION" text
- FAA facilities (ATCT, TRACON) are filtered out

### **Column Identification Failures**

- Console shows "WARNING: No summary column found"
- Ensure headers contain keywords: summary, narrative, description, remarks, event
- Check for city/state columns: city, location, town / state, province

### **File Permission Errors**

- Close Excel files before running Phase 3 consolidation
- Windows locks open CSV files - close all data viewers

---

## Performance Notes

- **Small datasets** (<10,000 records): ~5-15 minutes
- **Large datasets** (50,000+ records): 1-3 hours (geocoding dependent)
- **Cache speedup**: 80-95% faster on subsequent runs with same cities
- **API throttling**: Script pauses automatically, don't interrupt
- **Memory usage**: ~200-500MB for typical processing

---

## Use Cases

✅ **GIS Mapping** - Import to ArcGIS Online, QGIS, or other mapping platforms  
✅ **Spatial Analysis** - Analyze sighting patterns by airport proximity  
✅ **Trend Analysis** - Track drone activity over time and location  
✅ **Law Enforcement** - Identify response patterns by jurisdiction  
✅ **Aviation Safety** - Assess risk areas and altitude distributions  

---

## License & Attribution

This tool processes publicly available FAA UAS sighting reports. Ensure compliance with data usage policies when publishing derived datasets.
