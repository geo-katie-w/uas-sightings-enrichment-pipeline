# UAS Sighting Enrichment Pipeline

[![Security: Hardened](https://img.shields.io/badge/security-hardened-green.svg)](https://github.com/geo-katie-w/uas-sightings-enrichment-pipeline)
[![Platform: Cross-Platform](https://img.shields.io/badge/platform-windows%20%7C%20mac%20%7C%20linux-blue.svg)](https://github.com/geo-katie-w/uas-sightings-enrichment-pipeline)
[![Python: 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

## Overview

The **UAS Sighting Enrichment Pipeline** is a **production-grade, security-hardened** data processing tool designed to transform raw FAA Unmanned Aircraft System (UAS) sighting reports into clean, structured, GIS-ready datasets.

The pipeline extracts critical information from unstructured text summaries and enriches each record with:

- **Airport Codes** (IATA/ICAO) with geographic coordinates
- **Aircraft Type** identification
- **UAS/Drone Color** detection
- **Altitude** in feet
- **Evasive Action** flags
- **Law Enforcement Agency** notifications

**Security-First Design**: Built with comprehensive security controls including path traversal protection, file size validation, ReDoS prevention, and secure file permissions.

**Cross-Platform**: Runs seamlessly on Windows, macOS, and Linux with platform-aware file permission handling.

Designed to handle large datasets efficiently while respecting API rate limits, the pipeline uses a three-phase architecture: Split → Enrich → Consolidate.

---

## Data Source & Significance

### **What is This Data?**

This pipeline processes **official FAA UAS (Unmanned Aircraft System) sighting reports** – public records of drone encounters reported by pilots, law enforcement, and citizens across the United States.

**Official Data Source**: [FAA UAS Sightings Reports](https://www.faa.gov/uas/resources/public_records/uas_sightings_report)

### **Why This Matters**

UAS sighting data is critical for:

- **Aviation Safety**: Identifying high-risk areas where drones pose collision hazards to manned aircraft
- **Regulatory Compliance**: Tracking unauthorized drone operations near airports and restricted airspace
- **Law Enforcement**: Supporting investigations of illegal drone activity and airspace violations
- **Policy Development**: Informing FAA regulations and airspace management decisions
- **Public Awareness**: Understanding the scale and nature of drone incidents nationwide

### **Data Characteristics**

- **Timeframe**: Reports dating back to 2014, updated regularly by the FAA
- **Volume**: Thousands of incidents per year (growing annually)
- **Geographic Coverage**: All 50 US states and territories
- **Format**: Free-form text narratives optimized for human readability
- **Opportunity**: Reports are designed for incident documentation rather than automated analysis

**This pipeline transforms narrative-style sighting reports into structured, GIS-ready datasets** that enable spatial analysis, trend identification, and evidence-based decision making.

---

## Quick Start

### **Installation**

```bash
# Clone the repository
git clone https://github.com/geo-katie-w/uas-sightings-enrichment-pipeline.git
cd uas-sightings-enrichment-pipeline

# Install required dependencies
pip install pandas airportsdata geopy openpyxl jsonschema

# Optional: For Windows users - enhanced file permissions
pip install pywin32
```

### **Basic Usage**

```bash
# The pipeline auto-detects your platform and uses ~/FAA_UAS_Sightings by default
# Place your CSV/Excel files there, then run:
python UAS_Sighting_Enrichment_Pipeline.py

# Or specify a custom path:
export FAA_DATA_PATH="/path/to/your/data"  # Mac/Linux
set FAA_DATA_PATH="C:\path\to\your\data"   # Windows
python UAS_Sighting_Enrichment_Pipeline.py
```

### **Enable Debug Logging**

```bash
# Mac/Linux
export FAA_PIPELINE_DEBUG=true
python UAS_Sighting_Enrichment_Pipeline.py

# Windows PowerShell
$env:FAA_PIPELINE_DEBUG="true"
python UAS_Sighting_Enrichment_Pipeline.py

# Windows Command Prompt
set FAA_PIPELINE_DEBUG=true
python UAS_Sighting_Enrichment_Pipeline.py
```

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

### **Security Features**

- **Path Traversal Protection**: Validates all file paths against allowed directories
- **File Size Validation**: 100MB limit prevents memory exhaustion attacks
- **ReDoS Prevention**: Regex timeout protection (2-second limit) prevents catastrophic backtracking
- **Secure File Permissions**: Cache files restricted to owner-only access (0600 on Unix, ACL on Windows)
- **JSON Schema Validation**: Cache data validated against schema to prevent poisoning attacks
- **Input Sanitization**: Text length limits (50,000 chars) prevent processing abuse
- **Structured Logging**: Professional logging system with configurable debug mode
- **No SQL/XSS Vulnerabilities**: File-based processing only, no database or web output

### **Cross-Platform Compatibility**

- **Intelligent Path Handling**: Uses `Path.home()` for platform-agnostic defaults
- **Platform-Aware Permissions**: Windows ACLs on Windows, Unix permissions on Mac/Linux
- **Environment Variable Support**: Override defaults with `FAA_DATA_PATH`
- **Automatic Platform Detection**: Adapts behavior based on operating system

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
- **Operating Systems**: Windows 10+, macOS 10.14+, Linux (any modern distribution)

### **Python Dependencies**

#### **Required Packages**

```bash
pip install pandas airportsdata geopy openpyxl jsonschema
```

**Package Descriptions:**

- `pandas` - Data manipulation and CSV/Excel processing
- `airportsdata` - IATA/ICAO airport database with coordinates
- `geopy` - Geocoding library (Nominatim integration)
- `openpyxl` - Excel file format support
- `jsonschema` - Cache validation and security (required for validation)

#### **Optional Packages**

```bash
# Windows users - for proper file permission security
pip install pywin32
```

- `pywin32` - Windows-specific file ACL management (recommended for Windows)

### **Input Data Requirements**

- Source files: CSV or Excel (.xlsx)
- Required columns: Summary/Narrative text field, City, State
- File naming: Should include year (e.g., "Jan2023_March2023.csv")
- Encoding: UTF-8 preferred

---

## Configuration

### **1. Set Your Data Folder Path**

**Option A: Use Default (Recommended)**

The pipeline automatically uses `~/FAA_UAS_Sightings` in your home directory:
- Windows: `C:\Users\YourName\FAA_UAS_Sightings`
- Mac: `/Users/YourName/FAA_UAS_Sightings`
- Linux: `/home/YourName/FAA_UAS_Sightings`

Just create the folder and place your files there!

**Option B: Use Environment Variable**

```bash
# Mac/Linux
export FAA_DATA_PATH="/path/to/your/data"

# Windows PowerShell
$env:FAA_DATA_PATH="C:\path\to\your\data"

# Windows Command Prompt
set FAA_DATA_PATH="C:\path\to\your\data"
```

**Option C: Edit Configuration File**

Edit line 26 in `UAS_Sighting_Enrichment_Pipeline.py`:

```python
FOLDER_PATH = os.getenv('FAA_DATA_PATH') or str(Path.home() / "FAA_UAS_Sightings")
```

### **2. Security: Allowed Directories**

By default, the pipeline restricts file access to:
- Your home directory and subdirectories
- Current working directory

To allow additional directories, edit `ALLOWED_BASE_DIRS` (lines 34-39).

### **3. Optional: Adjust Processing Parameters**

```python
ROWS_PER_SPLIT = 250          # Chunk size (smaller = more stable)
MAX_RETRY_ATTEMPTS = 3        # API retry limit
RETRY_DELAY_BASE = 30         # Seconds between retries
```

### **4. Coordinate Validation Bounds**

US territory boundaries (default):

```python
LON_MIN, LON_MAX = -125, -65  # West to East
LAT_MIN, LAT_MAX = 25, 50     # South to North
```

### **5. Environment Variables**

| Variable | Description | Default | Example |
|----------|-------------|---------|----------|
| `FAA_DATA_PATH` | Data folder location | `~/FAA_UAS_Sightings` | `/data/faa` |
| `FAA_PIPELINE_DEBUG` | Enable debug logging | `false` | `true` |

### **6. Logging Configuration**

The pipeline uses Python's logging module with two levels:

- **INFO (default)**: Progress updates, file counts, validation summaries
- **DEBUG**: Detailed pattern matching, airport extraction logic, coordinate population

Enable debug mode for troubleshooting:

```bash
# See detailed extraction logic
export FAA_PIPELINE_DEBUG=true
python UAS_Sighting_Enrichment_Pipeline.py
```

Logs include timestamps, log levels, and structured messages:
```
2026-02-13 14:23:15 - INFO - Loaded 5234 US IATA airports and 3421 ICAO mappings
2026-02-13 14:23:16 - DEBUG - Extracted airport 'LAX' (priority: critical)
2026-02-13 14:23:17 - INFO - Valid coordinates: 245/250 records
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

## Security Best Practices

### **File Permissions**

The geocoding cache file is automatically secured:
- **Unix/Linux/Mac**: Permissions set to 0600 (owner read/write only)
- **Windows**: ACL restricts access to current user only (requires `pywin32`)

Verify cache permissions:

```bash
# Unix/Linux/Mac
ls -l ~/FAA_UAS_Sightings/geocoding_cache.json
# Should show: -rw------- (600)

# Windows PowerShell
Get-Acl ~/FAA_UAS_Sightings/geocoding_cache.json | Format-List
```

### **Path Traversal Protection**

The pipeline validates all paths before processing. If you see:

```
SecurityError: Folder path 'X' is outside allowed directories
```

Add the path to `ALLOWED_BASE_DIRS` in the configuration.

### **File Size Limits**

Files exceeding 100MB are automatically rejected:

```
ERROR: file.csv exceeds size limit (150.3MB > 100MB), skipping
```

Adjust `MAX_FILE_SIZE_MB` if processing legitimate large files.

### **Input Validation**

- CSV parsing errors are logged but don't crash the pipeline (`on_bad_lines='warn'`)
- Text fields are truncated to 50,000 characters to prevent ReDoS
- Regex operations timeout after 2 seconds
- JSON cache is validated against schema on every load

### **Running in Production**

1. **Set restrictive directory permissions**:
   ```bash
   chmod 700 ~/FAA_UAS_Sightings  # Unix/Linux/Mac
   ```

2. **Use environment variables** instead of hardcoding paths

3. **Review logs regularly** for warnings/errors

4. **Keep dependencies updated**:
   ```bash
   pip install --upgrade pandas geopy jsonschema
   ```

5. **Validate cache integrity** if shared across systems:
   - Delete `geocoding_cache.json` if suspicious
   - Cache regenerates automatically

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
- **File size limit**: 100MB max per file (configurable for security)
- **Regex timeout**: 2-second limit prevents ReDoS attacks
- **Chunk size**: 250 rows (smaller = more stable, prevents memory issues)

### **Platform-Specific Performance**

- **Windows**: Slightly slower file I/O due to ACL overhead (negligible)
- **macOS/Linux**: Faster Unix permission handling
- **All platforms**: Identical processing logic and accuracy

---

## Use Cases

**GIS Mapping** - Import to ArcGIS Online, QGIS, or other mapping platforms  
**Spatial Analysis** - Analyze sighting patterns by airport proximity  
**Trend Analysis** - Track drone activity over time and location  
**Law Enforcement** - Identify response patterns by jurisdiction  
**Aviation Safety** - Assess risk areas and altitude distributions  

---

## Platform-Specific Notes

### **Windows**

- **File Permissions**: Install `pywin32` for proper ACL security:
  ```powershell
  pip install pywin32
  ```

- **Path Format**: Use forward slashes or raw strings:
  ```python
  r"C:\Users\Name\Data"  # Raw string
  "C:/Users/Name/Data"    # Forward slashes (recommended)
  ```

- **Environment Variables**:
  ```powershell
  # PowerShell
  $env:FAA_DATA_PATH="C:\Data\FAA"
  
  # Command Prompt
  set FAA_DATA_PATH=C:\Data\FAA
  ```

### **macOS**

- Default data location: `/Users/YourName/FAA_UAS_Sightings`
- File permissions: Automatically set to 0600
- Environment variables:
  ```bash
  export FAA_DATA_PATH="/Users/YourName/custom/path"
  ```

### **Linux**

- Default data location: `/home/YourName/FAA_UAS_Sightings`
- File permissions: Automatically set to 0600
- Works on all distributions (Ubuntu, Fedora, Debian, etc.)
- Environment variables:
  ```bash
  export FAA_DATA_PATH="/data/faa"
  ```

---

## License & Attribution

This tool processes publicly available FAA UAS sighting reports. Ensure compliance with data usage policies when publishing derived datasets.
