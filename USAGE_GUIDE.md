# County Property Data Parser - Usage Guide

## Overview

This tool is designed to parse and clean large bulk property tax files from county appraisal districts. It handles files like the Harris County (HCAD) data you have, which can be hundreds of megabytes in size.

## Your Data Files

Based on your setup, the parser detected these files:

- **Real Accounts** (`real_acct.txt`): 819.19 MB - Main property records
- **Owners** (`owners.txt`): 82.07 MB - Property ownership information  
- **Additional files**: deeds.txt, permits.txt, parcel_tieback.txt, real_mnrl.txt

## Quick Start Commands

### 1. Parse Property Records (Large File - Use Chunks)
```bash
python3 -m county_parser parse-real-accounts ~/Downloads/hcad_2025/real_acct.txt --use-chunks --format parquet
```

### 2. Parse Owner Information
```bash
python3 -m county_parser parse-owners ~/Downloads/hcad_2025/owners.txt --format parquet
```

### 3. Join Data Together
```bash
python3 -m county_parser join-data --real-accounts output/parsed_real_acct.parquet --owners output/parsed_owners.parquet --output combined_property_data.parquet
```

### 4. Check Configuration
```bash
python3 -m county_parser info
```

## Processing Options

### Output Formats
- `--format parquet` (recommended for large data)
- `--format csv` (human readable)
- `--format json` (for web applications)

### Chunk Processing
For very large files (like your 819MB real_acct.txt), use `--use-chunks` to process in smaller pieces:
```bash
python3 -m county_parser parse-real-accounts ~/Downloads/hcad_2025/real_acct.txt --use-chunks --chunk-size 5000
```

## Data Cleaning Features

The parser automatically:

✅ **Standardizes addresses** - normalizes street types (Street → ST, Avenue → AVE)  
✅ **Cleans names** - identifies businesses vs individuals, standardizes formats  
✅ **Validates data** - handles missing values, converts data types  
✅ **Creates derived fields** - full addresses, boolean flags, normalized values  

## Example Output

After parsing, you'll get clean data with fields like:

**Real Accounts Data:**
- Account numbers, owner names, addresses
- Property values (land, building, total appraised)
- Market areas, school districts
- Legal descriptions, improvement years

**Owners Data:**
- Account number linkages
- Multiple owners per property
- Ownership percentages
- Business entity flags

## File Sizes & Performance

Your files will process as follows:
- **Real Accounts (819MB)**: ~5-10 minutes with chunking
- **Owners (82MB)**: ~1-2 minutes  
- **Output files**: Typically 30-50% smaller as Parquet format

## Integration Options

The parsed data can be imported into:
- **Excel/Google Sheets** (CSV format)
- **Python/Pandas** (Parquet format)
- **Databases** (PostgreSQL, SQLite)
- **BI Tools** (Tableau, Power BI)

## Getting Help

```bash
python3 -m county_parser --help
python3 -m county_parser parse-real-accounts --help
```

## Next Steps

1. **Start Small**: Parse the owners file first (smaller, faster)
2. **Process Main Data**: Use chunking for real_acct.txt  
3. **Join & Analyze**: Combine the data for comprehensive analysis
4. **Export Results**: Choose format based on your analysis needs
