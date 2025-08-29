# County Property Data Parser

A Python tool for parsing and cleaning large bulk property tax files from county appraisal districts.

## Features

- **Large File Support**: Efficiently processes files >200MB using streaming and chunking
- **Multiple Formats**: Handles CSV, tab-delimited, and other common county data formats  
- **Data Cleaning**: Built-in utilities for standardizing addresses, names, and property data
- **Flexible Output**: Export to CSV, Parquet, JSON, or other formats
- **CLI Interface**: Easy-to-use command-line tools for batch processing

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Parse real account data
python -m county_parser parse-real-accounts ~/Downloads/hcad_2025/real_acct.txt --output cleaned_real_accounts.parquet

# Parse owners data  
python -m county_parser parse-owners ~/Downloads/hcad_2025/owners.txt --output cleaned_owners.parquet

# Join related data
python -m county_parser join-data --real-accounts cleaned_real_accounts.parquet --owners cleaned_owners.parquet --output combined_data.parquet
```

## Supported File Types

- Real Account Records (`real_acct.txt`)
- Owner Information (`owners.txt`)
- Deed Records (`deeds.txt`)
- Permit Data (`permits.txt`)
- Parcel Tieback (`parcel_tieback.txt`)

## Configuration

Copy `.env.example` to `.env` and customize paths and processing options.
