# Docker Setup for County Scraper with MongoDB

## Quick Start

1. **Start MongoDB and Mongo Express**:
```bash
docker-compose up -d
```

2. **Verify MongoDB is running**:
```bash
docker-compose ps
```

3. **View logs** (if needed):
```bash
docker-compose logs mongodb
```

## Services

- **MongoDB**: `localhost:27017`
  - Username: `admin`
  - Password: `county_admin_2024`
  - Database: `county_data`

- **Mongo Express** (Web UI): `http://localhost:8081`
  - Username: `admin`
  - Password: `web_admin_2024`

## Environment Setup

1. **Copy environment template**:
```bash
cp .env.example .env
```

2. **Install Python dependencies**:
```bash
pip install -r requirements.txt
```

## Usage Examples

### 1. Save processed data to MongoDB:
```bash
# Process and save to MongoDB
python -m county_parser normalize-all --format json --output ./temp_data.json --sample-size 500

# Save to MongoDB
python -m county_parser save-to-mongodb -i ./temp_data.json
```

### 2. Check MongoDB status:
```bash
python -m county_parser mongodb-status
```

### 3. Create backup:
```bash
python -m county_parser backup-mongodb -o ./backup/county_data_backup.json
```

## MongoDB Collections

### Properties Collection
- **Index**: `account_id` (unique)
- **Indexes**: location, market area, value, timestamps
- **Full text search**: addresses, owner names, legal descriptions

### Processing Logs Collection  
- **Tracks**: batch processing, timestamps, status
- **Useful for**: auditing, monitoring, troubleshooting

## Stopping Services

```bash
docker-compose down
```

To remove data volumes (⚠️ **destroys all data**):
```bash
docker-compose down -v
```
