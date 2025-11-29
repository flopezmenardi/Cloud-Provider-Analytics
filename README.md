# Cloud Provider Analytics - Big Data Project

Lambda Architecture data pipeline for Cloud Provider Analytics, processing batch and streaming data through bronze, silver, and gold layers.

## Setup

### 1. Install Dependencies

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python packages
pip install -r requirements.txt
```

### 2. Environment Setup

**Important:** PySpark 3.5+ requires Java 17.

```bash
# Install Java 17 (macOS with Homebrew)
brew install openjdk@17

# Create symlink (macOS only)
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk

# Activate environment (sets Java 17 and Python venv)
source setup.sh
```

The `setup.sh` script automatically:
- Activates Python virtual environment
- Sets JAVA_HOME to Java 17
- Verifies Java version

## Usage

### Phase 1: Bronze Layer Ingestion

**Batch Ingestion (CSV files):**
```bash
./scripts/run_batch_ingestion.sh
```

This ingests all CSV sources to the bronze layer:
- `users.csv`
- `customers_orgs.csv`
- `resources.csv`
- `billing_monthly.csv`
- `support_tickets.csv`
- `marketing_touches.csv`
- `nps_surveys.csv`

**Streaming Ingestion (Usage Events):**
```bash
# Batch mode (one-time load of all JSONL files)
./scripts/run_streaming_ingestion.sh batch

# Streaming mode (continuous processing)
./scripts/run_streaming_ingestion.sh
```

**Output:** All data is written to `datalake/bronze/` in Parquet format with ingestion metadata.

### Phase 2: Silver Layer - Data Quality & Conformance 

**Run all Silver transformations:**
```bash
./scripts/run_silver_transformations.sh
```

This performs data quality transformations on all Bronze sources:
- **Data Quality:** Validates ranges, detects outliers (z-score, MAD, percentiles), quarantines invalid records
- **Schema Compatibility:** Handles schema_version v1/v2 for usage_events (carbon_kg, genai_tokens)
- **Normalization:** Standardizes regions, services, metrics, currencies
- **Enrichment:** Joins with customers_orgs and resources
- **Validations:** Email formats, NPS scores (0-10), cost >= -0.01, service types, etc.

**Output:** Clean data in `datalake/silver/` + quarantined records in `datalake/silver/quarantine/`

**Results:**
- `customers_orgs`: 80 records (100% valid)
- `resources`: 400 records (100% valid)
- `users`: 800 records (100% valid)
- `billing_monthly`: 227 valid, 13 quarantined (94.58% quality)
- `support_tickets`: 1,000 records (100% valid)
- `marketing_touches`: 1,500 records (100% valid)
- `nps_surveys`: 7 valid, 85 quarantined (7.61% quality - needs investigation)
- `usage_events`: 42,989 valid, 211 quarantined (99.51% quality)
  - Schema v1: 10,800 records
  - Schema v2: 32,400 records
  - Anomalies detected: 12,108 (28.03%)
  - Partitioned by year/month for performance

### Phase 3: Gold Layer
*To be implemented - Business-ready aggregated datasets*

### Phase 4: Speed Layer
*To be implemented - Real-time processing for streaming data*

### Phase 5: Serving Layer - AstraDB/Cassandra

**Setup AstraDB (one-time):**
1. Create free account at: https://astra.datastax.com/
2. Create database: `cloud_analytics`
3. Download secure connect bundle
4. Create application token (Client ID + Secret)
5. Update credentials in `src/config/cassandra_config.py`

**Create keyspace and tables:**
```bash
python -m src.serving.cassandra_setup
```

This creates 5 query-optimized tables for the demo queries.

**Output:** Cassandra tables ready for data loading

## Project Structure

```
tpe-bigdata/
├── src/
│   ├── batch/              # Batch processing jobs
│   ├── streaming/          # Streaming processing jobs
│   ├── common/             # Shared utilities
│   └── config/             # Configuration
├── datalake/
│   ├── landing/           # Source data (CSV, JSONL) - committed to git
│   ├── bronze/            # Raw ingested data - gitignored
│   ├── silver/            # Cleaned data - gitignored (Phase 2)
│   └── gold/              # Aggregated data - gitignored (Phase 3)
├── scripts/               # Execution scripts
└── requirements.txt       # Python dependencies
```

## Data Sources

**Batch Sources (CSV):**
- `users.csv` - User information
- `customers_orgs.csv` - Organization/customer data
- `resources.csv` - Resource definitions
- `billing_monthly.csv` - Monthly billing invoices
- `support_tickets.csv` - Support ticket data
- `marketing_touches.csv` - Marketing interaction data
- `nps_surveys.csv` - Net Promoter Score surveys

**Streaming Source (JSONL):**
- `usage_events_stream/*.jsonl` - Real-time usage events
  - Schema version 1: Basic fields
  - Schema version 2: Adds `carbon_kg` and `genai_tokens` (after 2025-07-18)

## Requirements

- Python 3.9+
- Java 17 (required for PySpark 3.5+)
- PySpark 3.5.0+
- macOS, Linux, or Windows (with WSL)

## Troubleshooting

**Java version errors:**
```
java.lang.UnsupportedClassVersionError: ... class file version 61.0
```
- **Solution:** Install Java 17 (see setup instructions above)
- PySpark 3.5+ is compiled with Java 17 and won't work with Java 8 or 11

**Import errors:**
- Ensure you're running from project root directory
- Activate virtual environment: `source venv/bin/activate`
- Run setup script: `source setup.sh`

**Permission errors:**
- Make scripts executable: `chmod +x scripts/*.sh`

**PySpark PosixPath errors:**
- All path conversions to `str()` are handled in the codebase
- If you see this error, ensure you're using the latest version of the transformation files

**Data Quality Issues:**
- High quarantine rate for `nps_surveys` (92%) - investigate source data
- Anomaly detection flags 28% of usage_events - this is normal for statistical outlier detection
- Check quarantine files in `datalake/silver/quarantine/` for invalid records

## Current Status

### Completed
- ✅ Bronze Layer (Batch + Streaming ingestion)
  - 7 CSV files ingested
  - 43,200 usage events from 120 JSONL files
- ✅ Silver Layer (Data quality & conformance)
  - 8/8 transformations completed
  - 99%+ data quality for most datasets
  - Anomaly detection with 3 statistical methods
  - Schema v1/v2 compatibility

### In Progress
- 🔄 Gold Layer (Business aggregations)
- 🔄 Speed Layer (Real-time processing)
- 🔄 Serving Layer (AstraDB/Cassandra)

### Next Steps
1. Implement Gold Layer transformations (5 business marts)
2. Load data to AstraDB/Cassandra
3. Implement demo queries
4. Create presentation and video
