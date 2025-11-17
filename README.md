# Cloud Provider Analytics - Big Data Project

Lambda Architecture data pipeline for Cloud Provider Analytics, processing batch and streaming data through bronze, silver, and gold layers.

## Setup

### 1. Install Dependencies

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install Python packages
pip install -r requirements.txt
```

### 2. Java Setup

The scripts automatically configure Java 8/11/17 for Spark compatibility. No manual setup needed.

**If you encounter Java errors:**
```bash
# Activate environment (sets Java 8 automatically)
source activate_env.sh

# Or manually set Java 8
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
```

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

### Phase 2: Silver Layer
*To be implemented - Data quality and conformance transformations*

### Phase 3: Gold Layer
*To be implemented - Business-ready aggregated datasets*

### Phase 4: Speed Layer
*To be implemented - Real-time processing for streaming data*

### Phase 5: Serving Layer
*To be implemented - Unified views combining batch and speed layers*

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

- Python 3.8+
- Java 8, 11, or 17 (auto-detected by scripts)
- PySpark 3.5.0+

## Troubleshooting

**Java version errors:**
- Scripts automatically use Java 8 if available
- Run `source activate_env.sh` to manually set Java 8

**Import errors:**
- Ensure you're running from project root directory
- Activate virtual environment: `source venv/bin/activate`

**Permission errors:**
- Make scripts executable: `chmod +x scripts/*.sh`
