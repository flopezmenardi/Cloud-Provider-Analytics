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

### Phase 3: Gold Layer - Business Marts

**Run all Gold mart creations:**
```bash
./scripts/run_gold_marts.sh
```

This creates 5 business-ready aggregated datasets:

1. **org_daily_usage_by_service** - Daily usage aggregations by org, date, and service
   - Metrics: cost, requests, CPU hours, storage, carbon footprint, GenAI tokens
   - 11,050 rows (3.9x compression from 42,989 events)

2. **revenue_by_org_month** - Monthly revenue with USD normalization
   - Metrics: total billed, credits, taxes, net revenue, invoice count
   - Enriched with org name, industry, plan tier
   - 227 rows

3. **tickets_by_org_date** - Support ticket metrics
   - Metrics by severity: total tickets, resolution time, SLA breach rate, CSAT
   - Category breakdowns: billing, technical, access
   - 984 rows

4. **genai_tokens_by_org_date** - GenAI/LLM token consumption (schema v2 only)
   - Metrics: total tokens, cost, cost per million tokens
   - Filtered to 3,115 GenAI events → 848 aggregated rows

5. **cost_anomaly_mart** - Aggregated cost anomalies for alerting
   - Filters 12,034 anomalous events (27.99%) → 6,106 aggregated rows
   - Detection method counts (z-score, MAD, percentiles)

**Output:** Aggregated data in `datalake/gold/` partitioned by year/month

**Execution time:** ~12 seconds for all 5 marts

### Phase 4: Serving Layer - AstraDB

The Serving Layer implements the Lambda Architecture serving component, loading Gold marts into AstraDB for low-latency queries using the Data API.

**Prerequisites**: Complete Bronze, Silver, and Gold layers first.

#### Quick Start (5 minutes)

See detailed guide in [ASTRADB_SETUP.md](ASTRADB_SETUP.md)

1. **Create AstraDB account** and database:
   - Go to https://astra.datastax.com/
   - Create database: `cloud_analytics`
   - Keyspace: `cloud_analytics`
   - Get API Endpoint and generate Token

2. **Configure credentials** in `.env` file:
   ```bash
   cp .env.example .env
   # Edit .env and add your credentials:
   # ASTRADB_TOKEN=AstraCS:your_token_here
   # ASTRADB_API_ENDPOINT=https://your-database-id.apps.astra.datastax.com
   # ASTRADB_NAMESPACE=cloud_analytics
   ```

3. **Install dependencies**:
   ```bash
   source setup.sh
   pip install astrapy python-dotenv
   ```

4. **Run setup pipeline**:
   ```bash
   ./scripts/setup_astradb.sh      # Create collections
   ./scripts/load_to_astradb.sh    # Load ~45K documents
   ./scripts/run_demo_queries.sh   # Run 5 queries
   ```

**Expected runtime**: ~2 minutes total

#### What Gets Created

**5 AstraDB Collections** (NoSQL documents):
- `org_daily_usage` - Daily cost queries by org and service (11,050 docs)
- `org_service_costs` - Top-N services by cost with time windows (~33,150 docs)
- `tickets_critical_daily` - Critical ticket metrics and SLA monitoring
- `revenue_monthly` - Monthly revenue trends (227 docs)
- `genai_tokens_daily` - GenAI token consumption (848 docs)

#### Demo Queries

The 5 demo queries showcase different analytics use cases:

1. **Query 1:** Daily costs by organization and service (FinOps)
2. **Query 2:** Top-N services by cost in time window (Cost optimization)
3. **Query 3:** Critical tickets & SLA breach rate (Support operations)
4. **Query 4:** Monthly revenue by organization (Finance)
5. **Query 5:** GenAI token usage by organization (Product analytics)

**See [ASTRADB_SETUP.md](ASTRADB_SETUP.md) for detailed step-by-step instructions with checkpoints and troubleshooting.**

### Phase 5: Speed Layer
*To be implemented - Real-time streaming aggregations (future work)*

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

### Completed ✅
- **Bronze Layer** (Batch + Streaming ingestion)
  - 7 CSV files ingested
  - 43,200 usage events from 120 JSONL files

- **Silver Layer** (Data quality & conformance)
  - 8/8 transformations completed
  - 99%+ data quality for most datasets
  - Anomaly detection with 3 statistical methods
  - Schema v1/v2 compatibility

- **Gold Layer** (Business marts)
  - 5/5 marts created successfully
  - 11,050 usage aggregations, 227 revenue records, 984 ticket metrics
  - 848 GenAI token records, 6,106 anomaly records
  - Execution time: ~12 seconds

- **Serving Layer** (Infrastructure)
  - Cassandra/AstraDB configuration and connection
  - 5 query-optimized table schemas created
  - Data loader implemented (Gold → Cassandra)
  - 5 demo CQL queries implemented
  - Execution scripts created

### Ready to Execute 🎯
1. **Setup AstraDB account** (or use local Cassandra)
2. **Configure credentials** in `src/config/cassandra_config.py`
3. **Run serving layer pipeline:**
   ```bash
   ./scripts/setup_cassandra.sh      # Create tables
   ./scripts/load_to_cassandra.sh    # Load data
   ./scripts/run_demo_queries.sh     # Execute queries
   ```

### Future Work 📋
- Speed Layer (real-time streaming aggregations)
- Complete end-to-end orchestration
- Presentation and video
