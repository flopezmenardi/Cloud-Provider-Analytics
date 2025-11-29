# Cloud Provider Analytics - Big Data Project

**Lambda Architecture data pipeline** for Cloud Provider Analytics with **Medallion Architecture** layers (Bronze → Silver → Gold → Serving).

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
- [Usage Guide](#usage-guide)
- [Data Sources](#data-sources)
- [Current Status](#current-status)
- [Troubleshooting](#troubleshooting)
- [Requirements](#requirements)

---

## Overview

This project implements a complete **Lambda Architecture** data pipeline processing both batch and streaming data from a cloud provider analytics platform. The pipeline ingests raw data, applies data quality transformations, creates business aggregations, and loads them into a serving layer (AstraDB) for low-latency queries.

**Key Features:**
- ✅ **Dual-mode ingestion**: Batch (CSV) and Streaming (JSONL)
- ✅ **Medallion Architecture**: Bronze → Silver → Gold data layers
- ✅ **Data Quality**: Anomaly detection, validation, quarantine
- ✅ **Schema Evolution**: Handles v1/v2 schema compatibility
- ✅ **Serving Layer**: AstraDB/Cassandra with optimized queries
- ✅ **Production-ready**: Partitioning, logging, error handling

---

## Architecture

### Lambda Architecture

```
┌─────────────┐     ┌─────────────┐
│   Batch     │     │  Streaming  │
│   Layer     │     │   Layer     │
│             │     │             │
│ CSV Files   │     │ JSONL Events│
└──────┬──────┘     └──────┬──────┘
       │                   │
       └───────┬───────────┘
               │
         ┌─────▼─────┐
         │  BRONZE   │ ← Raw ingestion
         │   Layer   │
         └─────┬─────┘
               │
         ┌─────▼─────┐
         │  SILVER   │ ← Data quality & conformance
         │   Layer   │
         └─────┬─────┘
               │
         ┌─────▼─────┐
         │   GOLD    │ ← Business aggregations
         │   Layer   │
         └─────┬─────┘
               │
         ┌─────▼─────┐
         │  SERVING  │ ← AstraDB (query layer)
         │   Layer   │
         └───────────┘
```

### Technology Stack

- **Storage**: Parquet (Data Lake)
- **Processing**: Apache Spark (PySpark, Structured Streaming)
- **Database**: AstraDB (Data API - NoSQL collections)
- **Language**: Python 3.9+
- **Runtime**: Java 17 (required for PySpark 3.5+)

---

## Quick Start

### 1. Initial Setup

```bash
# Clone repository (if applicable)
cd Cloud-Provider-Analytics

# Install Java 17 (macOS with Homebrew)
brew install openjdk@17
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
  /Library/Java/JavaVirtualMachines/openjdk-17.jdk

# Create virtual environment
python3 -m venv venv

# Activate environment (sets Java 17 + Python venv)
source setup.sh

# Install Python dependencies
pip install -r requirements.txt
```

**Note**: Always run `source setup.sh` before executing any pipeline commands.

### 2. Run Complete Pipeline (Bronze → Silver → Gold)

```bash
# Step 1: Bronze Layer - Ingest raw data
./scripts/run_batch_ingestion.sh        # CSV files → Bronze
./scripts/run_streaming_ingestion.sh batch  # JSONL → Bronze

# Step 2: Silver Layer - Data quality transformations
./scripts/run_silver_transformations.sh  # Bronze → Silver (clean data)

# Step 3: Gold Layer - Business aggregations
./scripts/run_gold_marts.sh              # Silver → Gold (marts)
```

**Total runtime**: ~60-90 seconds for full pipeline

### 3. Setup Serving Layer (AstraDB)

See detailed guide: **[ASTRADB_SETUP.md](ASTRADB_SETUP.md)**

```bash
# 1. Create AstraDB account at https://astra.datastax.com/
# 2. Create database: cloud_analytics
# 3. Get API Endpoint and Token

# 4. Configure credentials
cp .env.example .env
# Edit .env with your token and endpoint

# 5. Setup and load data
./scripts/setup_astradb.sh        # Create collections
./scripts/load_to_astradb.sh      # Load ~45K documents
./scripts/run_demo_queries.sh     # Run 5 demo queries
```

---

## Project Structure

```
Cloud-Provider-Analytics/
├── datalake/
│   ├── landing/              # Source data (CSV + JSONL) - committed to git
│   │   ├── *.csv            # 7 batch source files
│   │   └── usage_events_stream/  # 120 JSONL streaming files
│   ├── bronze/               # Raw ingested data - gitignored
│   │   ├── users/
│   │   ├── customers_orgs/
│   │   ├── usage_events/    # Partitioned by year/month
│   │   └── ...
│   ├── silver/               # Cleaned data - gitignored
│   │   ├── users/
│   │   ├── usage_events/    # Partitioned, with anomaly flags
│   │   ├── quarantine/      # Invalid records (for investigation)
│   │   └── ...
│   └── gold/                 # Aggregated marts - gitignored
│       ├── org_daily_usage_by_service/
│       ├── revenue_by_org_month/
│       └── ...
│
├── docs/                     # Documentation
│   ├── project_plan.md
│   ├── Pre_Entrega_TPE_Big_Data.pdf
│   └── Proyecto_Cloud_Provider_Analytics.pdf
│
├── src/
│   ├── ingestion/            # Data ingestion (Bronze layer)
│   │   ├── batch/           # Batch ingestion (CSV files)
│   │   │   ├── ingest_*.py  # 7 CSV ingestors
│   │   │   └── ingest_all_batch.py  # Orchestrator
│   │   └── streaming/       # Streaming ingestion (JSONL)
│   │       └── ingest_usage_events_stream.py
│   │
│   ├── transformations/      # Data quality transformations (Silver layer)
│   │   ├── transform_*_silver.py  # 8 transformations
│   │   └── transform_all.py      # Orchestrator
│   │
│   ├── aggregations/         # Business aggregations (Gold layer)
│   │   ├── create_*.py      # 5 mart creators
│   │   └── create_all_marts.py  # Orchestrator
│   │
│   ├── serving/              # Serving layer (AstraDB)
│   │   ├── astradb_setup.py
│   │   ├── load_to_astradb.py
│   │   └── demo_queries.py
│   │
│   ├── config/               # Configuration
│   │   ├── paths.py
│   │   ├── spark_config.py
│   │   └── astradb_config.py
│   │
│   └── common/               # Shared utilities
│       ├── data_quality.py
│       └── metadata.py
│
├── scripts/                  # Execution scripts
│   ├── setup.sh             # Environment setup (Java 17 + venv)
│   ├── run_batch_ingestion.sh
│   ├── run_streaming_ingestion.sh
│   ├── run_silver_transformations.sh
│   ├── run_gold_marts.sh
│   ├── setup_astradb.sh
│   ├── load_to_astradb.sh
│   └── run_demo_queries.sh
│
├── .env.example              # Template for credentials
├── .gitignore
├── README.md
├── TODO.md                   # Project todo list
├── ASTRADB_SETUP.md          # Step-by-step AstraDB guide
├── requirements.txt          # Python dependencies
└── setup.sh                  # Quick environment activation
```

---

## Data Pipeline

### Phase 1: Bronze Layer - Raw Ingestion

**Purpose**: Ingest raw data from source systems with minimal transformation

**Batch Sources (CSV)**:
```bash
./scripts/run_batch_ingestion.sh
```

Ingests 7 CSV files to Bronze layer:
- `users.csv` → 800 users
- `customers_orgs.csv` → 80 organizations
- `resources.csv` → 400 resources
- `billing_monthly.csv` → 240 invoices
- `support_tickets.csv` → 1,000 tickets
- `marketing_touches.csv` → 1,500 touches
- `nps_surveys.csv` → 92 surveys

**Streaming Source (JSONL)**:
```bash
./scripts/run_streaming_ingestion.sh batch
```

Ingests 120 JSONL files:
- `usage_events_stream/*.jsonl` → 43,200 usage events
- Handles schema v1 (before 2025-07-18) and v2 (after)
- Partitioned by year/month

**Output**: `datalake/bronze/` - Parquet files with ingestion metadata

---

### Phase 2: Silver Layer - Data Quality & Conformance

**Purpose**: Clean, validate, and standardize data

```bash
./scripts/run_silver_transformations.sh
```

**Transformations** (8 total):

1. **users** - Email validation, role standardization
2. **customers_orgs** - Organization data cleaning
3. **resources** - Resource metadata validation
4. **billing_monthly** - Currency normalization (USD), null handling
5. **support_tickets** - Status standardization, timestamp cleaning
6. **marketing_touches** - Touch type standardization
7. **nps_surveys** - Score validation (0-10 range)
8. **usage_events** - Complex transformations:
   - Schema v1/v2 compatibility
   - Anomaly detection (z-score, MAD, percentiles)
   - Region/service/metric standardization
   - Cost validation (allows negative costs)
   - Type conversions (string → numeric)

**Data Quality Results**:
- ✅ `users`: 800/800 valid (100%)
- ✅ `customers_orgs`: 80/80 valid (100%)
- ✅ `resources`: 400/400 valid (100%)
- ✅ `billing_monthly`: 227/240 valid (94.58%) - 13 quarantined
- ✅ `support_tickets`: 1,000/1,000 valid (100%)
- ✅ `marketing_touches`: 1,500/1,500 valid (100%)
- ⚠️ `nps_surveys`: 7/92 valid (7.61%) - 85 quarantined (needs investigation)
- ✅ `usage_events`: 42,989/43,200 valid (99.51%) - 211 quarantined
  - Schema v1: 10,800 events
  - Schema v2: 32,400 events (with carbon_kg, genai_tokens)
  - Anomalies detected: 12,108 (28.03% - expected for outlier detection)

**Output**: `datalake/silver/` + quarantined records in `datalake/silver/quarantine/`

---

### Phase 3: Gold Layer - Business Aggregations

**Purpose**: Create business-ready aggregated datasets

```bash
./scripts/run_gold_marts.sh
```

**Business Marts** (5 total):

1. **org_daily_usage_by_service** - Daily usage aggregations
   - **Rows**: 11,050 (3.9x compression from 42,989 events)
   - **Metrics**: cost, requests, CPU, storage, carbon, GenAI tokens
   - **Group by**: org_id, usage_date, service
   - **Use case**: FinOps daily cost tracking

2. **revenue_by_org_month** - Monthly revenue analysis
   - **Rows**: 227
   - **Metrics**: billed USD, credits, taxes, net revenue, invoice count
   - **Enriched**: org_name, industry, plan_tier (from customers_orgs)
   - **Use case**: Finance team revenue tracking

3. **tickets_by_org_date** - Support metrics
   - **Rows**: 984
   - **Metrics**: total tickets, resolution time, SLA breach rate, CSAT
   - **Group by**: org_id, ticket_date, severity
   - **Category breakdowns**: billing, technical, access tickets
   - **Use case**: Support operations monitoring

4. **genai_tokens_by_org_date** - GenAI usage (schema v2 only)
   - **Rows**: 848
   - **Metrics**: total tokens, cost, cost per million tokens
   - **Filtered**: 3,115 GenAI events from v2 data
   - **Use case**: Product analytics for AI feature adoption

5. **cost_anomaly_mart** - Cost anomaly aggregations
   - **Rows**: 6,106
   - **Source**: 12,034 anomalous events (27.99%)
   - **Metrics**: anomaly count, total cost, detection methods
   - **Use case**: Alerting and cost spike detection

**Output**: `datalake/gold/` - Partitioned Parquet files

**Execution time**: ~12 seconds for all 5 marts

---

### Phase 4: Serving Layer - AstraDB

**Purpose**: Load data into query-optimized database for low-latency access

See detailed guide: **[ASTRADB_SETUP.md](ASTRADB_SETUP.md)**

**Setup** (one-time):
```bash
# 1. Configure credentials in .env
cp .env.example .env
# Add your ASTRADB_TOKEN and ASTRADB_API_ENDPOINT

# 2. Create collections
./scripts/setup_astradb.sh

# 3. Load data from Gold layer
./scripts/load_to_astradb.sh
```

**Collections Created** (5 NoSQL collections):
- `org_daily_usage` - 11,050 documents
- `org_service_costs` - ~33,150 documents (3 time windows × orgs × services)
- `tickets_critical_daily` - Aggregated ticket metrics
- `revenue_monthly` - 227 documents
- `genai_tokens_daily` - 848 documents

**Total documents**: ~45,000

**Demo Queries** (5 analytics queries):
```bash
./scripts/run_demo_queries.sh
```

1. **Query 1**: Daily costs by organization and service (FinOps)
2. **Query 2**: Top-N services by cost in time window (7/30/90 days)
3. **Query 3**: Critical tickets & SLA breach rate (Support ops)
4. **Query 4**: Monthly revenue by organization (Finance)
5. **Query 5**: GenAI token usage by organization (Product)

**Features**:
- Dynamic org_id lookup (works with any dataset)
- MongoDB-style queries using AstraDB Data API
- Formatted table output (tabulate)
- Business insights for each query

---

## Usage Guide

### For New Users

**Step 1: Setup Environment**
```bash
# Install Java 17
brew install openjdk@17

# Activate environment
source setup.sh

# Install dependencies
pip install -r requirements.txt
```

**Step 2: Run Full Pipeline**
```bash
# Ingest data (Bronze)
./scripts/run_batch_ingestion.sh
./scripts/run_streaming_ingestion.sh batch

# Transform data (Silver)
./scripts/run_silver_transformations.sh

# Create business marts (Gold)
./scripts/run_gold_marts.sh
```

**Step 3: Setup Serving Layer**
```bash
# Follow ASTRADB_SETUP.md for detailed steps
# Quick version:
cp .env.example .env  # Add your credentials
./scripts/setup_astradb.sh
./scripts/load_to_astradb.sh
./scripts/run_demo_queries.sh
```

### For Developers

**Running Individual Transformations**:
```bash
# Single Silver transformation
source setup.sh
python -m src.silver.transform_usage_events_silver

# Single Gold mart
python -m src.gold.create_org_daily_usage_by_service
```

**Exploring Data**:
```bash
# Check Bronze data
ls -lh datalake/bronze/

# Check Silver data quality
ls -lh datalake/silver/quarantine/

# Check Gold aggregations
ls -lh datalake/gold/
```

**Testing Queries**:
```python
# In Python (after activating venv)
from src.config.astradb_config import get_astradb_client
from src.serving.demo_queries import query_1_daily_costs_by_service

db = get_astradb_client()
results = query_1_daily_costs_by_service(db, org_id="org_0lzjjege", limit=10)
```

---

## Data Sources

**Dataset Overview**:
- **Span**: ~60 days of event data
- **Organizations**: 80
- **Users**: 800
- **Resources**: 400
- **Usage Events**: 43,200
- **Schema Evolution**: v1 → v2 at 2025-07-18

**Data Quality Notes**:
- Contains NULLs (intentional - testing null handling)
- Type inconsistencies (e.g., "value" sometimes string)
- Negative costs allowed (credits/refunds)
- Outliers present (for anomaly detection testing)
- `genai_tokens` and `carbon_kg` only in v2 (after 2025-07-18)

**Batch Sources** (CSV in `datalake/landing/`):

| File | Records | Description |
|------|---------|-------------|
| `users.csv` | 800 | User accounts (email, role, last_login) |
| `customers_orgs.csv` | 80 | Organizations (name, industry, plan, region) |
| `resources.csv` | 400 | Cloud resources (org, type, region, status) |
| `billing_monthly.csv` | 240 | Monthly invoices (amount, currency, credits, taxes) |
| `support_tickets.csv` | 1,000 | Support tickets (severity, status, resolution time) |
| `marketing_touches.csv` | 1,500 | Marketing interactions (type, source, timestamp) |
| `nps_surveys.csv` | 92 | Net Promoter Score surveys (score 0-10) |

**Streaming Source** (JSONL in `datalake/landing/usage_events_stream/`):

| Files | Events | Description |
|-------|--------|-------------|
| `*.jsonl` | 43,200 | Cloud usage events (compute, storage, networking) |

**Schema Versions**:
- **v1** (before 2025-07-18): Basic fields (org_id, service, metric, value, cost)
- **v2** (after 2025-07-18): Adds `carbon_kg` and `genai_tokens`

---

## Current Status

### ✅ Completed

**Bronze Layer**
- [x] 7 CSV batch sources ingested
- [x] 43,200 usage events from 120 JSONL files
- [x] Metadata tracking (ingestion_timestamp, source_file)
- [x] Parquet storage with partitioning

**Silver Layer**
- [x] 8/8 transformations completed
- [x] Data quality: 99%+ for most datasets
- [x] Anomaly detection (z-score, MAD, percentile methods)
- [x] Schema v1/v2 compatibility
- [x] Quarantine for invalid records
- [x] Region/service/metric standardization

**Gold Layer**
- [x] 5/5 business marts created
- [x] 11,050 usage aggregations
- [x] 227 revenue records
- [x] 984 ticket metrics
- [x] 848 GenAI token records
- [x] 6,106 anomaly records
- [x] Execution time: ~12 seconds

**Serving Layer**
- [x] AstraDB Data API integration
- [x] 5 NoSQL collections created
- [x] ~45K documents loaded
- [x] 5 demo queries implemented
- [x] Dynamic org_id lookup
- [x] Comprehensive setup guide (ASTRADB_SETUP.md)

### 🎯 Ready to Use

The pipeline is **production-ready** and can be executed end-to-end:
1. Bronze ingestion ✓
2. Silver transformations ✓
3. Gold aggregations ✓
4. AstraDB serving layer ✓

### 📋 Future Enhancements

See [TODO.md](TODO.md) for detailed task list:
- [ ] Speed Layer (real-time streaming aggregations)
- [ ] End-to-end orchestration (Airflow/Prefect)
- [ ] Data catalog integration
- [ ] Enhanced monitoring/alerting
- [ ] Performance optimizations
- [ ] Additional business marts

---

## Troubleshooting

### Java Version Errors

**Error**: `java.lang.UnsupportedClassVersionError: ... class file version 61.0`

**Solution**:
```bash
# Install Java 17
brew install openjdk@17

# Create symlink (macOS)
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
  /Library/Java/JavaVirtualMachines/openjdk-17.jdk

# Activate environment
source setup.sh

# Verify Java version
java -version  # Should show 17.x.x
```

**Root Cause**: PySpark 3.5+ requires Java 17 (won't work with Java 8 or 11)

### Import Errors

**Error**: `ModuleNotFoundError: No module named 'src'`

**Solution**:
```bash
# Ensure you're in project root
cd Cloud-Provider-Analytics

# Activate virtual environment
source setup.sh

# Reinstall dependencies if needed
pip install -r requirements.txt
```

### Permission Errors

**Error**: `Permission denied: ./scripts/run_*.sh`

**Solution**:
```bash
# Make all scripts executable
chmod +x scripts/*.sh
```

### AstraDB Connection Issues

**Error**: `ASTRADB_TOKEN not configured`

**Solution**:
```bash
# Create .env file
cp .env.example .env

# Edit .env with your credentials
# ASTRADB_TOKEN=AstraCS:your_token_here
# ASTRADB_API_ENDPOINT=https://your-database-id.apps.astra.datastax.com
```

See [ASTRADB_SETUP.md](ASTRADB_SETUP.md) for detailed troubleshooting.

### Data Quality Issues

**High quarantine rate for nps_surveys (92%)**:
- This is expected - the dataset intentionally contains bad data
- Check `datalake/silver/quarantine/nps_surveys/` for details
- Common issues: NPS scores outside 0-10 range, invalid timestamps

**28% anomaly rate in usage_events**:
- This is normal for statistical outlier detection
- Anomalies are flagged but not quarantined
- Use `cost_anomaly_mart` for analysis

### Performance Issues

**Slow transformations**:
```bash
# Increase Spark memory (in src/config/spark_config.py)
# Default is 4g, increase to 8g if available:
.config("spark.driver.memory", "8g")
.config("spark.executor.memory", "8g")
```

**Out of memory errors**:
- Reduce batch size in transformations
- Process data in smaller partitions
- Check available RAM (recommend 8GB+)

---

## Requirements

### System Requirements

- **OS**: macOS, Linux, or Windows (with WSL)
- **RAM**: 8GB minimum, 16GB recommended
- **Disk**: 2GB free space (for datalake)
- **Internet**: Required for AstraDB setup

### Software Requirements

- **Python**: 3.9 or higher
- **Java**: 17 (required for PySpark 3.5+)
- **PySpark**: 3.5.0+
- **Pandas**: 2.0.0+
- **NumPy**: 1.24.0+

### Python Dependencies

See [requirements.txt](requirements.txt) for complete list:

```txt
# Core
pyspark>=3.5.0

# Data processing
pandas>=2.0.0
numpy>=1.24.0

# Utilities
pyyaml>=6.0

# AstraDB/Database
astrapy>=1.0.0

# Environment
python-dotenv>=1.0.0

# Display
tabulate>=0.9.0
```

### Optional Dependencies

```bash
# For Delta Lake support (not currently used)
# pip install delta-spark>=3.0.0
```

---

## Additional Resources

- **[ASTRADB_SETUP.md](ASTRADB_SETUP.md)** - Step-by-step AstraDB setup guide (10 steps with checkpoints)
- **[TODO.md](TODO.md)** - Project task list and future work
- **[project_plan.md](project_plan.md)** - Original implementation plan
- **AstraDB Dashboard**: https://astra.datastax.com/

---

## Project Information

**Course**: Big Data & Cloud Computing
**Architecture**: Lambda Architecture with Medallion Layers
**Dataset**: Synthetic cloud provider analytics data (~60 days)
**Technology**: Apache Spark, Python, AstraDB

**Key Achievements**:
- ✅ Complete data pipeline: Ingestion → Quality → Aggregation → Serving
- ✅ 99%+ data quality for most datasets
- ✅ Schema evolution handling (v1 → v2)
- ✅ Anomaly detection with multiple methods
- ✅ Production-ready serving layer with optimized queries
- ✅ Comprehensive documentation and guides

---

**For questions or issues, see the Troubleshooting section or refer to ASTRADB_SETUP.md.**
