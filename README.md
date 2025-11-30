# Cloud Provider Analytics - Big Data Project

**Lambda Architecture data pipeline** for Cloud Provider Analytics with **Medallion Architecture** layers (Bronze → Silver → Gold → Serving).

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Demo Execution](#demo-execution)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
- [Data Sources](#data-sources)
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
                         LANDING ZONE
                    (CSV + JSONL raw files)
                              |
         +--------------------+--------------------+
         |                                         |
         v                                         v
+------------------+                    +------------------+
|   BATCH PATH     |                    |   SPEED PATH     |
|                  |                    |                  |
| Master Data:     |                    | usage_events:    |
|  - users         |                    |  - Structured    |
|  - customers_orgs|                    |    Streaming     |
|  - resources     |                    |  - Watermark     |
|                  |                    |    (10 min)      |
| Fact Data:       |                    |  - Dedup by      |
|  - billing       |                    |    event_id      |
|  - tickets       |                    |  - Checkpoint    |
|  - marketing     |                    |                  |
|  - nps_surveys   |                    |                  |
+--------+---------+                    +--------+---------+
         |                                       |
         v                                       v
+------------------+                    +------------------+
|     BRONZE       |                    |   SPEED LAYER    |
|  (Raw Parquet)   |                    | (Real-time Views)|
|                  |                    |                  |
| + ingestion_ts   |                    | Tumbling (5min)  |
| + source_file    |                    | Sliding (15min)  |
| + partitioned    |                    | Cost Alerts      |
+--------+---------+                    | Hourly Summary   |
         |                              +--------+---------+
         v                                       |
+------------------+                             |
|     SILVER       |                             |
| (Validated)      |                             |
|                  |                             |
| Data Quality:    |                             |
|  - Type casting  |                             |
|  - Null handling |                             |
|  - Schema v1/v2  |                             |
|  - Anomaly flags |                             |
|  - Quarantine    |                             |
+--------+---------+                             |
         |                                       |
         v                                       |
+------------------+                             |
|      GOLD        |<----------------------------+
| (Business Marts) |       (merge/combine)
|                  |
| 5 Marts:         |
|  - org_daily_usage_by_service
|  - revenue_by_org_month
|  - cost_anomaly_mart
|  - tickets_by_org_date
|  - genai_tokens_by_org_date
+--------+---------+
         |
         v
+------------------+
|    SERVING       |
|    (AstraDB)     |
|                  |
| Query-first      |
| table design     |
| 6 collections    |
+------------------+
```

### Data Flow Summary

| Path | Sources | Processing | Output |
|------|---------|------------|--------|
| **Batch** | 7 CSV files (master + fact) | Bronze -> Silver -> Gold | Historical views |
| **Speed** | JSONL streaming events | Structured Streaming + Windows | Real-time metrics |
| **Serving** | Gold marts | Load to AstraDB | Low-latency queries |

### Technology Stack

- **Storage**: Parquet (Data Lake)
- **Processing**: Apache Spark (PySpark, Structured Streaming)
- **Database**: AstraDB (Data API - NoSQL collections)
- **Language**: Python 3.9+
- **Runtime**: Java 17 (required for PySpark 3.5+)

---

## Demo Execution

### Full Demo (Recommended)

```bash
# 1. Setup environment
source setup.sh

# 2. Run full pipeline (Bronze -> Silver -> Gold)
python -m src.orchestration.pipeline_orchestrator --full

# 3. Load to AstraDB serving layer (requires config)
python -m src.orchestration.pipeline_orchestrator --full --serving

# 4. Run the 5 required demo queries
python -m src.serving.demo_queries

# 5. (Optional) Run speed layer streaming for 60 seconds
python -m src.speed.streaming_aggregations --mode all --duration 60
```

### Expected Output

| Stage | Records | Time |
|-------|---------|------|
| Bronze (Batch) | ~4,000 records (7 sources) | ~15s |
| Bronze (Streaming) | ~43,000 events | ~20s |
| Silver | ~47,000 validated records | ~25s |
| Gold | ~20,000 aggregated records | ~15s |
| **Total** | **~47,000 source -> ~20,000 marts** | **~75s** |

### Demo Queries (5 Required)

| # | Query | Use Case |
|---|-------|----------|
| 1 | Daily costs by org and service | FinOps cost tracking |
| 2 | Top-N services by cost (14 days) | Cost optimization |
| 3 | Critical tickets + SLA breach rate | Support operations |
| 4 | Monthly revenue (credits/taxes/USD) | Finance reporting |
| 5 | GenAI tokens + estimated cost | Product analytics |
| 6 | Cost anomalies (3 methods) | Alerting (bonus) |

### Pipeline Options

```bash
python -m src.orchestration.pipeline_orchestrator --full           # Full batch pipeline
python -m src.orchestration.pipeline_orchestrator --full --serving # + AstraDB load
python -m src.orchestration.pipeline_orchestrator --bronze-only    # Bronze only
python -m src.orchestration.pipeline_orchestrator --silver-only    # Silver only
python -m src.orchestration.pipeline_orchestrator --gold-only      # Gold only
```

---

## Quick Start

### 1. Initial Setup

```bash
cd Cloud-Provider-Analytics

# Install Java 17 (macOS with Homebrew)
brew install openjdk@17
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
  /Library/Java/JavaVirtualMachines/openjdk-17.jdk

# Activate environment (sets Java 17 + Python venv)
source setup.sh
```

### 2. Run Full Pipeline (One Command)

```bash
source setup.sh
python -m src.orchestration.pipeline_orchestrator --full
```

This executes: Bronze (batch + streaming) -> Silver -> Gold

### 3. Run Tests

```bash
python -m pytest tests/ -v
```

### 4. Setup Serving Layer (AstraDB)

```bash
# Configure credentials in src/config/astradb_config.py or .env
# Then load data and run queries:
python -m src.serving.load_to_astradb
python -m src.serving.demo_queries
```

---

## Project Structure

```
Cloud-Provider-Analytics/
├── datalake/
│   ├── landing/           # Raw CSV + JSONL files
│   ├── bronze/            # Ingested Parquet (gitignored)
│   ├── silver/            # Validated + quarantine (gitignored)
│   ├── gold/              # Business marts (gitignored)
│   └── speed/             # Real-time aggregations (gitignored)
├── src/
│   ├── ingestion/         # Bronze layer (batch + streaming)
│   ├── transformations/   # Silver layer
│   ├── aggregations/      # Gold layer (5 marts)
│   ├── serving/           # AstraDB integration
│   ├── speed/             # Speed layer streaming
│   ├── orchestration/     # Pipeline orchestrator
│   ├── common/            # Data quality, lineage, idempotency
│   └── config/            # Spark, paths, AstraDB config
├── tests/                 # Unit and integration tests
├── scripts/               # Shell scripts for execution
├── setup.sh               # Environment activation
└── requirements.txt       # Python dependencies
```

---

## Data Pipeline

| Layer | Input | Output | Key Features |
|-------|-------|--------|--------------|
| **Bronze** | 7 CSV + 83 JSONL | Raw Parquet | Partitioning, metadata, schema enforcement |
| **Silver** | Bronze Parquet | Validated Parquet | Type casting, null handling, anomaly detection, quarantine |
| **Gold** | Silver Parquet | 5 Business Marts | Aggregations, enrichment, idempotent writes |
| **Serving** | Gold Parquet | AstraDB Collections | Query-first design, 6 collections |
| **Speed** | JSONL Stream | Real-time Parquet | Windows, watermarks, checkpointing |

---

## Data Sources

| Source | Type | Records | Description |
|--------|------|---------|-------------|
| `users.csv` | Batch | 800 | User accounts |
| `customers_orgs.csv` | Batch | 80 | Organizations |
| `resources.csv` | Batch | 400 | Cloud resources |
| `billing_monthly.csv` | Batch | 240 | Monthly invoices |
| `support_tickets.csv` | Batch | 1,000 | Support tickets |
| `marketing_touches.csv` | Batch | 1,500 | Marketing events |
| `nps_surveys.csv` | Batch | 92 | NPS surveys |
| `usage_events_stream/*.jsonl` | Stream | 43,200 | Usage events (v1/v2 schema) |

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Java version error | Install Java 17: `brew install openjdk@17` then `source setup.sh` |
| Module not found | Run `source setup.sh` to activate venv |
| Permission denied | Run `chmod +x scripts/*.sh` |
| AstraDB connection | Configure credentials in `.env` or `src/config/astradb_config.py` |
| Spark memory issues | Increase memory in `src/config/spark_config.py` |

---

## Requirements

| Component | Version |
|-----------|---------|
| Python | 3.9+ |
| Java | 17 (required for PySpark) |
| PySpark | 4.x |
| AstraDB | astrapy 1.0+ |
| RAM | 8GB minimum |

---

## Team

- Bengolea (63515)
- Ijjas (63555)
- Lopez Menardi (62707)

**Course**: 72.80 Big Data - ITBA

