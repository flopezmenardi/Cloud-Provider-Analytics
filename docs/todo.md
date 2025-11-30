# Cloud Provider Analytics - Complete Implementation Checklist

## Project Overview
**Course:** 72.80 Big Data - ITBA  
**Team:** Bengolea (63515), Ijjas (63555), Lopez Menardi (62707)  
**Architecture Pattern:** Lambda Architecture  
**Date:** October 2025

---

## 📁 1. DATA SOURCES VERIFICATION

### 1.1 Landing Zone - Batch Files (CSV)
- [ ] `customers_orgs.csv` - Customer/tenant data
  - [ ] Verify fields: industry, region, plan, NPS
  - [ ] Document null patterns and noisy values
- [ ] `users.csv` - Users per organization
  - [ ] Verify fields: roles, activity, timestamps
  - [ ] Check for orphaned users (no org_id match)
- [ ] `resources.csv` - Cloud resources by service
  - [ ] Verify services: compute, storage, database, networking, analytics, genai
  - [ ] Check resource_id uniqueness
- [ ] `support_tickets.csv` - Support tickets
  - [ ] Verify fields: category, severity, SLA, CSAT
  - [ ] Document null patterns in CSAT
  - [ ] Check open/closed status distribution
- [ ] `marketing_touches.csv` - Marketing touches
  - [ ] Verify fields: channels, conversions
- [ ] `nps_surveys.csv` - NPS surveys temporal
  - [ ] Verify fields: org_id, timestamp, score
  - [ ] Document null patterns
- [ ] `billing_monthly.csv` - 3-month billing data
  - [ ] Verify fields: taxes, credits/adjustments, currencies
  - [ ] Check currency conversion requirements

### 1.2 Landing Zone - Streaming Files (JSONL)
- [ ] `usage_events_stream/*.jsonl` - Usage event feed
  - [ ] Confirm 10-20 files available for demo subset
  - [ ] Document full file count for future backfill
  - [ ] Identify schema_version=1 files (first ~15 days)
  - [ ] Identify schema_version=2 files (~45 days mark onward)
  - [ ] Verify new v2 fields: `carbon_kg`, `genai_tokens`
  - [ ] Document null patterns
  - [ ] Document negative `cost_usd_increment` occurrences
  - [ ] Document cost spikes (outliers)

---

## 🏗️ 2. ARCHITECTURE IMPLEMENTATION

### 2.1 Lambda Architecture Justification
- [ ] Document why Lambda was chosen over Kappa
  - [ ] Historical accuracy for batch (CRM, billing)
  - [ ] Low latency for streaming (usage, costs)
  - [ ] Layer separation for corrections/replays
  - [ ] Schema evolution handling
  - [ ] Trade-off: logic duplication vs. resilience

### 2.2 Data Lake Zones Structure
- [ ] **Landing (Raw Immutable)**
  - [ ] Ensure original files are NOT modified
  - [ ] Document file locations
- [ ] **Bronze (Raw Standard)**
  - [ ] Create Parquet storage location
  - [ ] Implement partitioning strategy (date=/service=)
  - [ ] Add ingestion metadata columns: `ingest_ts`, `source_file`
- [ ] **Silver (Conformed)**
  - [ ] Create Parquet storage location
  - [ ] Implement partitioning strategy
  - [ ] Create quarantine zone for bad data
- [ ] **Gold (Business Marts)**
  - [ ] Create storage for each mart table
  - [ ] Create views/expositions for BI tools

---

## 📥 3. INGESTION LAYER

### 3.1 Batch Ingestion (CSV → Bronze)
- [ ] Read `customers_orgs.csv`
  - [ ] Define explicit schema
  - [ ] Add `ingest_ts` timestamp
  - [ ] Add `source_file` column
  - [ ] Write to Bronze as Parquet
- [ ] Read `users.csv`
  - [ ] Define explicit schema
  - [ ] Add ingestion metadata
  - [ ] Write to Bronze as Parquet
- [ ] Read `resources.csv`
  - [ ] Define explicit schema
  - [ ] Add ingestion metadata
  - [ ] Write to Bronze as Parquet
- [ ] Read `support_tickets.csv`
  - [ ] Define explicit schema
  - [ ] Add ingestion metadata
  - [ ] Write to Bronze as Parquet
- [ ] Read `marketing_touches.csv`
  - [ ] Define explicit schema
  - [ ] Add ingestion metadata
  - [ ] Write to Bronze as Parquet
- [ ] Read `nps_surveys.csv`
  - [ ] Define explicit schema
  - [ ] Add ingestion metadata
  - [ ] Write to Bronze as Parquet
- [ ] Read `billing_monthly.csv`
  - [ ] Define explicit schema
  - [ ] Add ingestion metadata
  - [ ] Write to Bronze as Parquet
- [ ] Implement partitioning for all Bronze tables

### 3.2 Streaming Ingestion (JSONL → Bronze)
- [ ] Configure Structured Streaming source
  - [ ] Set source directory path
  - [ ] Configure `maxFilesPerTrigger` for rate limiting
- [ ] Define explicit schema for usage events
  - [ ] Handle schema_version=1 fields
  - [ ] Handle schema_version=2 fields (carbon_kg, genai_tokens)
- [ ] Implement `withWatermark` on `event_time`
  - [ ] Define watermark duration (X minutes)
- [ ] Implement `dropDuplicates` by `event_id`
- [ ] Configure checkpointing
  - [ ] Set checkpoint location
  - [ ] Verify checkpoint recovery works
- [ ] Handle late data appropriately
- [ ] Partition output by `date` and/or `service`

---

## ✅ 4. DATA QUALITY LAYER

### 4.1 Type Consistency Rules
- [ ] `value` field: Cast string to numeric with fallback
- [ ] `cost_usd_increment`: Cast to decimal/double
- [ ] Timestamps: Parse and validate formats
- [ ] Numeric fields: Handle nulls with defaults or flags

### 4.2 Validation Rules (Minimum Required)
- [ ] **Rule 1:** `event_id` NOT NULL and unique per window
  - [ ] Implement validation check
  - [ ] Route failures to quarantine
- [ ] **Rule 2:** `cost_usd_increment` ∈ [-0.01, +∞)
  - [ ] Implement range validation
  - [ ] Flag anomalies if > p99 * X (define X)
  - [ ] Route severe failures to quarantine
- [ ] **Rule 3:** `unit` NOT NULL when `value` NOT NULL
  - [ ] Implement validation or imputation logic
- [ ] **Rule 4:** `schema_version` handled and compatibilized
  - [ ] v1 → v2 compatibility transformation
  - [ ] Fill nulls for new v2 columns appropriately

### 4.3 Anomaly Detection (Choose 3 Methods)
- [ ] **Method 1:** Z-Score detection
  - [ ] Define threshold (e.g., |z| > 3)
  - [ ] Apply to cost fields
- [ ] **Method 2:** MAD (Median Absolute Deviation)
  - [ ] Define threshold
  - [ ] Apply to cost fields
- [ ] **Method 3:** Percentile-based (p-tiles)
  - [ ] Define threshold (e.g., > p99)
  - [ ] Apply to cost fields
- [ ] Add `is_anomaly` flag column
- [ ] Add `anomaly_score` column
- [ ] Add `anomaly_method` column

### 4.4 Quarantine Zone
- [ ] Create quarantine Parquet storage
- [ ] Route failed records with:
  - [ ] Original record data
  - [ ] Failure reason(s)
  - [ ] Failure timestamp
  - [ ] Source file reference

---

## 🔄 5. TRANSFORMATIONS (SILVER LAYER)

### 5.1 Normalization
- [ ] Standardize numeric formats
- [ ] Standardize date formats
- [ ] Normalize region values (case, spelling)
- [ ] Normalize service values (case, spelling)
- [ ] Handle currency normalization to USD

### 5.2 Enrichment - Joins with Dimensions
- [ ] Join usage events with `organizations`
  - [ ] Add org attributes (industry, region, plan)
- [ ] Join usage events with `users`
  - [ ] Add user attributes (role)
- [ ] Join usage events with `resources`
  - [ ] Add resource attributes (service type)
- [ ] Handle missing dimension keys gracefully

### 5.3 Schema Version Compatibilization
- [ ] Create unified schema for v1 and v2
- [ ] For v1 records:
  - [ ] Set `carbon_kg` = 0 or NULL with flag
  - [ ] Set `genai_tokens` = 0 or NULL with flag
- [ ] Document compatibilization logic

### 5.4 Calculated Features
- [ ] `daily_cost_usd` by org/service
- [ ] `requests` count by org/service
- [ ] `cpu_hours` aggregation
- [ ] `storage_gb_hours` aggregation
- [ ] `genai_tokens` aggregation (when available)
- [ ] `carbon_kg` aggregation (when available)

### 5.5 Silver Output
- [ ] Write Silver tables as Parquet
- [ ] Implement appropriate partitioning
- [ ] Document Silver schema

---

## 📊 6. GOLD LAYER (BUSINESS MARTS)

### 6.1 FinOps Marts
- [ ] **`org_daily_usage_by_service`**
  - [ ] Grain: org_id, usage_date, service
  - [ ] Metrics: cost_usd, requests, cpu_hours, storage_gb_hours, genai_tokens, carbon_kg
  - [ ] Partition key design for Cassandra
- [ ] **`revenue_by_org_month`**
  - [ ] Grain: org_id, month
  - [ ] Metrics: revenue_usd, credits, taxes, fx_rate_applied
  - [ ] Normalize to USD
- [ ] **`cost_anomaly_mart`**
  - [ ] Grain: org_id, date, service
  - [ ] Fields: anomaly_score, anomaly_flag, anomaly_method

### 6.2 Support Marts
- [ ] **`tickets_by_org_date`**
  - [ ] Grain: org_id, date, severity
  - [ ] Metrics: ticket_count, sla_breach_count, sla_breach_rate, avg_csat
  - [ ] Handle null CSAT values

### 6.3 Product/Usage Marts (GenAI)
- [ ] **`genai_tokens_by_org_date`**
  - [ ] Grain: org_id, date
  - [ ] Metrics: total_tokens, estimated_cost_usd
  - [ ] Handle periods without genai data (show 0 or null with note)

---

## 🗄️ 7. SERVING LAYER (CASSANDRA/ASTRADB)

### 7.1 AstraDB Setup
- [ ] Create AstraDB account/instance
- [ ] Create keyspace: `cloud_analytics`
- [ ] Configure connection credentials
- [ ] Test connectivity from Colab

### 7.2 Table Design (Query-First Modeling)
- [ ] **Table for Query 1:** Daily costs/requests by org and service
  - [ ] Partition key: (org_id, service)
  - [ ] Clustering key: usage_date DESC
  - [ ] CREATE TABLE statement documented
- [ ] **Table for Query 2:** Top-N services by cost for org
  - [ ] Partition key: (org_id)
  - [ ] Clustering key: (period_end, total_cost DESC)
  - [ ] Or: materialized view approach
- [ ] **Table for Query 3:** Critical tickets and SLA breach by date
  - [ ] Partition key: appropriate for 30-day range
  - [ ] Clustering key: date DESC
- [ ] **Table for Query 4:** Monthly revenue by org
  - [ ] Partition key: (org_id)
  - [ ] Clustering key: month DESC
- [ ] **Table for Query 5:** GenAI tokens by org and date
  - [ ] Partition key: (org_id)
  - [ ] Clustering key: date DESC

### 7.3 Data Loading
- [ ] Implement Spark Cassandra Connector OR
- [ ] Implement foreachBatch + Python driver approach
- [ ] Load `org_daily_usage_by_service`
- [ ] Load `revenue_by_org_month`
- [ ] Load `tickets_by_org_date`
- [ ] Load `genai_tokens_by_org_date`
- [ ] Load `cost_anomaly_mart` (if separate table)

---

## 🔁 8. IDEMPOTENCY & RELIABILITY

### 8.1 Streaming Idempotency
- [ ] Configure checkpointing for Structured Streaming
- [ ] Verify checkpoint recovery works after restart
- [ ] Use natural keys for deduplication
- [ ] Implement upsert logic (not just append)

### 8.2 Batch Idempotency
- [ ] Design for re-run without duplicates
- [ ] Use overwrite mode OR implement merge logic
- [ ] Document re-processing procedure

### 8.3 Cassandra Upserts
- [ ] Design tables to handle upserts naturally
- [ ] Verify re-loading same data doesn't duplicate

---

## ⚡ 9. PERFORMANCE OPTIMIZATION

### 9.1 Partitioning Strategy
- [ ] Bronze: Partition by `date` (and optionally `service`)
- [ ] Silver: Partition by `date` and `service`
- [ ] Gold: Partition appropriate for Cassandra loading
- [ ] Document partition sizes (avoid too small/large)

### 9.2 Spark Optimization
- [ ] Use `coalesce` to reduce output files when appropriate
- [ ] Use `repartition` for parallelism when needed
- [ ] Cache/persist DataFrames when reused
- [ ] Broadcast small dimension tables in joins

### 9.3 Streaming Optimization
- [ ] Set appropriate `maxFilesPerTrigger`
- [ ] Set appropriate trigger interval
- [ ] Monitor memory usage during streaming

---

## 📝 10. DOCUMENTATION

### 10.1 Architecture Diagram
- [ ] High-level Lambda architecture diagram
- [ ] Data flow diagram (Landing → Bronze → Silver → Gold → Serving)
- [ ] Include: sources, technologies, zones

### 10.2 Data Dictionary
- [ ] Document Bronze table schemas
- [ ] Document Silver table schemas
- [ ] Document Gold mart schemas
- [ ] Document Cassandra table schemas

### 10.3 Decision Log
- [ ] Lambda vs Kappa decision
- [ ] Partitioning strategy decisions
- [ ] Cassandra key design decisions
- [ ] Anomaly threshold decisions
- [ ] Schema compatibilization decisions
- [ ] Watermark duration decision

### 10.4 Trade-offs Documentation
- [ ] Logic duplication in Lambda
- [ ] Streaming subset vs full backfill
- [ ] Performance vs. complexity choices

---

## 🎯 11. DEMO QUERIES (CQL)

### 11.1 Query 1: Daily Costs and Requests by Org/Service
- [ ] Write CQL query for date range filter
- [ ] Execute and capture results
- [ ] Document sample output

### 11.2 Query 2: Top-N Services by Cost (14 days)
- [ ] Write CQL query with LIMIT N
- [ ] Execute and capture results
- [ ] Document sample output

### 11.3 Query 3: Critical Tickets & SLA Breach Rate (30 days)
- [ ] Write CQL query filtering by severity
- [ ] Calculate breach rate
- [ ] Execute and capture results

### 11.4 Query 4: Monthly Revenue with Credits/Taxes (USD)
- [ ] Write CQL query
- [ ] Show normalized USD values
- [ ] Execute and capture results

### 11.5 Query 5: GenAI Tokens and Estimated Cost by Date
- [ ] Write CQL query
- [ ] Handle case where no genai data exists
- [ ] Execute and capture results
- [ ] Add methodological note if applicable

---

## 📦 12. DELIVERABLES

### 12.1 Presentation
- [ ] Architecture overview slide
- [ ] Technologies used slide
- [ ] Data Lake zones explanation
- [ ] Lambda pattern justification
- [ ] Pipeline stages breakdown
- [ ] Quality rules explanation
- [ ] Cassandra modeling explanation
- [ ] Demo queries results
- [ ] Challenges and decisions

### 12.2 Video
- [ ] Script/outline prepared
- [ ] Show running pipeline (Spark)
- [ ] Show ETL results
- [ ] Show Cassandra queries
- [ ] Explain architecture decisions
- [ ] Professional storytelling
- [ ] Duration: appropriate length

### 12.3 Code (Notebook)
- [ ] Clean, commented code
- [ ] Logical section organization
- [ ] All queries with output captures
- [ ] README or introductory markdown cells

---

## ⚠️ 13. RISKS & MITIGATIONS (VERIFY IMPLEMENTATION)

### 13.1 Volume/Velocity
- [ ] `maxFilesPerTrigger` configured
- [ ] Partitioning by date/service implemented
- [ ] Tested with demo subset (10-20 files)

### 13.2 Quality Issues
- [ ] String casting with fallback implemented
- [ ] Cost range rules [-0.01, +∞) implemented
- [ ] Outlier flags added
- [ ] Quarantine zone functional

### 13.3 Schema Evolution
- [ ] v1/v2 compatibility path implemented
- [ ] Backfill procedure documented (future work)

### 13.4 Late Events
- [ ] Watermark configured
- [ ] Windowed processing verified
- [ ] Idempotent writes confirmed

### 13.5 Serving Latency
- [ ] Query-first design validated
- [ ] Partition keys appropriate for queries
- [ ] Clustering keys support range scans

### 13.6 Lambda Complexity
- [ ] Shared transformation libraries (if applicable)
- [ ] Schema contracts documented

---

## 🧪 14. TESTING CHECKLIST

### 14.1 Unit Tests
- [ ] Schema parsing functions
- [ ] Quality rule functions
- [ ] Anomaly detection functions
- [ ] Transformation functions

### 14.2 Integration Tests
- [ ] Full batch pipeline end-to-end
- [ ] Full streaming pipeline (subset)
- [ ] Bronze → Silver → Gold flow
- [ ] Gold → Cassandra load
- [ ] All 5 CQL queries execute successfully

### 14.3 Edge Cases
- [ ] Empty files handled
- [ ] All-null records handled
- [ ] Extremely large values handled
- [ ] Negative costs handled
- [ ] Missing dimension keys handled
- [ ] Schema v1 only data works
- [ ] Schema v2 only data works
- [ ] Mixed v1/v2 data works

---

## 📅 15. MILESTONE TRACKING

| Milestone | Description | Status | Notes |
|-----------|-------------|--------|-------|
| M0 | Kickoff & data contract | ⬜ | |
| M1 | Ingesta Batch → Bronze | ⬜ | |
| M2 | Silver (casts/joins, v1/v2, 3 rules + quarantine) | ⬜ | |
| M3 | Streaming (10-20 jsonl: schema+watermark+dedup+checkpoint) | ⬜ | |
| M4 | Gold (5 business mart tables) | ⬜ | |
| M5 | Serving Cassandra (keyspace + 5 tables) | ⬜ | |
| M6 | 5 CQL queries + evidence capture | ⬜ | |
| M7 | Evidence (counts, timing, decisions) | ⬜ | |

---

## 📋 FINAL CHECKLIST BEFORE SUBMISSION

- [ ] All code runs without errors in Google Colab
- [ ] All 5 queries return valid results
- [ ] Screenshots/output captures included
- [ ] Architecture diagram complete
- [ ] Video recorded and reviewed
- [ ] Presentation polished
- [ ] Documentation complete
- [ ] Team reviewed all deliverables
- [ ] Submitted before deadline

---

## 💡 NOTES

### Optional Columns Handling
If `genai_tokens` or `carbon_kg` are not present in the sampled period:
- Gold views should show 0 or NULL
- Include methodological note explaining data availability

### Future Work (Documented)
- Full backfill of streaming events
- Additional anomaly detection methods
- SCD Type 2 for dimension changes
- Real-time dashboard integration

---

*Last updated: [DATE]*  
*Checklist version: 1.0*