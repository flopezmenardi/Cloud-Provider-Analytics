# Cloud Provider Analytics - Implementation Plan

## Architecture Overview

- **Architecture Pattern**: Lambda Architecture (Batch Layer + Speed Layer + Serving Layer)
- **Data Architecture**: Medallion Architecture (Bronze → Silver → Gold)
- **Ingestion**: Dual-mode (Batch for CSV files, Streaming for JSONL events)
- **Technology Stack**: Apache Spark (PySpark/Spark Structured Streaming), Delta Lake (or Parquet), Data Lake Storage

## Data Sources

### Batch Sources (CSV files in `datalake/landing/`)

1. `users.csv` - User information
2. `customers_orgs.csv` - Organization/customer data
3. `resources.csv` - Resource definitions
4. `billing_monthly.csv` - Monthly billing invoices
5. `support_tickets.csv` - Support ticket data
6. `marketing_touches.csv` - Marketing interaction data
7. `nps_surveys.csv` - Net Promoter Score surveys

### Streaming Source

- `usage_events_stream/*.jsonl` - Real-time usage events (120 JSONL files, schema version changes at 2025-07-18)

## Implementation Phases

### Phase 1: Infrastructure & Bronze Layer Setup

**Objective**: Set up data lake structure and implement raw data ingestion

#### Step 1.1: Project Structure Setup

- Create directory structure:
- `src/` - Source code
- `src/batch/` - Batch processing jobs
- `src/streaming/` - Streaming jobs
- `src/common/` - Shared utilities
- `src/config/` - Configuration files
- `datalake/bronze/` - Raw ingested data
- `datalake/silver/` - Cleaned/conformed data
- `datalake/gold/` - Aggregated/business-ready data
- `scripts/` - Deployment/execution scripts
- `tests/` - Unit/integration tests

#### Step 1.2: Configuration Management

- Create configuration files for:
- Data paths (landing, bronze, silver, gold)
- Spark configurations
- Schema definitions
- Environment variables

#### Step 1.3: Bronze Layer - Batch Ingestion

- Implement batch ingestion pipeline for CSV files:
- `ingest_users.py` - Ingest users.csv
- `ingest_customers_orgs.py` - Ingest customers_orgs.csv
- `ingest_resources.py` - Ingest resources.csv
- `ingest_billing_monthly.py` - Ingest billing_monthly.csv
- `ingest_support_tickets.py` - Ingest support_tickets.csv
- `ingest_marketing_touches.py` - Ingest marketing_touches.csv
- `ingest_nps_surveys.py` - Ingest nps_surveys.csv
- Each ingestion job should:
- Read from `datalake/landing/`
- Add metadata (ingestion_timestamp, source_file, etc.)
- Write to `datalake/bronze/` in Delta/Parquet format
- Preserve raw data (no transformations)

#### Step 1.4: Bronze Layer - Streaming Ingestion

- Implement streaming ingestion for usage events:
- `ingest_usage_events_stream.py` - Stream from JSONL files
- Handle schema version changes (schema_version 1 vs 2)
- Add ingestion metadata
- Write to `datalake/bronze/usage_events/` in streaming format
- Implement checkpointing for fault tolerance

### Phase 2: Silver Layer - Data Quality & Conformance

**Objective**: Clean, validate, and conform data from bronze layer

#### Step 2.1: Common Data Quality Framework

- Create data quality utilities:
- Schema validation
- Null handling strategies
- Data type conversions
- Duplicate detection
- Outlier detection

#### Step 2.2: Silver Transformations - Batch Sources

- Implement silver transformations for each batch source:
- `transform_users_silver.py`
- Clean email formats
- Standardize role values
- Handle NULL last_login
- Validate active status
- `transform_customers_orgs_silver.py`
- Standardize organization data
- Validate relationships
- `transform_resources_silver.py`
- Clean resource metadata
- Validate resource-org relationships
- `transform_billing_monthly_silver.py`
- Handle currency conversions (using exchange_rate_to_usd)
- Standardize date formats
- Handle NULL credits
- Calculate total amounts
- `transform_support_tickets_silver.py`
- Standardize ticket statuses
- Clean timestamps
- Validate ticket-org relationships
- `transform_marketing_touches_silver.py`
- Standardize touch types
- Clean timestamps
- `transform_nps_surveys_silver.py`
- Validate NPS scores (0-10)
- Clean timestamps

#### Step 2.3: Silver Transformations - Streaming Source

- Implement silver transformation for usage events:
- `transform_usage_events_silver.py`
- Handle schema version differences (schema_version 1 vs 2)
- Handle NULL values in metrics
- Standardize region names
- Standardize service names
- Validate metric types
- Handle negative costs (as per README notes)
- Type conversions (string to numeric where needed)
- Add derived fields (date partitions, etc.)

#### Step 2.4: Data Lineage & Metadata

- Implement metadata tracking:
- Data lineage from bronze to silver
- Quality metrics (null counts, validation failures)
- Transformation logs

### Phase 3: Gold Layer - Business Logic & Aggregations

**Objective**: Create business-ready datasets for analytics

#### Step 3.1: Gold Datasets - Customer Analytics

- `create_customer_summary_gold.py`
- Customer-level aggregations
- Total spend per customer
- Active resources per customer
- Support ticket metrics
- NPS trends

#### Step 3.2: Gold Datasets - Usage Analytics

- `create_usage_analytics_gold.py`
- Daily/hourly usage aggregations
- Service-level usage metrics
- Region-level usage patterns
- Cost analysis by service/region
- Carbon footprint aggregations (for schema_version 2)

#### Step 3.3: Gold Datasets - Financial Analytics

- `create_financial_analytics_gold.py`
- Monthly revenue trends
- Currency-normalized billing
- Credit impact analysis
- Cost vs billing reconciliation

#### Step 3.4: Gold Datasets - Operational Analytics

- `create_operational_analytics_gold.py`
- Support ticket resolution metrics
- Resource utilization patterns
- User activity metrics
- Marketing campaign effectiveness

#### Step 3.5: Gold Datasets - Unified Views

- `create_unified_customer_view_gold.py`
- Join customer, usage, billing, support data
- 360-degree customer view
- Customer health scores

### Phase 4: Lambda Architecture - Speed Layer

**Objective**: Implement real-time processing for streaming data

#### Step 4.1: Speed Layer - Real-time Aggregations

- `streaming_usage_aggregations.py`
- Real-time usage metrics (sliding windows)
- Real-time cost calculations
- Alert generation for anomalies

#### Step 4.2: Speed Layer - Real-time Enrichment

- `streaming_enrichment.py`
- Enrich streaming events with customer data
- Add resource metadata
- Calculate derived metrics in real-time

### Phase 5: Lambda Architecture - Serving Layer

**Objective**: Create unified views combining batch and speed layer results

#### Step 5.1: Serving Layer - Unified Metrics

- `create_serving_layer.py`
- Merge batch layer (complete historical data) with speed layer (real-time updates)
- Create unified metrics tables
- Implement upsert logic for real-time updates

### Phase 6: Orchestration & Scheduling

**Objective**: Automate pipeline execution

#### Step 6.1: Batch Job Orchestration

- Create orchestration script/workflow:
- Schedule bronze ingestion (daily/hourly)
- Schedule silver transformations (after bronze)
- Schedule gold aggregations (after silver)
- Handle dependencies

#### Step 6.2: Streaming Job Management

- Set up streaming job:
- Continuous execution
- Monitoring and alerting
- Restart capabilities

### Phase 7: Testing & Validation

**Objective**: Ensure data quality and pipeline reliability

#### Step 7.1: Unit Tests

- Test individual transformation functions
- Test data quality rules
- Test schema validations

#### Step 7.2: Integration Tests

- Test end-to-end pipelines
- Test batch + stream integration
- Validate data consistency

#### Step 7.3: Data Quality Tests

- Implement data quality checks:
- Completeness checks
- Accuracy checks
- Consistency checks
- Timeliness checks

### Phase 8: Documentation & Deployment

**Objective**: Document and package the solution

#### Step 8.1: Documentation

- Pipeline documentation
- Data dictionary
- Architecture diagrams
- Runbooks

#### Step 8.2: Deployment Scripts

- Environment setup scripts
- Deployment automation
- Configuration management

## Technical Considerations

### Data Formats

- **Bronze**: Delta Lake or Parquet (preserve raw data)
- **Silver**: Delta Lake (ACID transactions, schema evolution)
- **Gold**: Delta Lake or Parquet (optimized for query performance)

### Schema Evolution

- Handle schema changes in usage_events (schema_version 1 → 2)
- Implement schema registry or versioning strategy

### Performance Optimization

- Partitioning strategies (by date, org_id, etc.)
- Z-ordering for query optimization
- Caching strategies for frequently accessed data

### Error Handling

- Dead letter queues for failed records
- Retry mechanisms
- Data quality alerts

### Monitoring

- Pipeline execution metrics
- Data quality metrics
- Performance metrics
- Cost tracking

## Implementation Order (Incremental Approach)

1. **Week 1**: Phase 1 (Infrastructure + Bronze Layer)
2. **Week 2**: Phase 2 (Silver Layer - start with batch sources)
3. **Week 3**: Phase 2 completion + Phase 3 (Gold Layer - basic aggregations)
4. **Week 4**: Phase 4 (Speed Layer) + Phase 5 (Serving Layer)
5. **Week 5**: Phase 6 (Orchestration) + Phase 7 (Testing)
6. **Week 6**: Phase 8 (Documentation) + Final refinements

## Key Files to Create

### Core Pipeline Files

- `src/common/schemas.py` - Schema definitions
- `src/common/data_quality.py` - Data quality utilities
- `src/common/config.py` - Configuration management
- `src/batch/bronze_ingestion.py` - Batch ingestion orchestrator
- `src/batch/silver_transformation.py` - Silver transformation orchestrator
- `src/batch/gold_aggregation.py` - Gold aggregation orchestrator
- `src/streaming/streaming_pipeline.py` - Streaming pipeline
- `src/lambda/serving_layer.py` - Serving layer implementation

### Configuration Files

- `config/bronze_config.yaml` - Bronze layer configuration
- `config/silver_config.yaml` - Silver layer configuration
- `config/gold_config.yaml` - Gold layer configuration
- `config/streaming_config.yaml` - Streaming configuration

### Scripts

- `scripts/run_batch_pipeline.sh` - Batch pipeline execution
- `scripts/run_streaming_pipeline.sh` - Streaming pipeline execution
- `scripts/setup_environment.sh` - Environment setup

## Success Criteria

1. All data sources successfully ingested to bronze layer
2. All bronze data transformed to silver with quality checks
3. Gold layer contains business-ready aggregated datasets
4. Streaming pipeline processes events in real-time
5. Serving layer provides unified batch + stream views
6. Pipeline is orchestrated and scheduled
7. Data quality metrics are tracked and reported
8. Documentation is complete