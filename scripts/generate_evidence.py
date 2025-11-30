#!/usr/bin/env python3
"""
Generate Pipeline Evidence for Presentation

This script runs the full pipeline and generates evidence files
for the assignment presentation and video demo.

Evidence includes:
- Pipeline execution summary
- Data lineage records
- Stage metrics (input/output counts)
- Data quality statistics
- Anomaly detection results
- Sample query outputs
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_pipeline_evidence():
    """Generate comprehensive pipeline evidence."""
    from src.config.spark_config import get_spark_session
    from src.config.paths import (
        BRONZE_ROOT, get_silver_path, get_gold_path, get_quarantine_path
    )
    from src.common.lineage import PipelineEvidence, TransformationMetrics
    
    evidence = PipelineEvidence()
    spark = get_spark_session("EvidenceGenerator")
    
    try:
        print("\n" + "=" * 100)
        print("CLOUD PROVIDER ANALYTICS - EVIDENCE GENERATION")
        print("=" * 100)
        print(f"Run ID: {evidence.run_id}")
        print(f"Started: {evidence.start_time}")
        print()
        
        # Collect Bronze Layer Statistics
        print("Collecting Bronze Layer Statistics...")
        bronze_stats = {}
        bronze_tables = [
            "users", "customers_orgs", "resources", "billing_monthly",
            "support_tickets", "marketing_touches", "nps_surveys", "usage_events"
        ]
        
        for table in bronze_tables:
            try:
                path = BRONZE_ROOT / table
                if path.exists():
                    df = spark.read.parquet(str(path))
                    count = df.count()
                    bronze_stats[table] = count
                    logger.info(f"  Bronze {table}: {count:,} records")
            except Exception as e:
                logger.warning(f"  Bronze {table}: Not available - {e}")
        
        # Collect Silver Layer Statistics
        print("\nCollecting Silver Layer Statistics...")
        silver_stats = {}
        for table in bronze_tables:
            try:
                path = get_silver_path(table)
                if Path(path).exists():
                    df = spark.read.parquet(str(path))
                    count = df.count()
                    silver_stats[table] = count
                    logger.info(f"  Silver {table}: {count:,} records")
                    
                    # For usage_events, collect additional stats
                    if table == "usage_events":
                        if "schema_version" in df.columns:
                            version_counts = df.groupBy("schema_version").count().collect()
                            for row in version_counts:
                                logger.info(f"    Schema v{row['schema_version']}: {row['count']:,}")
                        
                        if "is_anomaly" in df.columns:
                            anomaly_count = df.filter(F.col("is_anomaly") == True).count()
                            logger.info(f"    Anomalies detected: {anomaly_count:,}")
            except Exception as e:
                logger.warning(f"  Silver {table}: Not available - {e}")
        
        # Collect Gold Layer Statistics
        print("\nCollecting Gold Layer Statistics...")
        gold_tables = [
            "org_daily_usage_by_service", "revenue_by_org_month",
            "tickets_by_org_date", "genai_tokens_by_org_date", "cost_anomaly_mart"
        ]
        gold_stats = {}
        
        for table in gold_tables:
            try:
                path = get_gold_path(table)
                if Path(path).exists():
                    df = spark.read.parquet(str(path))
                    count = df.count()
                    gold_stats[table] = count
                    logger.info(f"  Gold {table}: {count:,} records")
            except Exception as e:
                logger.warning(f"  Gold {table}: Not available - {e}")
        
        # Collect Quarantine Statistics
        print("\nCollecting Quarantine Statistics...")
        quarantine_stats = {}
        for table in bronze_tables:
            try:
                path = get_quarantine_path(table)
                if Path(path).exists():
                    df = spark.read.parquet(str(path))
                    count = df.count()
                    if count > 0:
                        quarantine_stats[table] = count
                        logger.info(f"  Quarantine {table}: {count:,} records")
            except Exception as e:
                pass
        
        # Calculate Data Quality Metrics
        print("\nCalculating Data Quality Metrics...")
        for table in bronze_tables:
            bronze_count = bronze_stats.get(table, 0)
            silver_count = silver_stats.get(table, 0)
            quarantine_count = quarantine_stats.get(table, 0)
            
            if bronze_count > 0:
                quality_rate = (silver_count / bronze_count) * 100
                logger.info(f"  {table}: {quality_rate:.2f}% quality rate")
                
                evidence.add_data_quality_evidence(
                    layer="silver",
                    table=table,
                    valid_count=silver_count,
                    quarantine_count=quarantine_count,
                    anomaly_count=0,
                    validation_rules=["not_null", "range_check", "type_cast"]
                )
        
        # Generate Sample Outputs
        print("\nGenerating Sample Query Outputs...")
        
        # Sample 1: Daily Usage by Org/Service
        try:
            usage_path = get_gold_path("org_daily_usage_by_service")
            if Path(usage_path).exists():
                df = spark.read.parquet(str(usage_path))
                sample = df.orderBy(F.desc("total_cost_usd")).limit(10)
                print("\n  Top 10 Daily Usage Records by Cost:")
                sample.show(truncate=False)
        except:
            pass
        
        # Sample 2: Revenue by Org/Month
        try:
            revenue_path = get_gold_path("revenue_by_org_month")
            if Path(revenue_path).exists():
                df = spark.read.parquet(str(revenue_path))
                sample = df.orderBy(F.desc("total_billed_usd")).limit(5)
                print("\n  Top 5 Monthly Revenue Records:")
                sample.show(truncate=False)
        except:
            pass
        
        # Sample 3: Anomaly Summary
        try:
            anomaly_path = get_gold_path("cost_anomaly_mart")
            if Path(anomaly_path).exists():
                df = spark.read.parquet(str(anomaly_path))
                if df.count() > 0:
                    print("\n  Cost Anomaly Summary:")
                    df.groupBy("severity").agg(
                        F.count("*").alias("count"),
                        F.sum("total_anomalous_cost").alias("total_cost")
                    ).show()
        except:
            pass
        
        # Save Evidence
        print("\nSaving Evidence Files...")
        evidence_path = evidence.save_evidence()
        
        # Print Final Summary
        evidence.print_summary()
        
        print(f"\nEvidence files saved to: {evidence_path}")
        print("\nFiles generated:")
        print("  - pipeline_summary.json")
        print("  - lineage.json")
        print("  - stage_metrics.json")
        print("  - data_quality.json")
        
        return 0
        
    except Exception as e:
        logger.error(f"Evidence generation failed: {e}", exc_info=True)
        return 1
    finally:
        spark.stop()


def main():
    exit_code = generate_pipeline_evidence()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
