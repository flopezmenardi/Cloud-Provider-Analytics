"""
Load Gold Layer data into AstraDB Serving Layer

This script reads parquet files from the Gold layer and loads them
into AstraDB collections for serving queries.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession, functions as F
from src.config.paths import get_gold_path
from src.config.astradb_config import get_astradb_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def clear_collection(db, collection_name: str):
    """Clear all documents from a collection for idempotent reloads."""
    try:
        collection = db.get_collection(collection_name)
        collection.delete_many({})
        logger.info(f"  Cleared existing data from {collection_name}")
    except Exception as e:
        logger.warning(f"  Could not clear {collection_name}: {e}")


def load_org_daily_usage(spark: SparkSession, db):
    """
    Load org_daily_usage_by_service Gold mart into org_daily_usage collection.
    """
    logger.info("Loading org_daily_usage...")
    clear_collection(db, "org_daily_usage")

    # Read Gold mart
    gold_path = get_gold_path("org_daily_usage_by_service")
    df = spark.read.parquet(str(gold_path))

    row_count = df.count()
    logger.info(f"  Read {row_count:,} rows from {gold_path}")

    # Convert to pandas
    pdf = df.toPandas()

    # Convert to JSON documents
    documents = []
    for _, row in pdf.iterrows():
        # Create composite ID
        doc_id = f"{row['org_id']}_{row['usage_date']}_{row['service']}"

        doc = {
            "_id": doc_id,
            "org_id": row['org_id'],
            "usage_date": str(row['usage_date']),
            "service": row['service'],
            "total_events": int(row['total_events']) if pd.notna(row['total_events']) else 0,
            "total_cost_usd": float(row['total_cost_usd']) if pd.notna(row['total_cost_usd']) else 0.0,
            "total_requests": int(row['total_requests']) if pd.notna(row['total_requests']) else 0,
            "cpu_hours": float(row['cpu_hours']) if pd.notna(row['cpu_hours']) else 0.0,
            "storage_gb_hours": float(row['storage_gb_hours']) if pd.notna(row['storage_gb_hours']) else 0.0,
            "carbon_kg_total": float(row['carbon_kg_total']) if pd.notna(row['carbon_kg_total']) else 0.0,
            "genai_tokens_total": int(row['genai_tokens_total']) if pd.notna(row['genai_tokens_total']) else 0,
        }
        documents.append(doc)

    # Insert in batches
    collection = db.get_collection("org_daily_usage")
    batch_size = 100
    inserted = 0

    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        collection.insert_many(batch)
        inserted += len(batch)

        if inserted % 1000 == 0:
            logger.info(f"  Inserted {inserted:,}/{len(documents):,} documents...")

    logger.info(f"✓ Loaded {inserted:,} documents into org_daily_usage")


def load_org_service_costs(spark: SparkSession, db):
    """
    Compute and load top services by cost for different time windows.
    """
    logger.info("Loading org_service_costs (Top-N by cost)...")
    clear_collection(db, "org_service_costs")

    # Read Gold mart
    gold_path = get_gold_path("org_daily_usage_by_service")
    df = spark.read.parquet(str(gold_path))

    # Define time windows (last 7, 30, 90 days)
    windows = [7, 30, 90]

    # Get max date to compute windows
    max_date = df.select(F.max("usage_date")).collect()[0][0]

    collection = db.get_collection("org_service_costs")
    total_inserted = 0

    for window_days in windows:
        logger.info(f"  Computing costs for {window_days}-day window...")

        # Filter to window
        filtered_df = df.filter(
            F.datediff(F.lit(max_date), F.col("usage_date")) <= window_days
        )

        # Aggregate by org and service
        agg_df = filtered_df.groupBy("org_id", "service").agg(
            F.sum("total_cost_usd").alias("total_cost_usd")
        )

        # Add window_days column
        agg_df = agg_df.withColumn("window_days", F.lit(window_days))

        pdf = agg_df.toPandas()

        # Convert to documents
        documents = []
        for _, row in pdf.iterrows():
            doc_id = f"{row['org_id']}_{row['window_days']}_{row['service']}"
            doc = {
                "_id": doc_id,
                "org_id": row['org_id'],
                "window_days": int(row['window_days']),
                "service": row['service'],
                "total_cost_usd": float(row['total_cost_usd']) if pd.notna(row['total_cost_usd']) else 0.0
            }
            documents.append(doc)

        # Insert batch
        if documents:
            collection.insert_many(documents)
            total_inserted += len(documents)
            logger.info(f"    Loaded {len(documents):,} org-service combinations for {window_days}-day window")

    logger.info(f"✓ Loaded {total_inserted:,} documents into org_service_costs")


def load_tickets_critical_daily(spark: SparkSession, db):
    """
    Load tickets_by_org_date Gold mart aggregated by date and severity.
    """
    logger.info("Loading tickets_critical_daily...")
    clear_collection(db, "tickets_critical_daily")

    # Read Gold mart
    gold_path = get_gold_path("tickets_by_org_date")
    df = spark.read.parquet(str(gold_path))

    row_count = df.count()
    logger.info(f"  Read {row_count:,} rows from {gold_path}")

    # Aggregate across all orgs by date and severity
    agg_df = df.groupBy("ticket_date", "severity").agg(
        F.sum("total_tickets").alias("total_tickets"),
        F.avg("sla_breach_rate").alias("sla_breach_rate"),
        F.avg("avg_resolution_hours").alias("avg_resolution_hours")
    )

    agg_df = agg_df.withColumnRenamed("ticket_date", "date")

    pdf = agg_df.toPandas()

    # Convert to documents
    documents = []
    for _, row in pdf.iterrows():
        doc_id = f"{row['date']}_{row['severity']}"
        doc = {
            "_id": doc_id,
            "date": str(row['date']),
            "severity": row['severity'],
            "total_tickets": int(row['total_tickets']) if pd.notna(row['total_tickets']) else 0,
            "sla_breach_rate": float(row['sla_breach_rate']) if pd.notna(row['sla_breach_rate']) else 0.0,
            "avg_resolution_hours": float(row['avg_resolution_hours']) if pd.notna(row['avg_resolution_hours']) else 0.0
        }
        documents.append(doc)

    # Insert
    if documents:
        collection = db.get_collection("tickets_critical_daily")
        collection.insert_many(documents)
        logger.info(f"✓ Loaded {len(documents):,} documents into tickets_critical_daily")
    else:
        logger.warning("No ticket data to load")


def load_revenue_monthly(spark: SparkSession, db):
    """
    Load revenue_by_org_month Gold mart into revenue_monthly collection.
    """
    logger.info("Loading revenue_monthly...")
    clear_collection(db, "revenue_monthly")

    # Read Gold mart
    gold_path = get_gold_path("revenue_by_org_month")
    df = spark.read.parquet(str(gold_path))

    row_count = df.count()
    logger.info(f"  Read {row_count:,} rows from {gold_path}")

    pdf = df.toPandas()

    # Convert to documents
    documents = []
    for _, row in pdf.iterrows():
        doc_id = f"{row['org_id']}_{row['year_month']}"
        doc = {
            "_id": doc_id,
            "org_id": row['org_id'],
            "year_month": row['year_month'],
            "org_name": row.get('org_name', ''),
            "total_billed_usd": float(row['total_billed_usd']) if pd.notna(row['total_billed_usd']) else 0.0,
            "total_credits_usd": float(row['total_credits_usd']) if pd.notna(row['total_credits_usd']) else 0.0,
            "total_taxes_usd": float(row['total_taxes_usd']) if pd.notna(row['total_taxes_usd']) else 0.0,
            "net_revenue": float(row['net_revenue']) if pd.notna(row['net_revenue']) else 0.0,
            "invoice_count": int(row['invoice_count']) if pd.notna(row['invoice_count']) else 0
        }
        documents.append(doc)

    # Insert
    collection = db.get_collection("revenue_monthly")
    batch_size = 100
    inserted = 0

    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        collection.insert_many(batch)
        inserted += len(batch)

    logger.info(f"✓ Loaded {inserted:,} documents into revenue_monthly")


def load_genai_tokens_daily(spark: SparkSession, db):
    """
    Load genai_tokens_by_org_date Gold mart into genai_tokens_daily collection.
    """
    logger.info("Loading genai_tokens_daily...")
    clear_collection(db, "genai_tokens_daily")

    # Read Gold mart
    gold_path = get_gold_path("genai_tokens_by_org_date")
    df = spark.read.parquet(str(gold_path))

    row_count = df.count()
    logger.info(f"  Read {row_count:,} rows from {gold_path}")

    pdf = df.toPandas()

    # Convert to documents
    documents = []
    for _, row in pdf.iterrows():
        doc_id = f"{row['org_id']}_{row['usage_date']}"
        doc = {
            "_id": doc_id,
            "org_id": row['org_id'],
            "usage_date": str(row['usage_date']),
            "total_genai_tokens": int(row['total_genai_tokens']) if pd.notna(row['total_genai_tokens']) else 0,
            "total_cost_usd": float(row['total_cost_usd']) if pd.notna(row['total_cost_usd']) else 0.0,
            "cost_per_million_tokens": float(row['cost_per_million_tokens']) if pd.notna(row['cost_per_million_tokens']) else 0.0
        }
        documents.append(doc)

    # Insert
    if documents:
        collection = db.get_collection("genai_tokens_daily")
        collection.insert_many(documents)
        logger.info(f"✓ Loaded {len(documents):,} documents into genai_tokens_daily")
    else:
        logger.warning("No GenAI token data to load (no schema v2 events)")


def load_cost_anomalies(spark: SparkSession, db):
    """
    Load cost_anomaly_mart Gold mart into cost_anomalies collection.
    Demonstrates 3-method anomaly detection results.
    """
    logger.info("Loading cost_anomalies...")
    clear_collection(db, "cost_anomalies")

    # Read Gold mart
    gold_path = get_gold_path("cost_anomaly_mart")
    df = spark.read.parquet(str(gold_path))

    row_count = df.count()
    logger.info(f"  Read {row_count:,} rows from {gold_path}")

    if row_count == 0:
        logger.warning("No anomaly data to load")
        return

    pdf = df.toPandas()

    # Convert to documents
    documents = []
    for _, row in pdf.iterrows():
        doc_id = f"{row['org_id']}_{row['usage_date']}_{row['service']}"
        doc = {
            "_id": doc_id,
            "org_id": row['org_id'],
            "usage_date": str(row['usage_date']),
            "service": row['service'],
            "anomaly_count": int(row['anomaly_count']) if pd.notna(row['anomaly_count']) else 0,
            "total_anomalous_cost": float(row['total_anomalous_cost']) if pd.notna(row['total_anomalous_cost']) else 0.0,
            "avg_anomalous_cost": float(row['avg_anomalous_cost']) if pd.notna(row['avg_anomalous_cost']) else 0.0,
            "max_cost_spike": float(row['max_cost_spike']) if pd.notna(row['max_cost_spike']) else 0.0,
            "zscore_detections": int(row['zscore_detections']) if pd.notna(row['zscore_detections']) else 0,
            "mad_detections": int(row['mad_detections']) if pd.notna(row['mad_detections']) else 0,
            "percentile_detections": int(row['percentile_detections']) if pd.notna(row['percentile_detections']) else 0,
            "severity": row.get('severity', 'unknown'),
            "high_confidence_anomalies": int(row.get('high_confidence_anomalies', 0)) if pd.notna(row.get('high_confidence_anomalies')) else 0,
            "confirmed_anomalies": int(row.get('confirmed_anomalies', 0)) if pd.notna(row.get('confirmed_anomalies')) else 0
        }
        documents.append(doc)

    # Insert
    if documents:
        collection = db.get_collection("cost_anomalies")
        collection.insert_many(documents)
        logger.info(f"✓ Loaded {len(documents):,} documents into cost_anomalies")
    else:
        logger.warning("No anomaly documents to load")


def main():
    """
    Main loading function.
    Loads all Gold marts into AstraDB collections.
    """
    start_time = datetime.now()

    logger.info("=" * 100)
    logger.info("GOLD LAYER → ASTRADB SERVING LAYER")
    logger.info(f"Start time: {start_time}")
    logger.info("=" * 100)

    # Initialize Spark
    logger.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("Gold to AstraDB Loader") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Connect to AstraDB
    logger.info("Connecting to AstraDB...")
    db = get_astradb_client()
    logger.info("✓ Connected to AstraDB")

    results = {}

    try:
        # Load each collection
        loaders = [
            ("org_daily_usage", load_org_daily_usage),
            ("org_service_costs", load_org_service_costs),
            ("tickets_critical_daily", load_tickets_critical_daily),
            ("revenue_monthly", load_revenue_monthly),
            ("genai_tokens_daily", load_genai_tokens_daily),
            ("cost_anomalies", load_cost_anomalies)
        ]

        for i, (name, loader_func) in enumerate(loaders, 1):
            logger.info("")
            logger.info("=" * 100)
            logger.info(f"[{i}/{len(loaders)}] Loading collection: {name}")
            logger.info("=" * 100)

            try:
                step_start = datetime.now()
                loader_func(spark, db)
                step_duration = (datetime.now() - step_start).total_seconds()

                results[name] = {
                    "status": "SUCCESS",
                    "duration_seconds": step_duration
                }
                logger.info(f"✓ {name} completed in {step_duration:.2f}s")

            except Exception as e:
                step_duration = (datetime.now() - step_start).total_seconds()
                results[name] = {
                    "status": "FAILED",
                    "error": str(e),
                    "duration_seconds": step_duration
                }
                logger.error(f"✗ {name} FAILED after {step_duration:.2f}s: {e}", exc_info=True)
                continue

    finally:
        spark.stop()

    # Print summary
    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds()

    logger.info("")
    logger.info("=" * 100)
    logger.info("LOADING SUMMARY")
    logger.info("=" * 100)
    logger.info(f"Start time:  {start_time}")
    logger.info(f"End time:    {end_time}")
    logger.info(f"Total duration: {total_duration:.2f}s ({total_duration/60:.2f} minutes)")
    logger.info("")

    successful = sum(1 for r in results.values() if r["status"] == "SUCCESS")
    failed = sum(1 for r in results.values() if r["status"] == "FAILED")

    logger.info(f"Collections: {successful} successful, {failed} failed")
    logger.info("")

    for name, result in results.items():
        status_symbol = "✓" if result["status"] == "SUCCESS" else "✗"
        duration = result["duration_seconds"]
        logger.info(f"  {status_symbol} {name:30s} - {result['status']:10s} ({duration:6.2f}s)")

        if result["status"] == "FAILED":
            logger.info(f"      Error: {result['error']}")

    logger.info("=" * 100)

    if failed > 0:
        logger.warning(f"⚠ {failed} collection(s) failed to load. Please review logs above.")
        return 1
    else:
        logger.info("✓ All data loaded to AstraDB successfully!")
        logger.info("")
        logger.info("Next step: Run demo queries with: ./scripts/run_demo_queries.sh")
        return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
