"""
Load Gold Layer data into Cassandra Serving Layer

This script reads parquet files from the Gold layer and loads them
into Cassandra tables for serving queries.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from decimal import Decimal

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession, functions as F
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, SimpleStatement, ConsistencyLevel

from src.config.paths import get_gold_path
from src.config.cassandra_config import get_cassandra_config, validate_astradb_config, CASSANDRA_MODE

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_cassandra_session():
    """Create and return a Cassandra session."""
    config = get_cassandra_config()

    if config["mode"] == "astradb":
        logger.info("Connecting to AstraDB...")
        validate_astradb_config()

        cloud_config = {
            'secure_connect_bundle': config["secure_connect_bundle"]
        }

        auth_provider = PlainTextAuthProvider(
            config["client_id"],
            config["client_secret"]
        )

        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect(keyspace=config["keyspace"])

        logger.info(f"✓ Connected to AstraDB, keyspace: {config['keyspace']}")

    else:
        logger.info("Connecting to local Cassandra...")
        cluster = Cluster(
            contact_points=config["contact_points"],
            port=config["port"]
        )
        session = cluster.connect(keyspace=config["keyspace"])
        logger.info(f"✓ Connected to local Cassandra, keyspace: {config['keyspace']}")

    return session


def load_org_daily_usage(spark: SparkSession, session):
    """
    Load org_daily_usage_by_service Gold mart into org_daily_usage Cassandra table.
    """
    logger.info("Loading org_daily_usage...")

    # Read Gold mart
    gold_path = get_gold_path("org_daily_usage_by_service")
    df = spark.read.parquet(str(gold_path))

    logger.info(f"  Read {df.count()} rows from {gold_path}")

    # Prepare insert statement
    insert_stmt = session.prepare("""
        INSERT INTO org_daily_usage (
            org_id, usage_date, service, total_cost_usd, total_requests,
            cpu_hours, storage_gb_hours, carbon_kg, genai_tokens
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    # Convert to pandas for easier iteration
    pdf = df.toPandas()

    # Insert in batches
    batch_size = 100
    total_rows = len(pdf)
    inserted = 0

    for i in range(0, total_rows, batch_size):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        batch_data = pdf.iloc[i:i + batch_size]

        for _, row in batch_data.iterrows():
            batch.add(insert_stmt, (
                row['org_id'],
                row['usage_date'],
                row['service'],
                Decimal(str(row['total_cost_usd'])) if row['total_cost_usd'] else Decimal('0'),
                int(row['total_requests']) if row['total_requests'] else 0,
                Decimal(str(row['cpu_hours'])) if row['cpu_hours'] else Decimal('0'),
                Decimal(str(row['storage_gb_hours'])) if row['storage_gb_hours'] else Decimal('0'),
                Decimal(str(row['carbon_kg_total'])) if row['carbon_kg_total'] else Decimal('0'),
                int(row['genai_tokens_total']) if row['genai_tokens_total'] else 0,
            ))

        session.execute(batch)
        inserted += len(batch_data)

        if inserted % 1000 == 0:
            logger.info(f"  Inserted {inserted}/{total_rows} rows...")

    logger.info(f"✓ Loaded {inserted} rows into org_daily_usage")


def load_org_service_costs(spark: SparkSession, session):
    """
    Compute and load top services by cost for different time windows.
    This is a materialized view for fast Top-N queries.
    """
    logger.info("Loading org_service_costs (Top-N by cost)...")

    # Read Gold mart
    gold_path = get_gold_path("org_daily_usage_by_service")
    df = spark.read.parquet(str(gold_path))

    # Define time windows (last 7, 30, 90 days)
    windows = [7, 30, 90]

    # Get max date to compute windows
    max_date = df.select(F.max("usage_date")).collect()[0][0]

    insert_stmt = session.prepare("""
        INSERT INTO org_service_costs (
            org_id, window_days, service, total_cost_usd
        ) VALUES (?, ?, ?, ?)
    """)

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

        # Insert data
        batch_size = 100
        for i in range(0, len(pdf), batch_size):
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch_data = pdf.iloc[i:i + batch_size]

            for _, row in batch_data.iterrows():
                batch.add(insert_stmt, (
                    row['org_id'],
                    int(row['window_days']),
                    row['service'],
                    Decimal(str(row['total_cost_usd'])) if row['total_cost_usd'] else Decimal('0')
                ))

            session.execute(batch)

        total_inserted += len(pdf)
        logger.info(f"    Loaded {len(pdf)} org-service combinations for {window_days}-day window")

    logger.info(f"✓ Loaded {total_inserted} rows into org_service_costs")


def load_tickets_critical_daily(spark: SparkSession, session):
    """
    Load tickets_by_org_date Gold mart aggregated by date and severity.
    """
    logger.info("Loading tickets_critical_daily...")

    # Read Gold mart
    gold_path = get_gold_path("tickets_by_org_date")
    df = spark.read.parquet(str(gold_path))

    logger.info(f"  Read {df.count()} rows from {gold_path}")

    # Aggregate across all orgs by date and severity
    agg_df = df.groupBy("ticket_date", "severity").agg(
        F.sum("total_tickets").alias("total_tickets"),
        F.sum(F.when(F.col("status") == "open", F.col("total_tickets")).otherwise(0)).alias("open_tickets"),
        F.sum(F.when(F.col("status") == "resolved", F.col("total_tickets")).otherwise(0)).alias("resolved_tickets"),
        F.avg("sla_breach_rate").alias("sla_breach_rate"),
        F.avg("avg_resolution_hours").alias("avg_resolution_hours")
    )

    agg_df = agg_df.withColumnRenamed("ticket_date", "date")

    pdf = agg_df.toPandas()

    insert_stmt = session.prepare("""
        INSERT INTO tickets_critical_daily (
            date, severity, total_tickets, open_tickets, resolved_tickets,
            sla_breach_rate, avg_resolution_hours
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    # Insert data
    batch_size = 100
    total_rows = len(pdf)
    inserted = 0

    for i in range(0, total_rows, batch_size):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        batch_data = pdf.iloc[i:i + batch_size]

        for _, row in batch_data.iterrows():
            batch.add(insert_stmt, (
                row['date'],
                row['severity'],
                int(row['total_tickets']) if row['total_tickets'] else 0,
                int(row['open_tickets']) if row['open_tickets'] else 0,
                int(row['resolved_tickets']) if row['resolved_tickets'] else 0,
                Decimal(str(row['sla_breach_rate'])) if row['sla_breach_rate'] else Decimal('0'),
                Decimal(str(row['avg_resolution_hours'])) if row['avg_resolution_hours'] else Decimal('0')
            ))

        session.execute(batch)
        inserted += len(batch_data)

    logger.info(f"✓ Loaded {inserted} rows into tickets_critical_daily")


def load_revenue_monthly(spark: SparkSession, session):
    """
    Load revenue_by_org_month Gold mart into revenue_monthly Cassandra table.
    """
    logger.info("Loading revenue_monthly...")

    # Read Gold mart
    gold_path = get_gold_path("revenue_by_org_month")
    df = spark.read.parquet(str(gold_path))

    logger.info(f"  Read {df.count()} rows from {gold_path}")

    pdf = df.toPandas()

    insert_stmt = session.prepare("""
        INSERT INTO revenue_monthly (
            org_id, year_month, org_name, total_billed_usd, total_credits,
            total_taxes, net_revenue, invoice_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    # Insert data
    batch_size = 100
    total_rows = len(pdf)
    inserted = 0

    for i in range(0, total_rows, batch_size):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        batch_data = pdf.iloc[i:i + batch_size]

        for _, row in batch_data.iterrows():
            batch.add(insert_stmt, (
                row['org_id'],
                row['year_month'],
                row.get('org_name', ''),
                Decimal(str(row['total_billed_usd'])) if row['total_billed_usd'] else Decimal('0'),
                Decimal(str(row['total_credits_usd'])) if row['total_credits_usd'] else Decimal('0'),
                Decimal(str(row['total_taxes_usd'])) if row['total_taxes_usd'] else Decimal('0'),
                Decimal(str(row['net_revenue'])) if row['net_revenue'] else Decimal('0'),
                int(row['invoice_count']) if row['invoice_count'] else 0
            ))

        session.execute(batch)
        inserted += len(batch_data)

        if inserted % 1000 == 0:
            logger.info(f"  Inserted {inserted}/{total_rows} rows...")

    logger.info(f"✓ Loaded {inserted} rows into revenue_monthly")


def load_genai_tokens_daily(spark: SparkSession, session):
    """
    Load genai_tokens_by_org_date Gold mart into genai_tokens_daily Cassandra table.
    """
    logger.info("Loading genai_tokens_daily...")

    # Read Gold mart
    gold_path = get_gold_path("genai_tokens_by_org_date")
    df = spark.read.parquet(str(gold_path))

    logger.info(f"  Read {df.count()} rows from {gold_path}")

    pdf = df.toPandas()

    insert_stmt = session.prepare("""
        INSERT INTO genai_tokens_daily (
            org_id, usage_date, total_genai_tokens, total_cost_usd, cost_per_million_tokens
        ) VALUES (?, ?, ?, ?, ?)
    """)

    # Insert data
    batch_size = 100
    total_rows = len(pdf)
    inserted = 0

    for i in range(0, total_rows, batch_size):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        batch_data = pdf.iloc[i:i + batch_size]

        for _, row in batch_data.iterrows():
            batch.add(insert_stmt, (
                row['org_id'],
                row['usage_date'],
                int(row['total_genai_tokens']) if row['total_genai_tokens'] else 0,
                Decimal(str(row['total_cost_usd'])) if row['total_cost_usd'] else Decimal('0'),
                Decimal(str(row['cost_per_million_tokens'])) if row['cost_per_million_tokens'] else Decimal('0')
            ))

        session.execute(batch)
        inserted += len(batch_data)

    logger.info(f"✓ Loaded {inserted} rows into genai_tokens_daily")


def main():
    """
    Main loading function.
    Loads all Gold marts into Cassandra serving tables.
    """
    start_time = datetime.now()

    logger.info("=" * 100)
    logger.info("GOLD LAYER → CASSANDRA SERVING LAYER")
    logger.info(f"Start time: {start_time}")
    logger.info("=" * 100)

    # Initialize Spark
    logger.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("Gold to Cassandra Loader") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Connect to Cassandra
    logger.info("Connecting to Cassandra...")
    session = get_cassandra_session()

    results = {}

    try:
        # Load each table
        loaders = [
            ("org_daily_usage", load_org_daily_usage),
            ("org_service_costs", load_org_service_costs),
            ("tickets_critical_daily", load_tickets_critical_daily),
            ("revenue_monthly", load_revenue_monthly),
            ("genai_tokens_daily", load_genai_tokens_daily)
        ]

        for i, (name, loader_func) in enumerate(loaders, 1):
            logger.info("")
            logger.info("=" * 100)
            logger.info(f"[{i}/{len(loaders)}] Loading table: {name}")
            logger.info("=" * 100)

            try:
                step_start = datetime.now()
                loader_func(spark, session)
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
        session.shutdown()

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

    logger.info(f"Tables: {successful} successful, {failed} failed")
    logger.info("")

    for name, result in results.items():
        status_symbol = "✓" if result["status"] == "SUCCESS" else "✗"
        duration = result["duration_seconds"]
        logger.info(f"  {status_symbol} {name:30s} - {result['status']:10s} ({duration:6.2f}s)")

        if result["status"] == "FAILED":
            logger.info(f"      Error: {result['error']}")

    logger.info("=" * 100)

    if failed > 0:
        logger.warning(f"⚠ {failed} table(s) failed to load. Please review logs above.")
        return 1
    else:
        logger.info("✓ All data loaded to Cassandra successfully!")
        logger.info("")
        logger.info("Next step: Run demo queries with: python -m src.serving.demo_queries")
        return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
