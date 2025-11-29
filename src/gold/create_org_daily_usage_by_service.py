"""Gold mart: org_daily_usage_by_service

Daily aggregations of usage metrics per organization and service.
"""
import logging, sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import get_silver_path, get_gold_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_org_daily_usage_by_service(spark: SparkSession) -> None:
    """
    Create org_daily_usage_by_service Gold mart.

    Aggregates:
    - Daily usage metrics by organization and service
    - Total cost, requests, CPU hours, storage, carbon, GenAI tokens
    """
    logger.info("=" * 80)
    logger.info("Creating Gold mart: org_daily_usage_by_service")
    logger.info("=" * 80)

    # Read from Silver
    silver_path = get_silver_path("usage_events")
    logger.info(f"Reading from Silver: {silver_path}")
    df = spark.read.parquet(str(silver_path))
    initial_count = df.count()
    logger.info(f"Initial Silver events: {initial_count:,}")

    # Add usage_date from event_timestamp
    df = df.withColumn("usage_date", F.to_date(F.col("event_timestamp")))

    # Aggregate by org_id, usage_date, service
    logger.info("Aggregating by org_id, usage_date, service...")

    agg_df = df.groupBy("org_id", "usage_date", "service").agg(
        # Total events
        F.count("*").alias("total_events"),

        # Total cost
        F.sum("cost_usd_increment").alias("total_cost_usd"),

        # Requests (count where metric='requests')
        F.sum(F.when(F.col("metric") == "requests", 1).otherwise(0)).alias("total_requests"),

        # CPU hours (sum value where metric='cpu_utilization')
        F.sum(
            F.when(F.col("metric") == "cpu_utilization", F.col("value")).otherwise(0)
        ).alias("cpu_hours"),

        # Storage GB hours (sum value where metric='storage')
        F.sum(
            F.when(F.col("metric") == "storage", F.col("value")).otherwise(0)
        ).alias("storage_gb_hours"),

        # Carbon kg (null-safe sum for schema v2)
        F.sum(F.coalesce(F.col("carbon_kg"), F.lit(0.0))).alias("carbon_kg_total"),

        # GenAI tokens (null-safe sum for schema v2)
        F.sum(F.coalesce(F.col("genai_tokens"), F.lit(0))).alias("genai_tokens_total")
    )

    # Add partitioning columns
    agg_df = agg_df.withColumn("year", F.year(F.col("usage_date")))
    agg_df = agg_df.withColumn("month", F.month(F.col("usage_date")))

    final_count = agg_df.count()
    logger.info(f"Final aggregated rows: {final_count:,}")
    logger.info(f"Aggregation ratio: {initial_count}/{final_count} = {initial_count/final_count if final_count > 0 else 0:.1f}x compression")

    # Write to Gold (partitioned by year, month)
    gold_path = get_gold_path("org_daily_usage_by_service")
    logger.info(f"Writing to Gold: {gold_path}")

    agg_df.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(str(gold_path))

    logger.info("✓ org_daily_usage_by_service Gold mart created successfully")
    logger.info("=" * 80)


def main():
    spark = get_spark_session("Gold: org_daily_usage_by_service")
    try:
        create_org_daily_usage_by_service(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
