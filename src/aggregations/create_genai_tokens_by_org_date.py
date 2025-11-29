"""Gold mart: genai_tokens_by_org_date

GenAI token usage and costs (schema v2 only).
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


def create_genai_tokens_by_org_date(spark: SparkSession) -> None:
    """
    Create genai_tokens_by_org_date Gold mart.

    Aggregates:
    - GenAI token usage by organization and date
    - Costs and efficiency metrics
    - Only includes schema v2 events with genai_tokens
    """
    logger.info("=" * 80)
    logger.info("Creating Gold mart: genai_tokens_by_org_date")
    logger.info("=" * 80)

    # Read from Silver
    silver_path = get_silver_path("usage_events")
    logger.info(f"Reading from Silver: {silver_path}")
    df = spark.read.parquet(str(silver_path))
    initial_count = df.count()
    logger.info(f"Initial Silver events: {initial_count:,}")

    # Filter for schema v2 events with genai_tokens
    logger.info("Filtering for schema_version=2 AND genai_tokens > 0...")
    df = df.filter(
        (F.col("schema_version") == 2) &
        (F.col("genai_tokens").isNotNull()) &
        (F.col("genai_tokens") > 0)
    )

    filtered_count = df.count()
    logger.info(f"GenAI events after filtering: {filtered_count:,}")

    if filtered_count == 0:
        logger.warning("⚠ No GenAI token events found (schema v2 may not be in dataset)")
        logger.warning("Creating empty mart for documentation purposes")

        # Create empty DataFrame with correct schema
        from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, DoubleType

        schema = StructType([
            StructField("org_id", StringType(), True),
            StructField("usage_date", DateType(), True),
            StructField("service", StringType(), True),
            StructField("total_genai_tokens", LongType(), True),
            StructField("total_cost_usd", DoubleType(), True),
            StructField("event_count", LongType(), True),
            StructField("avg_tokens_per_event", DoubleType(), True),
            StructField("cost_per_million_tokens", DoubleType(), True)
        ])

        agg_df = spark.createDataFrame([], schema)

    else:
        # Add usage_date from event_timestamp
        df = df.withColumn("usage_date", F.to_date(F.col("event_timestamp")))

        # Aggregate by org_id, usage_date, service
        logger.info("Aggregating GenAI metrics...")

        agg_df = df.groupBy("org_id", "usage_date", "service").agg(
            # Token totals
            F.sum("genai_tokens").alias("total_genai_tokens"),

            # Cost
            F.sum("cost_usd_increment").alias("total_cost_usd"),

            # Event count
            F.count("*").alias("event_count")
        )

        # Calculate derived metrics
        agg_df = agg_df.withColumn(
            "avg_tokens_per_event",
            F.col("total_genai_tokens") / F.col("event_count")
        )

        agg_df = agg_df.withColumn(
            "cost_per_million_tokens",
            F.when(
                F.col("total_genai_tokens") > 0,
                (F.col("total_cost_usd") / F.col("total_genai_tokens")) * 1000000
            ).otherwise(0.0)
        )

        final_count = agg_df.count()
        logger.info(f"Final aggregated rows: {final_count:,}")

    # Write to Gold
    gold_path = get_gold_path("genai_tokens_by_org_date")
    logger.info(f"Writing to Gold: {gold_path}")

    agg_df.write.mode("overwrite").parquet(str(gold_path))

    logger.info("✓ genai_tokens_by_org_date Gold mart created successfully")
    logger.info("=" * 80)


def main():
    spark = get_spark_session("Gold: genai_tokens_by_org_date")
    try:
        create_genai_tokens_by_org_date(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
