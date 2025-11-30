"""Gold mart: cost_anomaly_mart

Aggregated view of cost anomalies for alerting and monitoring.
Per project requirements: anomaly detection using 3 methods (z-score, MAD, percentile).
"""
import logging, sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import get_silver_path, get_gold_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_cost_anomaly_mart(spark: SparkSession) -> None:
    """
    Create cost_anomaly_mart Gold mart.

    Aggregates:
    - Cost anomalies detected by z-score, MAD, or percentile methods
    - Aggregated by organization, date, and service
    - Includes anomaly counts, costs, and detection methods
    """
    logger.info("=" * 80)
    logger.info("Creating Gold mart: cost_anomaly_mart")
    logger.info("=" * 80)

    # Read from Silver
    silver_path = get_silver_path("usage_events")
    logger.info(f"Reading from Silver: {silver_path}")
    df = spark.read.parquet(str(silver_path))
    initial_count = df.count()
    logger.info(f"Initial Silver events: {initial_count:,}")

    # Filter for anomalies (any of the 3 detection methods)
    logger.info("Filtering for anomalies...")
    df = df.filter(
        (F.col("is_anomaly_zscore") == True) |
        (F.col("is_anomaly_mad") == True) |
        (F.col("is_anomaly_percentile") == True)
    )

    anomaly_count = df.count()
    logger.info(f"Anomaly events: {anomaly_count:,} ({100*anomaly_count/initial_count if initial_count > 0 else 0:.2f}%)")

    if anomaly_count == 0:
        logger.warning("⚠ No anomalies found in Silver data")
        logger.warning("Creating empty mart for documentation purposes")

        # Create empty DataFrame with correct schema
        from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, DoubleType, ArrayType

        schema = StructType([
            StructField("org_id", StringType(), True),
            StructField("usage_date", DateType(), True),
            StructField("service", StringType(), True),
            StructField("anomaly_count", LongType(), True),
            StructField("total_anomalous_cost", DoubleType(), True),
            StructField("avg_anomalous_cost", DoubleType(), True),
            StructField("max_cost_spike", DoubleType(), True),
            StructField("zscore_detections", LongType(), True),
            StructField("mad_detections", LongType(), True),
            StructField("percentile_detections", LongType(), True)
        ])

        agg_df = spark.createDataFrame([], schema)

    else:
        # Add usage_date from event_timestamp
        df = df.withColumn("usage_date", F.to_date(F.col("event_timestamp")))

        # Calculate anomaly score (number of methods that flagged it)
        df = df.withColumn(
            "anomaly_score",
            F.when(F.col("is_anomaly_zscore") == True, 1).otherwise(0) +
            F.when(F.col("is_anomaly_mad") == True, 1).otherwise(0) +
            F.when(F.col("is_anomaly_percentile") == True, 1).otherwise(0)
        )

        # Determine primary detection method
        df = df.withColumn(
            "primary_method",
            F.when(F.col("is_anomaly_zscore") == True, "zscore")
            .when(F.col("is_anomaly_mad") == True, "mad")
            .when(F.col("is_anomaly_percentile") == True, "percentile")
            .otherwise("unknown")
        )

        # Aggregate by org_id, usage_date, service
        logger.info("Aggregating anomalies...")

        agg_df = df.groupBy("org_id", "usage_date", "service").agg(
            # Anomaly counts
            F.count("*").alias("anomaly_count"),

            # Cost metrics
            F.sum("cost_usd_increment").alias("total_anomalous_cost"),
            F.avg("cost_usd_increment").alias("avg_anomalous_cost"),
            F.max("cost_usd_increment").alias("max_cost_spike"),
            F.min("cost_usd_increment").alias("min_anomalous_cost"),

            # Anomaly scores
            F.avg("anomaly_score").alias("avg_anomaly_score"),
            F.max("anomaly_score").alias("max_anomaly_score"),

            # Detection method counts
            F.sum(F.when(F.col("is_anomaly_zscore") == True, 1).otherwise(0)).alias("zscore_detections"),
            F.sum(F.when(F.col("is_anomaly_mad") == True, 1).otherwise(0)).alias("mad_detections"),
            F.sum(F.when(F.col("is_anomaly_percentile") == True, 1).otherwise(0)).alias("percentile_detections"),

            # Multi-method detections (flagged by 2+ methods = high confidence)
            F.sum(F.when(F.col("anomaly_score") >= 2, 1).otherwise(0)).alias("high_confidence_anomalies"),
            F.sum(F.when(F.col("anomaly_score") == 3, 1).otherwise(0)).alias("confirmed_anomalies")
        )

        # Add severity classification
        agg_df = agg_df.withColumn(
            "severity",
            F.when(F.col("max_anomaly_score") == 3, "critical")
            .when(F.col("max_anomaly_score") == 2, "high")
            .when(F.col("max_anomaly_score") == 1, "medium")
            .otherwise("low")
        )

        final_count = agg_df.count()
        logger.info(f"Final aggregated anomaly rows: {final_count:,}")

        # Log anomaly breakdown
        severity_counts = agg_df.groupBy("severity").count().collect()
        for row in severity_counts:
            logger.info(f"  Severity {row['severity']}: {row['count']:,} org-date-service combinations")

    # Write to Gold
    gold_path = get_gold_path("cost_anomaly_mart")
    logger.info(f"Writing to Gold: {gold_path}")

    agg_df.write.mode("overwrite").parquet(str(gold_path))

    logger.info("✓ cost_anomaly_mart Gold mart created successfully")
    logger.info("=" * 80)


def main():
    spark = get_spark_session("Gold: cost_anomaly_mart")
    try:
        create_cost_anomaly_mart(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
