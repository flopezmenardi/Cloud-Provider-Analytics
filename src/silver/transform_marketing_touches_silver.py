"""Silver layer transformation for marketing_touches"""
import logging, sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import BRONZE_MARKETING_TOUCHES, get_silver_path, get_quarantine_path
from src.common.data_quality import quarantine_records

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_marketing_touches_to_silver(spark: SparkSession) -> None:
    logger.info("Starting marketing_touches Silver transformation")

    df = spark.read.parquet(str(BRONZE_MARKETING_TOUCHES))
    logger.info(f"Initial count: {df.count():,}")

    # Standardize (CSV has: campaign, channel, timestamp, clicked, converted)
    df = df.withColumn("channel", F.lower(F.trim(F.col("channel"))))
    df = df.withColumn("campaign", F.lower(F.trim(F.col("campaign"))))

    # Parse timestamp (column is "timestamp" not "touch_timestamp")
    df = df.withColumn("touch_timestamp", F.to_timestamp(F.col("timestamp")))

    # Convert conversion flag to boolean
    df = df.withColumn("converted", F.col("converted").cast("boolean"))

    # Validations
    df = df.withColumn(
        "is_valid",
        F.col("touch_id").isNotNull() &
        F.col("org_id").isNotNull()
    )

    df = df.withColumn("silver_processing_timestamp", F.current_timestamp())

    valid_df, _ = quarantine_records(
        df, F.col("is_valid") == True,
        str(get_quarantine_path("marketing_touches")), "marketing_touches"
    )

    valid_df.write.mode("overwrite").parquet(str(get_silver_path("marketing_touches")))
    logger.info("marketing_touches Silver transformation completed")


def main():
    spark = get_spark_session("MarketingTouches Silver Transformation")
    try:
        transform_marketing_touches_to_silver(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
