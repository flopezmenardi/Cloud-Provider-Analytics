"""Silver layer transformation for resources"""
import logging, sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import BRONZE_RESOURCES, get_silver_path, get_quarantine_path
from src.common.data_quality import quarantine_records

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VALID_SERVICES = ['compute', 'storage', 'database', 'networking', 'analytics', 'genai']


def transform_resources_to_silver(spark: SparkSession) -> None:
    logger.info("Starting resources Silver transformation")

    df = spark.read.parquet(str(BRONZE_RESOURCES))
    logger.info(f"Initial count: {df.count():,}")

    # Standardize
    df = df.withColumn("service", F.lower(F.trim(F.col("service"))))
    df = df.withColumn("region", F.lower(F.trim(F.col("region"))))
    df = df.withColumn("state", F.lower(F.trim(F.col("state"))))

    # Validate service is in valid list
    df = df.withColumn(
        "is_valid",
        F.col("resource_id").isNotNull() &
        F.col("org_id").isNotNull() &
        F.col("service").isin(VALID_SERVICES)
    )

    df = df.withColumn("silver_processing_timestamp", F.current_timestamp())

    valid_df, _ = quarantine_records(
        df, F.col("is_valid") == True,
        str(get_quarantine_path("resources")), "resources"
    )

    valid_df.write.mode("overwrite").parquet(str(get_silver_path("resources")))
    logger.info("resources Silver transformation completed")


def main():
    spark = get_spark_session("Resources Silver Transformation")
    try:
        transform_resources_to_silver(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
