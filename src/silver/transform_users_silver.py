"""Silver layer transformation for users"""
import logging, sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import BRONZE_USERS, get_silver_path, get_quarantine_path
from src.common.data_quality import quarantine_records

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_users_to_silver(spark: SparkSession) -> None:
    logger.info("Starting users Silver transformation")

    df = spark.read.parquet(str(BRONZE_USERS))
    logger.info(f"Initial count: {df.count():,}")

    # Clean and standardize
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
    df = df.withColumn("role", F.lower(F.trim(F.col("role"))))

    # Parse last_login timestamp
    df = df.withColumn(
        "last_login",
        F.when(F.col("last_login").isNotNull(),
               F.to_timestamp(F.col("last_login"))).otherwise(F.lit(None))
    )

    # Validate active status (boolean)
    df = df.withColumn(
        "active",
        F.col("active").cast("boolean")
    )

    # Validations
    df = df.withColumn(
        "is_valid",
        F.col("user_id").isNotNull() &
        F.col("org_id").isNotNull() &
        F.col("email").isNotNull() &
        F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    )

    df = df.withColumn("silver_processing_timestamp", F.current_timestamp())

    # Quarantine and write
    valid_df, _ = quarantine_records(
        df, F.col("is_valid") == True,
        str(get_quarantine_path("users")), "users"
    )

    valid_df.write.mode("overwrite").parquet(str(get_silver_path("users")))
    logger.info("users Silver transformation completed")


def main():
    spark = get_spark_session("Users Silver Transformation")
    try:
        transform_users_to_silver(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
