"""
Silver layer transformation for customers_orgs
"""

import logging
import sys
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import BRONZE_CUSTOMERS_ORGS, get_silver_path, get_quarantine_path
from src.common.data_quality import quarantine_records

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_customers_orgs_to_silver(spark: SparkSession) -> None:
    """
    Transform customers_orgs from Bronze to Silver.

    Transformations:
    - Standardize industry, region, tier (lowercase, trim)
    - Normalize NPS scores (validate 0-10)
    - Handle nulls in optional fields
    - Validate org_id not null
    """
    logger.info("Starting customers_orgs Silver transformation")

    # Read from Bronze
    df = spark.read.parquet(str(BRONZE_CUSTOMERS_ORGS))
    initial_count = df.count()
    logger.info(f"Initial record count: {initial_count:,}")

    # Standardize text fields
    df = df.withColumn("industry", F.lower(F.trim(F.col("industry"))))
    df = df.withColumn("hq_region", F.lower(F.trim(F.col("hq_region"))))
    df = df.withColumn("plan_tier", F.lower(F.trim(F.col("plan_tier"))))

    # Normalize NPS score (should be between 0-10, or null)
    df = df.withColumn(
        "nps_score",
        F.when(
            F.col("nps_score").isNotNull() &
            (F.col("nps_score") >= 0) &
            (F.col("nps_score") <= 10),
            F.col("nps_score")
        ).otherwise(F.lit(None))
    )

    # Validations
    df = df.withColumn(
        "is_valid",
        F.col("org_id").isNotNull() &
        F.col("org_name").isNotNull()
    )

    # Add silver processing timestamp
    df = df.withColumn("silver_processing_timestamp", F.current_timestamp())

    # Quarantine invalid records
    valid_df, _ = quarantine_records(
        df,
        valid_condition=F.col("is_valid") == True,
        quarantine_path=str(get_quarantine_path("customers_orgs")),
        source_name="customers_orgs"
    )

    # Write to Silver
    silver_path = get_silver_path("customers_orgs")
    valid_df.write.mode("overwrite").parquet(str(silver_path))

    logger.info(f"customers_orgs Silver transformation completed: {silver_path}")


def main():
    spark = get_spark_session("CustomersOrgs Silver Transformation")
    try:
        transform_customers_orgs_to_silver(spark)
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
