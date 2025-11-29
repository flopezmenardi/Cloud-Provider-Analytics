"""Silver layer transformation for support_tickets"""
import logging, sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import BRONZE_SUPPORT_TICKETS, get_silver_path, get_quarantine_path
from src.common.data_quality import quarantine_records

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VALID_STATUS = ['open', 'in_progress', 'resolved', 'closed']
VALID_SEVERITY = ['low', 'medium', 'high', 'critical']


def transform_support_tickets_to_silver(spark: SparkSession) -> None:
    logger.info("Starting support_tickets Silver transformation")

    df = spark.read.parquet(str(BRONZE_SUPPORT_TICKETS))
    logger.info(f"Initial count: {df.count():,}")

    # Standardize severity and category (no status column in CSV)
    df = df.withColumn("severity", F.lower(F.trim(F.col("severity"))))
    df = df.withColumn("category", F.lower(F.trim(F.col("category"))))

    # Parse timestamps
    df = df.withColumn("created_at", F.to_timestamp(F.col("created_at")))
    df = df.withColumn("resolved_at", F.to_timestamp(F.col("resolved_at")))

    # Calculate resolution_time_hours
    df = df.withColumn(
        "resolution_time_hours",
        F.when(
            F.col("resolved_at").isNotNull(),
            (F.unix_timestamp(F.col("resolved_at")) -
             F.unix_timestamp(F.col("created_at"))) / 3600.0
        ).otherwise(F.lit(None))
    )

    # Validate CSAT score (1-5 if exists) - column name is "csat" not "csat_score"
    df = df.withColumn(
        "csat_score",
        F.when(
            F.col("csat").isNotNull() &
            (F.col("csat") >= 1) &
            (F.col("csat") <= 5),
            F.col("csat")
        ).otherwise(F.lit(None))
    )

    # Validations (no status column in CSV, only severity)
    df = df.withColumn(
        "is_valid",
        F.col("ticket_id").isNotNull() &
        F.col("org_id").isNotNull() &
        F.col("severity").isin(VALID_SEVERITY)
    )

    df = df.withColumn("silver_processing_timestamp", F.current_timestamp())

    valid_df, _ = quarantine_records(
        df, F.col("is_valid") == True,
        str(get_quarantine_path("support_tickets")), "support_tickets"
    )

    valid_df.write.mode("overwrite").parquet(str(get_silver_path("support_tickets")))
    logger.info("support_tickets Silver transformation completed")


def main():
    spark = get_spark_session("SupportTickets Silver Transformation")
    try:
        transform_support_tickets_to_silver(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
