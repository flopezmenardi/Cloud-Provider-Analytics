"""Silver layer transformation for billing_monthly"""
import logging, sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import BRONZE_BILLING_MONTHLY, get_silver_path, get_quarantine_path
from src.common.data_quality import quarantine_records

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_billing_to_silver(spark: SparkSession) -> None:
    logger.info("Starting billing_monthly Silver transformation")

    df = spark.read.parquet(str(BRONZE_BILLING_MONTHLY))
    logger.info(f"Initial count: {df.count():,}")

    # Handle NULL credits (default to 0)
    df = df.withColumn(
        "credits",
        F.when(F.col("credits").isNull(), 0).otherwise(F.col("credits"))
    )

    # Handle NULL taxes (default to 0)
    df = df.withColumn(
        "taxes",
        F.when(F.col("taxes").isNull(), 0).otherwise(F.col("taxes"))
    )

    # Convert subtotal to USD (rename to amount_usd for consistency)
    df = df.withColumn(
        "amount_usd",
        F.col("subtotal") * F.coalesce(F.col("exchange_rate_to_usd"), F.lit(1.0))
    )

    df = df.withColumn(
        "credits_usd",
        F.col("credits") * F.coalesce(F.col("exchange_rate_to_usd"), F.lit(1.0))
    )

    df = df.withColumn(
        "taxes_usd",
        F.col("taxes") * F.coalesce(F.col("exchange_rate_to_usd"), F.lit(1.0))
    )

    # Calculate net_amount = amount_usd - credits_usd + taxes_usd
    df = df.withColumn(
        "net_amount",
        F.col("amount_usd") - F.col("credits_usd") + F.col("taxes_usd")
    )

    # Standardize currency to USD
    df = df.withColumn("currency", F.lit("USD"))

    # Validations
    df = df.withColumn(
        "is_valid",
        F.col("invoice_id").isNotNull() &
        F.col("org_id").isNotNull() &
        (F.col("amount_usd") >= 0)
    )

    df = df.withColumn("silver_processing_timestamp", F.current_timestamp())

    valid_df, _ = quarantine_records(
        df, F.col("is_valid") == True,
        str(get_quarantine_path("billing_monthly")), "billing_monthly"
    )

    valid_df.write.mode("overwrite").parquet(str(get_silver_path("billing_monthly")))
    logger.info("billing_monthly Silver transformation completed")


def main():
    spark = get_spark_session("Billing Silver Transformation")
    try:
        transform_billing_to_silver(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
