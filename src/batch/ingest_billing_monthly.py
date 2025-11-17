"""
Bronze layer ingestion for billing_monthly.csv
"""
import logging
from pyspark.sql import SparkSession

from src.config.spark_config import get_spark_session
from src.config.paths import LANDING_BILLING_MONTHLY
from src.batch.bronze_ingestion_base import BronzeIngestionBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_billing_monthly(spark: SparkSession, mode: str = "overwrite") -> None:
    """
    Ingest billing_monthly.csv to bronze layer
    
    Args:
        spark: SparkSession
        mode: Write mode (overwrite, append)
    """
    ingestion = BronzeIngestionBase(
        spark=spark,
        source_file=LANDING_BILLING_MONTHLY,
        target_table="billing_monthly"
    )
    ingestion.ingest(mode=mode)


if __name__ == "__main__":
    spark = get_spark_session("IngestBillingMonthly")
    try:
        ingest_billing_monthly(spark)
        logger.info("Billing monthly ingestion completed successfully")
    except Exception as e:
        logger.error(f"Billing monthly ingestion failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

