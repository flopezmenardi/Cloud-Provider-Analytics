"""
Bronze layer ingestion for customers_orgs.csv
"""
import logging
from pyspark.sql import SparkSession

from src.config.spark_config import get_spark_session
from src.config.paths import LANDING_CUSTOMERS_ORGS
from src.batch.bronze_ingestion_base import BronzeIngestionBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_customers_orgs(spark: SparkSession, mode: str = "overwrite") -> None:
    """
    Ingest customers_orgs.csv to bronze layer
    
    Args:
        spark: SparkSession
        mode: Write mode (overwrite, append)
    """
    ingestion = BronzeIngestionBase(
        spark=spark,
        source_file=LANDING_CUSTOMERS_ORGS,
        target_table="customers_orgs"
    )
    ingestion.ingest(mode=mode)


if __name__ == "__main__":
    spark = get_spark_session("IngestCustomersOrgs")
    try:
        ingest_customers_orgs(spark)
        logger.info("Customers/Orgs ingestion completed successfully")
    except Exception as e:
        logger.error(f"Customers/Orgs ingestion failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

