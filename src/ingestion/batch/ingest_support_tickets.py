"""
Bronze layer ingestion for support_tickets.csv
"""
import logging
from pyspark.sql import SparkSession

from src.config.spark_config import get_spark_session
from src.config.paths import LANDING_SUPPORT_TICKETS
from src.ingestion.batch.bronze_ingestion_base import BronzeIngestionBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_support_tickets(spark: SparkSession, mode: str = "overwrite") -> None:
    """
    Ingest support_tickets.csv to bronze layer
    
    Args:
        spark: SparkSession
        mode: Write mode (overwrite, append)
    """
    ingestion = BronzeIngestionBase(
        spark=spark,
        source_file=LANDING_SUPPORT_TICKETS,
        target_table="support_tickets"
    )
    ingestion.ingest(mode=mode)


if __name__ == "__main__":
    spark = get_spark_session("IngestSupportTickets")
    try:
        ingest_support_tickets(spark)
        logger.info("Support tickets ingestion completed successfully")
    except Exception as e:
        logger.error(f"Support tickets ingestion failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

