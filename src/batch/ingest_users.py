"""
Bronze layer ingestion for users.csv
"""
import logging
from pyspark.sql import SparkSession

from src.config.spark_config import get_spark_session
from src.config.paths import LANDING_USERS
from src.batch.bronze_ingestion_base import BronzeIngestionBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_users(spark: SparkSession, mode: str = "overwrite") -> None:
    """
    Ingest users.csv to bronze layer
    
    Args:
        spark: SparkSession
        mode: Write mode (overwrite, append)
    """
    ingestion = BronzeIngestionBase(
        spark=spark,
        source_file=LANDING_USERS,
        target_table="users"
    )
    ingestion.ingest(mode=mode)


if __name__ == "__main__":
    spark = get_spark_session("IngestUsers")
    try:
        ingest_users(spark)
        logger.info("Users ingestion completed successfully")
    except Exception as e:
        logger.error(f"Users ingestion failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

