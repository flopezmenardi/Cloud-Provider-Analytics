"""
Bronze layer ingestion for resources.csv
"""
import logging
from pyspark.sql import SparkSession

from src.config.spark_config import get_spark_session
from src.config.paths import LANDING_RESOURCES
from src.ingestion.batch.bronze_ingestion_base import BronzeIngestionBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_resources(spark: SparkSession, mode: str = "overwrite") -> None:
    """
    Ingest resources.csv to bronze layer
    
    Args:
        spark: SparkSession
        mode: Write mode (overwrite, append)
    """
    ingestion = BronzeIngestionBase(
        spark=spark,
        source_file=LANDING_RESOURCES,
        target_table="resources"
    )
    ingestion.ingest(mode=mode)


if __name__ == "__main__":
    spark = get_spark_session("IngestResources")
    try:
        ingest_resources(spark)
        logger.info("Resources ingestion completed successfully")
    except Exception as e:
        logger.error(f"Resources ingestion failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

