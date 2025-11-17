"""
Bronze layer ingestion for nps_surveys.csv
"""
import logging
from pyspark.sql import SparkSession

from src.config.spark_config import get_spark_session
from src.config.paths import LANDING_NPS_SURVEYS
from src.batch.bronze_ingestion_base import BronzeIngestionBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_nps_surveys(spark: SparkSession, mode: str = "overwrite") -> None:
    """
    Ingest nps_surveys.csv to bronze layer
    
    Args:
        spark: SparkSession
        mode: Write mode (overwrite, append)
    """
    ingestion = BronzeIngestionBase(
        spark=spark,
        source_file=LANDING_NPS_SURVEYS,
        target_table="nps_surveys"
    )
    ingestion.ingest(mode=mode)


if __name__ == "__main__":
    spark = get_spark_session("IngestNPSSurveys")
    try:
        ingest_nps_surveys(spark)
        logger.info("NPS surveys ingestion completed successfully")
    except Exception as e:
        logger.error(f"NPS surveys ingestion failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

