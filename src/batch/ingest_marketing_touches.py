"""
Bronze layer ingestion for marketing_touches.csv
"""
import logging
from pyspark.sql import SparkSession

from src.config.spark_config import get_spark_session
from src.config.paths import LANDING_MARKETING_TOUCHES
from src.batch.bronze_ingestion_base import BronzeIngestionBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_marketing_touches(spark: SparkSession, mode: str = "overwrite") -> None:
    """
    Ingest marketing_touches.csv to bronze layer
    
    Args:
        spark: SparkSession
        mode: Write mode (overwrite, append)
    """
    ingestion = BronzeIngestionBase(
        spark=spark,
        source_file=LANDING_MARKETING_TOUCHES,
        target_table="marketing_touches"
    )
    ingestion.ingest(mode=mode)


if __name__ == "__main__":
    spark = get_spark_session("IngestMarketingTouches")
    try:
        ingest_marketing_touches(spark)
        logger.info("Marketing touches ingestion completed successfully")
    except Exception as e:
        logger.error(f"Marketing touches ingestion failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

