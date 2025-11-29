"""
Orchestrator script to ingest all batch sources to bronze layer
"""
import logging
from pyspark.sql import SparkSession

from src.config.spark_config import get_spark_session
from src.ingestion.batch.ingest_users import ingest_users
from src.ingestion.batch.ingest_customers_orgs import ingest_customers_orgs
from src.ingestion.batch.ingest_resources import ingest_resources
from src.ingestion.batch.ingest_billing_monthly import ingest_billing_monthly
from src.ingestion.batch.ingest_support_tickets import ingest_support_tickets
from src.ingestion.batch.ingest_marketing_touches import ingest_marketing_touches
from src.ingestion.batch.ingest_nps_surveys import ingest_nps_surveys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_all_batch_sources(spark: SparkSession, mode: str = "overwrite") -> None:
    """
    Ingest all batch sources to bronze layer
    
    Args:
        spark: SparkSession
        mode: Write mode (overwrite, append)
    """
    ingestion_jobs = [
        ("Users", ingest_users),
        ("Customers/Orgs", ingest_customers_orgs),
        ("Resources", ingest_resources),
        ("Billing Monthly", ingest_billing_monthly),
        ("Support Tickets", ingest_support_tickets),
        ("Marketing Touches", ingest_marketing_touches),
        ("NPS Surveys", ingest_nps_surveys),
    ]
    
    for name, ingest_func in ingestion_jobs:
        try:
            logger.info(f"Starting ingestion for {name}")
            ingest_func(spark, mode=mode)
            logger.info(f"Completed ingestion for {name}")
        except Exception as e:
            logger.error(f"Failed to ingest {name}: {str(e)}", exc_info=True)
            raise


if __name__ == "__main__":
    spark = get_spark_session("IngestAllBatchSources")
    try:
        ingest_all_batch_sources(spark)
        logger.info("All batch sources ingested successfully")
    except Exception as e:
        logger.error(f"Batch ingestion failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

