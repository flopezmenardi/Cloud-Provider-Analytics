"""Silver layer transformation for nps_surveys"""
import logging, sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import BRONZE_NPS_SURVEYS, get_silver_path, get_quarantine_path
from src.common.data_quality import quarantine_records

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_nps_surveys_to_silver(spark: SparkSession) -> None:
    logger.info("Starting nps_surveys Silver transformation")

    df = spark.read.parquet(str(BRONZE_NPS_SURVEYS))
    logger.info(f"Initial count: {df.count():,}")

    # Parse timestamp
    df = df.withColumn("survey_date", F.to_timestamp(F.col("survey_date")))

    # Validate NPS score (0-10)
    df = df.withColumn(
        "nps_score",
        F.when(
            F.col("nps_score").isNotNull() &
            (F.col("nps_score") >= 0) &
            (F.col("nps_score") <= 10),
            F.col("nps_score")
        ).otherwise(F.lit(None))
    )

    # Validations (CSV has: org_id, survey_date, nps_score, comment - no survey_id)
    df = df.withColumn(
        "is_valid",
        F.col("org_id").isNotNull() &
        F.col("nps_score").isNotNull()
    )

    df = df.withColumn("silver_processing_timestamp", F.current_timestamp())

    valid_df, _ = quarantine_records(
        df, F.col("is_valid") == True,
        str(get_quarantine_path("nps_surveys")), "nps_surveys"
    )

    valid_df.write.mode("overwrite").parquet(str(get_silver_path("nps_surveys")))
    logger.info("nps_surveys Silver transformation completed")


def main():
    spark = get_spark_session("NPSSurveys Silver Transformation")
    try:
        transform_nps_surveys_to_silver(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
