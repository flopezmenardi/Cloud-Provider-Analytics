"""Gold mart: tickets_by_org_date

Support ticket metrics by organization, date, and severity.
"""
import logging, sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import get_silver_path, get_gold_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_tickets_by_org_date(spark: SparkSession) -> None:
    """
    Create tickets_by_org_date Gold mart.

    Aggregates:
    - Ticket metrics by organization, date, and severity
    - Resolution times, SLA breaches, CSAT scores
    - Category breakdowns
    """
    logger.info("=" * 80)
    logger.info("Creating Gold mart: tickets_by_org_date")
    logger.info("=" * 80)

    # Read from Silver
    silver_path = get_silver_path("support_tickets")
    logger.info(f"Reading from Silver: {silver_path}")
    df = spark.read.parquet(str(silver_path))
    initial_count = df.count()
    logger.info(f"Initial Silver tickets: {initial_count:,}")

    # Add ticket_date from created_at
    df = df.withColumn("ticket_date", F.to_date(F.col("created_at")))

    # Calculate resolution_hours if resolved
    df = df.withColumn(
        "resolution_hours",
        F.when(
            F.col("resolved_at").isNotNull(),
            (F.unix_timestamp("resolved_at") - F.unix_timestamp("created_at")) / 3600
        ).otherwise(None)
    )

    # Aggregate by org_id, ticket_date, severity
    logger.info("Aggregating by org_id, ticket_date, severity...")

    agg_df = df.groupBy("org_id", "ticket_date", "severity").agg(
        # Total tickets
        F.count("*").alias("total_tickets"),

        # Resolution metrics (only for resolved tickets)
        F.avg("resolution_hours").alias("avg_resolution_hours"),

        # SLA breach metrics
        F.sum(F.when(F.col("sla_breached") == True, 1).otherwise(0)).alias("sla_breach_count"),

        # CSAT average
        F.avg("csat_score").alias("csat_avg"),

        # Category breakdowns
        F.sum(F.when(F.col("category") == "billing", 1).otherwise(0)).alias("billing_tickets"),
        F.sum(F.when(F.col("category") == "technical", 1).otherwise(0)).alias("technical_tickets"),
        F.sum(F.when(F.col("category") == "access", 1).otherwise(0)).alias("access_tickets"),
        F.sum(F.when(F.col("category") == "other", 1).otherwise(0)).alias("other_tickets")
    )

    # Calculate SLA breach rate
    agg_df = agg_df.withColumn(
        "sla_breach_rate",
        F.when(
            F.col("total_tickets") > 0,
            F.col("sla_breach_count") / F.col("total_tickets")
        ).otherwise(0.0)
    )

    final_count = agg_df.count()
    logger.info(f"Final aggregated rows: {final_count:,}")

    # Write to Gold
    gold_path = get_gold_path("tickets_by_org_date")
    logger.info(f"Writing to Gold: {gold_path}")

    agg_df.write.mode("overwrite").parquet(str(gold_path))

    logger.info("✓ tickets_by_org_date Gold mart created successfully")
    logger.info("=" * 80)


def main():
    spark = get_spark_session("Gold: tickets_by_org_date")
    try:
        create_tickets_by_org_date(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
