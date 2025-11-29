"""Gold mart: revenue_by_org_month

Monthly revenue aggregations with USD normalization and enrichment.
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


def create_revenue_by_org_month(spark: SparkSession) -> None:
    """
    Create revenue_by_org_month Gold mart.

    Aggregates:
    - Monthly revenue by organization
    - USD normalized billing, credits, taxes
    - Enriched with org metadata (name, tier, industry, region)
    """
    logger.info("=" * 80)
    logger.info("Creating Gold mart: revenue_by_org_month")
    logger.info("=" * 80)

    # Read billing from Silver
    billing_path = get_silver_path("billing_monthly")
    logger.info(f"Reading billing from Silver: {billing_path}")
    billing_df = spark.read.parquet(str(billing_path))
    logger.info(f"Billing records: {billing_df.count():,}")

    # Read customers_orgs for enrichment
    customers_path = get_silver_path("customers_orgs")
    logger.info(f"Reading customers_orgs from Silver: {customers_path}")
    customers_df = spark.read.parquet(str(customers_path))
    logger.info(f"Customer records: {customers_df.count():,}")

    # Extract year_month from month column (format: YYYY-MM)
    billing_df = billing_df.withColumn("year_month", F.col("month"))

    # Aggregate by org_id and year_month
    logger.info("Aggregating by org_id, year_month...")

    agg_df = billing_df.groupBy("org_id", "year_month").agg(
        # Revenue metrics (all in USD from Silver)
        F.sum("amount_usd").alias("total_billed_usd"),
        F.sum("credits_usd").alias("total_credits_usd"),
        F.sum("taxes_usd").alias("total_taxes_usd"),
        F.sum("net_amount").alias("net_revenue"),

        # Invoice count
        F.count("*").alias("invoice_count")
    )

    final_count = agg_df.count()
    logger.info(f"Aggregated revenue rows: {final_count:,}")

    # Enrich with customer metadata
    logger.info("Enriching with customer metadata...")
    customers_select = customers_df.select(
        F.col("org_id"),
        F.col("org_name"),
        F.col("industry"),
        F.col("plan_tier"),
        F.col("hq_region")
    )

    enriched_df = agg_df.join(customers_select, on="org_id", how="left")

    # Write to Gold
    gold_path = get_gold_path("revenue_by_org_month")
    logger.info(f"Writing to Gold: {gold_path}")

    enriched_df.write.mode("overwrite").parquet(str(gold_path))

    logger.info("✓ revenue_by_org_month Gold mart created successfully")
    logger.info("=" * 80)


def main():
    spark = get_spark_session("Gold: revenue_by_org_month")
    try:
        create_revenue_by_org_month(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
