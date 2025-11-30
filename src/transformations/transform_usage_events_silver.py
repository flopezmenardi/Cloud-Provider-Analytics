"""
Silver layer transformation for usage_events
Handles schema version compatibility (v1/v2), data quality, and enrichment.
"""

import logging
import sys
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import (
    BRONZE_USAGE_EVENTS,
    get_silver_path,
    get_quarantine_path
)
from src.common.data_quality import (
    add_anomaly_flags,
    quarantine_records,
    validate_ranges
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def normalize_value_column(df: DataFrame) -> DataFrame:
    """
    Convert value column from string to double with fallback.
    Handles mixed string/numeric types.
    """
    df = df.withColumn(
        "value_normalized",
        F.when(
            F.col("value").isNotNull() & (F.trim(F.col("value")) != ""),
            F.col("value").cast(DoubleType())
        ).otherwise(F.lit(None))
    )
    return df


def compatibilize_schema_versions(df: DataFrame) -> DataFrame:
    """
    Create unified columns for schema v1 and v2.
    - v1: carbon_kg and genai_tokens are null
    - v2: use actual values if present
    """
    # Ensure schema_version column exists
    if "schema_version" not in df.columns:
        df = df.withColumn("schema_version", F.lit(1))

    # Unified carbon_kg column
    df = df.withColumn(
        "carbon_kg_unified",
        F.when(F.col("schema_version") == 2, F.col("carbon_kg"))
        .otherwise(F.lit(None).cast(DoubleType()))
    )

    # Unified genai_tokens column
    df = df.withColumn(
        "genai_tokens_unified",
        F.when(F.col("schema_version") == 2, F.col("genai_tokens"))
        .otherwise(F.lit(None))
    )

    # Drop original columns and rename unified ones
    df = df.drop("carbon_kg", "genai_tokens")
    df = df.withColumnRenamed("carbon_kg_unified", "carbon_kg")
    df = df.withColumnRenamed("genai_tokens_unified", "genai_tokens")

    return df


def normalize_dimensions(df: DataFrame) -> DataFrame:
    """
    Normalize and standardize dimension columns.
    """
    # Standardize region names (lowercase, trim)
    df = df.withColumn(
        "region",
        F.lower(F.trim(F.col("region")))
    )

    # Standardize service names (lowercase, trim)
    df = df.withColumn(
        "service",
        F.lower(F.trim(F.col("service")))
    )

    # Standardize metric names (lowercase, trim)
    df = df.withColumn(
        "metric",
        F.lower(F.trim(F.col("metric")))
    )

    # Standardize unit (lowercase, trim)
    df = df.withColumn(
        "unit",
        F.when(
            F.col("unit").isNotNull(),
            F.lower(F.trim(F.col("unit")))
        ).otherwise(F.lit(None))
    )

    return df


def apply_validations(df: DataFrame) -> DataFrame:
    """
    Apply data quality validations per PDF requirements.

    Validations:
    1. event_id not null and unique (dedup already in Bronze)
    2. cost_usd_increment >= -0.01
    3. unit not null when value not null (or imputation)
    4. schema_version handled
    """
    # Validation 1: event_id not null
    df = df.withColumn(
        "valid_event_id",
        F.col("event_id").isNotNull()
    )

    # Validation 2: cost_usd_increment >= -0.01
    df = df.withColumn(
        "valid_cost",
        F.col("cost_usd_increment").isNotNull() &
        (F.col("cost_usd_increment") >= -0.01)
    )

    # Validation 3: unit not null when value_normalized not null
    # Impute unit if value exists but unit is missing
    df = df.withColumn(
        "unit",
        F.when(
            (F.col("value_normalized").isNotNull()) & (F.col("unit").isNull()),
            F.lit("unknown")  # Imputation
        ).otherwise(F.col("unit"))
    )

    df = df.withColumn(
        "valid_unit",
        F.when(
            F.col("value_normalized").isNotNull(),
            F.col("unit").isNotNull()
        ).otherwise(F.lit(True))  # If no value, unit validation passes
    )

    # Validation 4: schema_version is valid (1 or 2)
    df = df.withColumn(
        "valid_schema_version",
        F.col("schema_version").isin([1, 2])
    )

    # Combined validation flag
    df = df.withColumn(
        "is_valid",
        F.col("valid_event_id") &
        F.col("valid_cost") &
        F.col("valid_unit") &
        F.col("valid_schema_version")
    )

    return df


def enrich_with_resources(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Enrich usage events with resource metadata.
    Join with resources table from Silver (or Bronze if Silver not available).
    """
    try:
        # Try to read from Silver first, fallback to Bronze
        try:
            resources_df = spark.read.parquet(get_silver_path("resources"))
            logger.info("Enriching with Silver resources")
        except:
            from src.config.paths import BRONZE_RESOURCES
            resources_df = spark.read.parquet(str(BRONZE_RESOURCES))
            logger.info("Enriching with Bronze resources (Silver not available)")

        # Select relevant columns (note: no resource_type in CSV)
        resources_df = resources_df.select(
            F.col("resource_id"),
            F.col("service").alias("resource_service"),
            F.col("state")
        )

        # Left join (preserve all usage events even if resource not found)
        df = df.join(
            resources_df,
            on="resource_id",
            how="left"
        )

        # Flag if resource was found
        df = df.withColumn(
            "resource_found",
            F.col("resource_service").isNotNull()
        )

    except Exception as e:
        logger.warning(f"Could not enrich with resources: {e}")
        df = df.withColumn("resource_found", F.lit(False))

    return df


def enrich_with_customers(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Enrich usage events with customer/org metadata.
    Join with customers_orgs table from Silver (or Bronze if Silver not available).
    """
    try:
        # Try to read from Silver first, fallback to Bronze
        try:
            customers_df = spark.read.parquet(get_silver_path("customers_orgs"))
            logger.info("Enriching with Silver customers_orgs")
        except:
            from src.config.paths import BRONZE_CUSTOMERS_ORGS
            customers_df = spark.read.parquet(str(BRONZE_CUSTOMERS_ORGS))
            logger.info("Enriching with Bronze customers_orgs (Silver not available)")

        # Select relevant columns
        customers_df = customers_df.select(
            F.col("org_id"),
            F.col("org_name"),
            F.col("plan_tier").alias("tier"),  # Rename plan_tier to tier for consistency
            F.col("industry")
        )

        # Left join
        df = df.join(
            customers_df,
            on="org_id",
            how="left"
        )

        # Flag if org was found
        df = df.withColumn(
            "org_found",
            F.col("org_name").isNotNull()
        )

    except Exception as e:
        logger.warning(f"Could not enrich with customers: {e}")
        df = df.withColumn("org_found", F.lit(False))

    return df


def enrich_with_users(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Enrich usage events with user metadata.
    Join with users table from Silver (or Bronze if Silver not available).
    
    Required per project spec: "join a orgs/users/resources"
    """
    try:
        # Try to read from Silver first, fallback to Bronze
        try:
            users_df = spark.read.parquet(get_silver_path("users"))
            logger.info("Enriching with Silver users")
        except:
            from src.config.paths import BRONZE_USERS
            users_df = spark.read.parquet(str(BRONZE_USERS))
            logger.info("Enriching with Bronze users (Silver not available)")

        # Get user count per org for enrichment
        users_per_org = users_df.groupBy("org_id").agg(
            F.count("*").alias("org_user_count"),
            F.countDistinct("role").alias("org_role_count")
        )

        # Left join
        df = df.join(
            users_per_org,
            on="org_id",
            how="left"
        )

        # Fill nulls with 0
        df = df.withColumn(
            "org_user_count",
            F.coalesce(F.col("org_user_count"), F.lit(0))
        )
        df = df.withColumn(
            "org_role_count",
            F.coalesce(F.col("org_role_count"), F.lit(0))
        )

        logger.info("User enrichment completed")

    except Exception as e:
        logger.warning(f"Could not enrich with users: {e}")
        df = df.withColumn("org_user_count", F.lit(0))
        df = df.withColumn("org_role_count", F.lit(0))

    return df


def add_derived_fields(df: DataFrame) -> DataFrame:
    """
    Add derived fields for analytics.
    """
    # Parse timestamp if it's a string
    df = df.withColumn(
        "timestamp",
        F.when(
            F.col("timestamp").cast("string").isNotNull(),
            F.to_timestamp(F.col("timestamp"))
        ).otherwise(F.col("timestamp"))
    )

    # Extract date components for partitioning
    df = df.withColumn("event_date", F.to_date(F.col("timestamp")))
    df = df.withColumn("event_year", F.year(F.col("timestamp")))
    df = df.withColumn("event_month", F.month(F.col("timestamp")))
    df = df.withColumn("event_day", F.dayofmonth(F.col("timestamp")))

    # Add processing timestamp
    df = df.withColumn("silver_processing_timestamp", F.current_timestamp())

    return df


def transform_usage_events_to_silver(spark: SparkSession) -> None:
    """
    Main transformation function for usage_events Bronze -> Silver.

    Steps:
    1. Read from Bronze
    2. Normalize value column (string -> double)
    3. Compatibilize schema versions (v1/v2)
    4. Normalize dimensions (region, service, metric)
    5. Apply validations
    6. Enrich with resources
    7. Enrich with customers/orgs
    8. Enrich with users (per spec: join to orgs/users/resources)
    9. Add derived fields
    10. Detect anomalies (3 methods)
    11. Split valid/quarantine
    12. Write to Silver
    """
    logger.info("=" * 80)
    logger.info("Starting usage_events Silver transformation")
    logger.info("=" * 80)

    # Read from Bronze
    logger.info(f"Reading from Bronze: {BRONZE_USAGE_EVENTS}")
    df = spark.read.parquet(str(BRONZE_USAGE_EVENTS))

    initial_count = df.count()
    logger.info(f"Initial record count: {initial_count:,}")

    # Step 1: Normalize value column
    logger.info("Step 1: Normalizing value column (string -> double)")
    df = normalize_value_column(df)

    # Step 2: Compatibilize schema versions
    logger.info("Step 2: Compatibilizing schema versions (v1/v2)")
    df = compatibilize_schema_versions(df)

    # Count records by schema version
    version_counts = df.groupBy("schema_version").count().collect()
    for row in version_counts:
        logger.info(f"  Schema v{row['schema_version']}: {row['count']:,} records")

    # Step 3: Normalize dimensions
    logger.info("Step 3: Normalizing dimensions (region, service, metric)")
    df = normalize_dimensions(df)

    # Step 4: Apply validations
    logger.info("Step 4: Applying data quality validations")
    df = apply_validations(df)

    # Step 5: Enrich with resources
    logger.info("Step 5: Enriching with resources metadata")
    df = enrich_with_resources(df, spark)

    # Step 6: Enrich with customers
    logger.info("Step 6: Enriching with customers/orgs metadata")
    df = enrich_with_customers(df, spark)

    # Step 7: Enrich with users (required by spec)
    logger.info("Step 7: Enriching with users metadata")
    df = enrich_with_users(df, spark)

    # Step 8: Add derived fields
    logger.info("Step 8: Adding derived fields")
    df = add_derived_fields(df)

    # Step 9: Detect anomalies using 3 methods
    logger.info("Step 9: Detecting anomalies using 3 methods (z-score, MAD, percentiles)")
    df = add_anomaly_flags(
        df,
        col="cost_usd_increment",
        methods=['zscore', 'mad', 'percentile'],
        zscore_threshold=3.0,
        mad_threshold=3.0,
        percentile_lower=0.01,
        percentile_upper=0.99
    )

    # Count anomalies
    anomaly_count = df.filter(F.col("is_anomaly") == True).count()
    logger.info(f"  Anomalies detected: {anomaly_count:,} ({100*anomaly_count/initial_count:.2f}%)")

    # Step 10: Split valid/quarantine
    logger.info("Step 10: Splitting valid and quarantine records")
    quarantine_path = get_quarantine_path("usage_events")

    valid_df, quarantine_df = quarantine_records(
        df,
        valid_condition=F.col("is_valid") == True,
        quarantine_path=quarantine_path,
        source_name="usage_events"
    )

    # Step 11: Write to Silver
    silver_path = get_silver_path("usage_events")
    logger.info(f"Step 11: Writing to Silver: {silver_path}")

    # Write partitioned by year and month for query performance
    valid_df.write.mode("overwrite") \
        .partitionBy("event_year", "event_month") \
        .parquet(str(silver_path))

    final_count = valid_df.count()
    logger.info(f"Final valid record count: {final_count:,}")
    logger.info(f"Data quality rate: {100*final_count/initial_count:.2f}%")

    logger.info("=" * 80)
    logger.info("usage_events Silver transformation completed successfully")
    logger.info("=" * 80)


def main():
    """Main entry point"""
    spark = get_spark_session("UsageEvents Silver Transformation")

    try:
        transform_usage_events_to_silver(spark)
    except Exception as e:
        logger.error(f"Error in Silver transformation: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
