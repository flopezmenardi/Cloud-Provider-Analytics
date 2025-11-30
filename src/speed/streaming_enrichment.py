"""
Speed Layer - Streaming Enrichment

Enriches streaming events with customer and resource metadata in real-time.
Part of Lambda Architecture speed layer.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, LongType
)
from pyspark.sql.streaming import StreamingQuery

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import (
    LANDING_USAGE_EVENTS_STREAM,
    DATALAKE_ROOT,
    get_silver_path
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Speed layer output paths
SPEED_ROOT = DATALAKE_ROOT / "speed"
SPEED_ENRICHED = SPEED_ROOT / "enriched_events"
SPEED_CHECKPOINT = SPEED_ROOT / "checkpoints"

# Ensure directories exist
SPEED_ROOT.mkdir(parents=True, exist_ok=True)
SPEED_ENRICHED.mkdir(parents=True, exist_ok=True)
SPEED_CHECKPOINT.mkdir(parents=True, exist_ok=True)


# Schema for usage events
USAGE_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("org_id", StringType(), True),
    StructField("resource_id", StringType(), True),
    StructField("service", StringType(), True),
    StructField("region", StringType(), True),
    StructField("metric", StringType(), True),
    StructField("value", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("cost_usd_increment", DoubleType(), True),
    StructField("schema_version", IntegerType(), True),
    StructField("carbon_kg", DoubleType(), True),
    StructField("genai_tokens", LongType(), True),
])


class StreamingEnricher:
    """
    Real-time streaming enrichment using Spark Structured Streaming.
    Joins streaming events with dimension tables (customers, resources).
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.customers_df = None
        self.resources_df = None
        self.queries = {}
    
    def _load_dimension_tables(self):
        """
        Load dimension tables from Silver layer for enrichment.
        These are broadcast-joined with the stream.
        """
        logger.info("Loading dimension tables from Silver layer...")
        
        # Load customers/orgs
        customers_path = get_silver_path("customers_orgs")
        if customers_path.exists():
            self.customers_df = self.spark.read.parquet(str(customers_path))
            # Select only needed columns for broadcast efficiency
            self.customers_df = self.customers_df.select(
                F.col("org_id"),
                F.col("org_name"),
                F.col("industry"),
                F.col("tier"),
                F.col("country")
            ).dropDuplicates(["org_id"])
            
            # Cache and broadcast hint
            self.customers_df = F.broadcast(self.customers_df)
            logger.info(f"  Loaded {self.customers_df.count()} customers/orgs")
        else:
            logger.warning(f"  Customers Silver table not found at {customers_path}")
            self.customers_df = None
        
        # Load resources
        resources_path = get_silver_path("resources")
        if resources_path.exists():
            self.resources_df = self.spark.read.parquet(str(resources_path))
            self.resources_df = self.resources_df.select(
                F.col("resource_id"),
                F.col("resource_name"),
                F.col("resource_type"),
                F.col("status").alias("resource_status")
            ).dropDuplicates(["resource_id"])
            
            self.resources_df = F.broadcast(self.resources_df)
            logger.info(f"  Loaded {self.resources_df.count()} resources")
        else:
            logger.warning(f"  Resources Silver table not found at {resources_path}")
            self.resources_df = None
    
    def _prepare_stream(self) -> DataFrame:
        """
        Prepare the streaming DataFrame with proper schema and timestamp parsing.
        """
        stream_df = self.spark.readStream \
            .option("maxFilesPerTrigger", 5) \
            .schema(USAGE_EVENT_SCHEMA) \
            .json(str(LANDING_USAGE_EVENTS_STREAM / "*.jsonl"))
        
        # Parse timestamp
        stream_df = stream_df.withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        
        # Handle value column
        stream_df = stream_df.withColumn(
            "value_numeric",
            F.when(
                F.col("value").isNull() | (F.col("value") == "null"),
                None
            ).otherwise(
                F.col("value").cast("double")
            )
        )
        
        return stream_df
    
    def create_enriched_stream(
        self,
        watermark_delay: str = "10 minutes"
    ) -> StreamingQuery:
        """
        Create enriched event stream with customer and resource metadata.
        
        Use case: Real-time events enriched with org name, industry, resource type
        for downstream analytics and dashboards.
        """
        logger.info("Starting enriched event stream...")
        
        # Load dimension tables
        self._load_dimension_tables()
        
        stream_df = self._prepare_stream()
        stream_df = stream_df.withWatermark("event_time", watermark_delay)
        
        # Enrich with customer data
        if self.customers_df is not None:
            stream_df = stream_df.join(
                self.customers_df,
                on="org_id",
                how="left"
            )
        else:
            # Add null columns if no customer data
            stream_df = stream_df.withColumn("org_name", F.lit(None).cast("string"))
            stream_df = stream_df.withColumn("industry", F.lit(None).cast("string"))
            stream_df = stream_df.withColumn("tier", F.lit(None).cast("string"))
            stream_df = stream_df.withColumn("country", F.lit(None).cast("string"))
        
        # Enrich with resource data
        if self.resources_df is not None:
            stream_df = stream_df.join(
                self.resources_df,
                on="resource_id",
                how="left"
            )
        else:
            stream_df = stream_df.withColumn("resource_name", F.lit(None).cast("string"))
            stream_df = stream_df.withColumn("resource_type", F.lit(None).cast("string"))
            stream_df = stream_df.withColumn("resource_status", F.lit(None).cast("string"))
        
        # Add derived metrics
        stream_df = stream_df.withColumn(
            "is_genai_event",
            F.when(F.col("genai_tokens").isNotNull() & (F.col("genai_tokens") > 0), True)
            .otherwise(False)
        )
        
        stream_df = stream_df.withColumn(
            "cost_category",
            F.when(F.col("cost_usd_increment") > 10.0, "high")
            .when(F.col("cost_usd_increment") > 1.0, "medium")
            .otherwise("low")
        )
        
        # Add processing timestamp
        stream_df = stream_df.withColumn(
            "enriched_at",
            F.current_timestamp()
        )
        
        # Select final columns
        enriched_df = stream_df.select(
            # Event fields
            "event_id",
            "event_time",
            "org_id",
            "resource_id",
            "service",
            "region",
            "metric",
            "value_numeric",
            "unit",
            "cost_usd_increment",
            "schema_version",
            "carbon_kg",
            "genai_tokens",
            # Enriched fields - customer
            "org_name",
            "industry",
            "tier",
            "country",
            # Enriched fields - resource
            "resource_name",
            "resource_type",
            "resource_status",
            # Derived fields
            "is_genai_event",
            "cost_category",
            "enriched_at"
        )
        
        # Write enriched stream
        query = enriched_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", str(SPEED_ENRICHED)) \
            .option("checkpointLocation", str(SPEED_CHECKPOINT / "enriched")) \
            .partitionBy("service") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        self.queries["enriched_events"] = query
        logger.info(f"Enriched stream query started: {query.id}")
        
        return query
    
    def create_industry_aggregations(
        self,
        window_duration: str = "15 minutes",
        watermark_delay: str = "10 minutes"
    ) -> StreamingQuery:
        """
        Create real-time industry-level aggregations.
        
        Use case: Track usage patterns by industry in real-time.
        """
        logger.info("Starting industry aggregation stream...")
        
        self._load_dimension_tables()
        
        if self.customers_df is None:
            logger.error("Cannot create industry aggregations without customer data")
            return None
        
        stream_df = self._prepare_stream()
        stream_df = stream_df.withWatermark("event_time", watermark_delay)
        
        # Join with customers
        stream_df = stream_df.join(
            self.customers_df,
            on="org_id",
            how="left"
        )
        
        # Aggregate by industry and window
        industry_df = stream_df.groupBy(
            F.window("event_time", window_duration),
            "industry"
        ).agg(
            F.sum("cost_usd_increment").alias("total_cost_usd"),
            F.count("*").alias("event_count"),
            F.countDistinct("org_id").alias("active_orgs"),
            F.countDistinct("service").alias("services_used"),
            F.avg("cost_usd_increment").alias("avg_cost_per_event")
        )
        
        # Flatten window
        industry_df = industry_df.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "industry",
            "total_cost_usd",
            "event_count",
            "active_orgs",
            "services_used",
            "avg_cost_per_event"
        )
        
        # Write
        query = industry_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", str(SPEED_ROOT / "industry_aggregations")) \
            .option("checkpointLocation", str(SPEED_CHECKPOINT / "industry_agg")) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        self.queries["industry_aggregations"] = query
        logger.info(f"Industry aggregation query started: {query.id}")
        
        return query
    
    def create_tier_metrics(
        self,
        window_duration: str = "10 minutes",
        watermark_delay: str = "10 minutes"
    ) -> StreamingQuery:
        """
        Create real-time metrics by customer tier.
        
        Use case: Monitor usage patterns by tier (enterprise, pro, free).
        """
        logger.info("Starting tier metrics stream...")
        
        self._load_dimension_tables()
        
        if self.customers_df is None:
            logger.error("Cannot create tier metrics without customer data")
            return None
        
        stream_df = self._prepare_stream()
        stream_df = stream_df.withWatermark("event_time", watermark_delay)
        
        # Join with customers
        stream_df = stream_df.join(
            self.customers_df,
            on="org_id",
            how="left"
        )
        
        # Aggregate by tier
        tier_df = stream_df.groupBy(
            F.window("event_time", window_duration),
            "tier"
        ).agg(
            F.sum("cost_usd_increment").alias("total_cost_usd"),
            F.count("*").alias("event_count"),
            F.countDistinct("org_id").alias("active_orgs"),
            F.sum(F.coalesce("genai_tokens", F.lit(0))).alias("genai_tokens"),
            F.sum(F.coalesce("carbon_kg", F.lit(0.0))).alias("carbon_kg")
        )
        
        # Flatten window
        tier_df = tier_df.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "tier",
            "total_cost_usd",
            "event_count",
            "active_orgs",
            "genai_tokens",
            "carbon_kg"
        )
        
        # Write
        query = tier_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", str(SPEED_ROOT / "tier_metrics")) \
            .option("checkpointLocation", str(SPEED_CHECKPOINT / "tier_metrics")) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        self.queries["tier_metrics"] = query
        logger.info(f"Tier metrics query started: {query.id}")
        
        return query
    
    def stop_all(self):
        """Stop all streaming queries."""
        for name, query in self.queries.items():
            if query and query.isActive:
                logger.info(f"Stopping query: {name}")
                query.stop()
        logger.info("All streaming queries stopped")
    
    def await_all(self, timeout: Optional[int] = None):
        """Wait for all streaming queries to terminate."""
        for name, query in self.queries.items():
            if query:
                logger.info(f"Awaiting termination: {name}")
                query.awaitTermination(timeout)


def run_streaming_enrichment(
    mode: str = "enriched",
    duration_seconds: Optional[int] = None
):
    """
    Run streaming enrichment.
    
    Args:
        mode: Which enrichment to run ("enriched", "industry", "tier", "all")
        duration_seconds: How long to run (None = indefinitely)
    """
    logger.info("=" * 80)
    logger.info("SPEED LAYER - STREAMING ENRICHMENT")
    logger.info("=" * 80)
    
    spark = get_spark_session("SpeedLayer-StreamingEnrichment")
    enricher = StreamingEnricher(spark)
    
    try:
        if mode in ["all", "enriched"]:
            enricher.create_enriched_stream()
        
        if mode in ["all", "industry"]:
            enricher.create_industry_aggregations()
        
        if mode in ["all", "tier"]:
            enricher.create_tier_metrics()
        
        active_queries = len([q for q in enricher.queries.values() if q])
        logger.info(f"Started {active_queries} streaming queries")
        logger.info("Speed layer enrichment is running. Press Ctrl+C to stop.")
        
        if duration_seconds:
            import time
            time.sleep(duration_seconds)
            enricher.stop_all()
        else:
            enricher.await_all()
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        enricher.stop_all()
    finally:
        spark.stop()
        logger.info("Speed layer enrichment stopped")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run Speed Layer streaming enrichment")
    parser.add_argument(
        "--mode",
        choices=["enriched", "industry", "tier", "all"],
        default="enriched",
        help="Which enrichment to run"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Duration in seconds (default: run indefinitely)"
    )
    
    args = parser.parse_args()
    run_streaming_enrichment(mode=args.mode, duration_seconds=args.duration)


