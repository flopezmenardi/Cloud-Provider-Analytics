"""
Speed Layer - Streaming Aggregations

Implements real-time usage metrics with sliding and tumbling windows.
Part of Lambda Architecture speed layer.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
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
    DATALAKE_ROOT
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Speed layer output paths
SPEED_ROOT = DATALAKE_ROOT / "speed"
SPEED_REALTIME_COSTS = SPEED_ROOT / "realtime_costs"
SPEED_REALTIME_ALERTS = SPEED_ROOT / "realtime_alerts"
SPEED_CHECKPOINT = SPEED_ROOT / "checkpoints"

# Ensure directories exist
SPEED_ROOT.mkdir(parents=True, exist_ok=True)
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


class StreamingAggregator:
    """
    Real-time streaming aggregator using Spark Structured Streaming.
    Implements windowed aggregations for usage metrics.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.queries = {}
    
    def _prepare_stream(self) -> DataFrame:
        """
        Prepare the streaming DataFrame with proper schema and timestamp parsing.
        """
        stream_df = self.spark.readStream \
            .option("maxFilesPerTrigger", 5) \
            .schema(USAGE_EVENT_SCHEMA) \
            .json(str(LANDING_USAGE_EVENTS_STREAM / "*.jsonl"))
        
        # Parse timestamp and add event time column for windowing
        stream_df = stream_df.withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        
        # Handle value column (can be string or number)
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
    
    def create_tumbling_window_costs(
        self,
        window_duration: str = "5 minutes",
        watermark_delay: str = "10 minutes"
    ) -> StreamingQuery:
        """
        Create tumbling window aggregation for costs by org and service.
        
        Tumbling windows are fixed-size, non-overlapping windows.
        Use case: Calculate costs every 5 minutes for each org/service.
        
        Args:
            window_duration: Window size (e.g., "5 minutes", "1 hour")
            watermark_delay: How late data can arrive and still be processed
        """
        logger.info(f"Starting tumbling window aggregation: {window_duration} window, {watermark_delay} watermark")
        
        stream_df = self._prepare_stream()
        
        # Apply watermark for handling late data
        stream_df = stream_df.withWatermark("event_time", watermark_delay)
        
        # Tumbling window aggregation
        windowed_df = stream_df.groupBy(
            F.window("event_time", window_duration),
            "org_id",
            "service"
        ).agg(
            F.sum("cost_usd_increment").alias("window_cost_usd"),
            F.count("*").alias("event_count"),
            F.sum(F.coalesce("genai_tokens", F.lit(0))).alias("genai_tokens"),
            F.avg("cost_usd_increment").alias("avg_cost_per_event")
        )
        
        # Flatten window struct
        windowed_df = windowed_df.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "org_id",
            "service",
            "window_cost_usd",
            "event_count",
            "genai_tokens",
            "avg_cost_per_event"
        )
        
        # Write to speed layer
        query = windowed_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", str(SPEED_REALTIME_COSTS / "tumbling")) \
            .option("checkpointLocation", str(SPEED_CHECKPOINT / "tumbling_costs")) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        self.queries["tumbling_costs"] = query
        logger.info(f"Tumbling window query started: {query.id}")
        
        return query
    
    def create_sliding_window_costs(
        self,
        window_duration: str = "15 minutes",
        slide_duration: str = "5 minutes",
        watermark_delay: str = "10 minutes"
    ) -> StreamingQuery:
        """
        Create sliding window aggregation for costs.
        
        Sliding windows overlap, providing smoother trends.
        Use case: 15-minute moving average, updated every 5 minutes.
        
        Args:
            window_duration: Window size
            slide_duration: How often to compute the window
            watermark_delay: Late data tolerance
        """
        logger.info(f"Starting sliding window aggregation: {window_duration} window, {slide_duration} slide")
        
        stream_df = self._prepare_stream()
        stream_df = stream_df.withWatermark("event_time", watermark_delay)
        
        # Sliding window aggregation
        windowed_df = stream_df.groupBy(
            F.window("event_time", window_duration, slide_duration),
            "org_id",
            "service",
            "region"
        ).agg(
            F.sum("cost_usd_increment").alias("window_cost_usd"),
            F.count("*").alias("event_count"),
            F.sum(F.coalesce("carbon_kg", F.lit(0.0))).alias("carbon_kg"),
            F.max("cost_usd_increment").alias("max_cost_event"),
            F.min("cost_usd_increment").alias("min_cost_event")
        )
        
        # Flatten window struct
        windowed_df = windowed_df.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "org_id",
            "service",
            "region",
            "window_cost_usd",
            "event_count",
            "carbon_kg",
            "max_cost_event",
            "min_cost_event"
        )
        
        # Write to speed layer
        query = windowed_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", str(SPEED_REALTIME_COSTS / "sliding")) \
            .option("checkpointLocation", str(SPEED_CHECKPOINT / "sliding_costs")) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        self.queries["sliding_costs"] = query
        logger.info(f"Sliding window query started: {query.id}")
        
        return query
    
    def create_cost_alerts(
        self,
        cost_threshold: float = 100.0,
        window_duration: str = "5 minutes",
        watermark_delay: str = "5 minutes"
    ) -> StreamingQuery:
        """
        Create real-time cost alert stream.
        
        Generates alerts when cost in a window exceeds threshold.
        Use case: Alert when 5-minute cost for any org/service exceeds $100.
        
        Args:
            cost_threshold: Alert threshold in USD
            window_duration: Window for aggregating costs
            watermark_delay: Late data tolerance
        """
        logger.info(f"Starting cost alert stream: threshold=${cost_threshold}")
        
        stream_df = self._prepare_stream()
        stream_df = stream_df.withWatermark("event_time", watermark_delay)
        
        # Window aggregation
        windowed_df = stream_df.groupBy(
            F.window("event_time", window_duration),
            "org_id",
            "service"
        ).agg(
            F.sum("cost_usd_increment").alias("window_cost_usd"),
            F.count("*").alias("event_count")
        )
        
        # Filter for alerts (cost exceeds threshold)
        alerts_df = windowed_df.filter(F.col("window_cost_usd") > cost_threshold)
        
        # Add alert metadata
        alerts_df = alerts_df.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "org_id",
            "service",
            "window_cost_usd",
            "event_count",
            F.lit(cost_threshold).alias("threshold"),
            F.current_timestamp().alias("alert_generated_at"),
            F.lit("HIGH_COST").alias("alert_type")
        )
        
        # Write alerts
        query = alerts_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", str(SPEED_REALTIME_ALERTS)) \
            .option("checkpointLocation", str(SPEED_CHECKPOINT / "alerts")) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        self.queries["cost_alerts"] = query
        logger.info(f"Cost alert query started: {query.id}")
        
        return query
    
    def create_hourly_summary(
        self,
        watermark_delay: str = "30 minutes"
    ) -> StreamingQuery:
        """
        Create hourly summary aggregation.
        
        Use case: Hourly usage and cost summaries for dashboard.
        """
        logger.info("Starting hourly summary aggregation")
        
        stream_df = self._prepare_stream()
        stream_df = stream_df.withWatermark("event_time", watermark_delay)
        
        # Hourly tumbling window
        windowed_df = stream_df.groupBy(
            F.window("event_time", "1 hour"),
            "org_id"
        ).agg(
            F.sum("cost_usd_increment").alias("hourly_cost_usd"),
            F.count("*").alias("total_events"),
            F.countDistinct("service").alias("services_used"),
            F.countDistinct("region").alias("regions_active"),
            F.sum(F.coalesce("genai_tokens", F.lit(0))).alias("genai_tokens"),
            F.sum(F.coalesce("carbon_kg", F.lit(0.0))).alias("carbon_kg")
        )
        
        # Flatten
        windowed_df = windowed_df.select(
            F.col("window.start").alias("hour_start"),
            F.col("window.end").alias("hour_end"),
            "org_id",
            "hourly_cost_usd",
            "total_events",
            "services_used",
            "regions_active",
            "genai_tokens",
            "carbon_kg"
        )
        
        # Write
        query = windowed_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", str(SPEED_REALTIME_COSTS / "hourly")) \
            .option("checkpointLocation", str(SPEED_CHECKPOINT / "hourly_summary")) \
            .trigger(processingTime="1 minute") \
            .start()
        
        self.queries["hourly_summary"] = query
        logger.info(f"Hourly summary query started: {query.id}")
        
        return query
    
    def stop_all(self):
        """Stop all streaming queries."""
        for name, query in self.queries.items():
            if query.isActive:
                logger.info(f"Stopping query: {name}")
                query.stop()
        logger.info("All streaming queries stopped")
    
    def await_all(self, timeout: Optional[int] = None):
        """Wait for all streaming queries to terminate."""
        for name, query in self.queries.items():
            logger.info(f"Awaiting termination: {name}")
            query.awaitTermination(timeout)


def run_streaming_aggregations(
    mode: str = "all",
    duration_seconds: Optional[int] = None
):
    """
    Run streaming aggregations.
    
    Args:
        mode: Which aggregations to run ("all", "tumbling", "sliding", "alerts", "hourly")
        duration_seconds: How long to run (None = indefinitely)
    """
    logger.info("=" * 80)
    logger.info("SPEED LAYER - STREAMING AGGREGATIONS")
    logger.info("=" * 80)
    
    spark = get_spark_session("SpeedLayer-StreamingAggregations")
    aggregator = StreamingAggregator(spark)
    
    try:
        if mode in ["all", "tumbling"]:
            aggregator.create_tumbling_window_costs()
        
        if mode in ["all", "sliding"]:
            aggregator.create_sliding_window_costs()
        
        if mode in ["all", "alerts"]:
            aggregator.create_cost_alerts(cost_threshold=50.0)
        
        if mode in ["all", "hourly"]:
            aggregator.create_hourly_summary()
        
        logger.info(f"Started {len(aggregator.queries)} streaming queries")
        logger.info("Speed layer is running. Press Ctrl+C to stop.")
        
        if duration_seconds:
            import time
            time.sleep(duration_seconds)
            aggregator.stop_all()
        else:
            aggregator.await_all()
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        aggregator.stop_all()
    finally:
        spark.stop()
        logger.info("Speed layer stopped")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run Speed Layer streaming aggregations")
    parser.add_argument(
        "--mode", 
        choices=["all", "tumbling", "sliding", "alerts", "hourly"],
        default="all",
        help="Which aggregations to run"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Duration in seconds (default: run indefinitely)"
    )
    
    args = parser.parse_args()
    run_streaming_aggregations(mode=args.mode, duration_seconds=args.duration)


