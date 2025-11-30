"""
Bronze layer streaming ingestion for usage_events JSONL files
"""
import logging
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lit, current_timestamp, to_timestamp,
    when, isnan, isnull
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, LongType
)
from pyspark.sql.streaming import StreamingQuery

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session
from src.config.paths import (
    LANDING_USAGE_EVENTS_STREAM,
    BRONZE_USAGE_EVENTS,
    BRONZE_CHECKPOINT
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Schema for usage events (handles both schema_version 1 and 2)
USAGE_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("org_id", StringType(), True),
    StructField("resource_id", StringType(), True),
    StructField("service", StringType(), True),
    StructField("region", StringType(), True),
    StructField("metric", StringType(), True),
    StructField("value", StringType(), True),  # Can be string or number
    StructField("unit", StringType(), True),
    StructField("cost_usd_increment", DoubleType(), True),
    StructField("schema_version", IntegerType(), True),
    # Schema version 2 fields (optional)
    StructField("carbon_kg", DoubleType(), True),
    StructField("genai_tokens", LongType(), True),
])


def normalize_value_column(df):
    """
    Normalize the value column - handle string/number inconsistencies
    Convert string values to double where possible, keep nulls
    """
    from pyspark.sql.functions import when, col, isnan, isnull
    
    return df.withColumn(
        "value",
        when(
            col("value").isNull() | (col("value") == "null"),
            None
        ).otherwise(
            when(
                col("value").cast("double").isNotNull() & 
                ~isnan(col("value").cast("double")),
                col("value").cast("double")
            ).otherwise(None)
        )
    )


def add_derived_columns(df):
    """
    Add derived columns for partitioning and analysis
    """
    from pyspark.sql.functions import to_date, year, month, dayofmonth
    
    df_with_timestamp = df.withColumn(
        "event_timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    
    return df_with_timestamp \
        .withColumn("event_date", to_date(col("event_timestamp"))) \
        .withColumn("event_year", year(col("event_timestamp"))) \
        .withColumn("event_month", month(col("event_timestamp"))) \
        .withColumn("event_day", dayofmonth(col("event_timestamp")))


def ingest_usage_events_stream(
    spark: SparkSession,
    mode: str = "append",
    watermark_delay: str = "10 minutes"
) -> StreamingQuery:
    """
    Ingest usage events from JSONL files using Spark Structured Streaming
    
    Implements required features per project specification:
    - Explicit schema definition
    - Watermark for late data handling
    - Deduplication by event_id
    
    Args:
        spark: SparkSession
        mode: Output mode (append, complete, update)
        watermark_delay: How late data can arrive (default: 10 minutes)
        
    Returns:
        StreamingQuery object
    """
    logger.info(f"Starting streaming ingestion from {LANDING_USAGE_EVENTS_STREAM}")
    
    # Check if source directory exists
    if not LANDING_USAGE_EVENTS_STREAM.exists():
        raise FileNotFoundError(
            f"Source directory not found: {LANDING_USAGE_EVENTS_STREAM}"
        )
    
    # Read JSONL files as streaming source with explicit schema
    stream_df = spark.readStream \
        .option("maxFilesPerTrigger", 10) \
        .schema(USAGE_EVENT_SCHEMA) \
        .json(str(LANDING_USAGE_EVENTS_STREAM / "*.jsonl"))
    
    logger.info("Streaming source configured with explicit schema")
    
    # Normalize value column (handle string/number inconsistencies)
    stream_df = normalize_value_column(stream_df)
    
    # Add derived columns (includes event_timestamp for watermark)
    stream_df = add_derived_columns(stream_df)
    
    # Apply watermark for late data handling (required by project spec)
    stream_df = stream_df.withWatermark("event_timestamp", watermark_delay)
    logger.info(f"Watermark applied: {watermark_delay} delay for late data")
    
    # Deduplication by event_id within watermark window (required by project spec)
    stream_df = stream_df.dropDuplicates(["event_id"])
    logger.info("Deduplication by event_id enabled")
    
    # Add ingestion metadata
    stream_df = stream_df.withColumn("ingestion_timestamp", current_timestamp())
    stream_df = stream_df.withColumn("source_type", lit("stream"))
    stream_df = stream_df.withColumn("source_directory", lit(str(LANDING_USAGE_EVENTS_STREAM)))
    
    # Write to bronze layer
    query = stream_df.writeStream \
        .outputMode(mode) \
        .format("parquet") \
        .option("path", str(BRONZE_USAGE_EVENTS)) \
        .option("checkpointLocation", str(BRONZE_CHECKPOINT)) \
        .partitionBy("event_year", "event_month", "event_day") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    logger.info(f"Streaming query started. Checkpoint: {BRONZE_CHECKPOINT}")
    logger.info(f"Writing to: {BRONZE_USAGE_EVENTS}")
    
    return query


def ingest_usage_events_batch(
    spark: SparkSession,
    mode: str = "overwrite"
) -> None:
    """
    Ingest usage events from JSONL files as batch (for initial load)
    
    Implements deduplication by event_id to ensure idempotency.
    
    Args:
        spark: SparkSession
        mode: Write mode (overwrite, append)
    """
    logger.info(f"Starting batch ingestion from {LANDING_USAGE_EVENTS_STREAM}")
    
    if not LANDING_USAGE_EVENTS_STREAM.exists():
        raise FileNotFoundError(
            f"Source directory not found: {LANDING_USAGE_EVENTS_STREAM}"
        )
    
    # Read all JSONL files as batch with explicit schema
    # Use the directory path directly - Spark will read all JSONL files
    jsonl_files = str(LANDING_USAGE_EVENTS_STREAM) + "/*.jsonl"
    df = spark.read \
        .schema(USAGE_EVENT_SCHEMA) \
        .json(jsonl_files)
    
    initial_count = df.count()
    logger.info(f"Read {initial_count:,} events from JSONL files")
    
    # Deduplication by event_id (required by project spec)
    df = df.dropDuplicates(["event_id"])
    dedup_count = df.count()
    logger.info(f"After dedup by event_id: {dedup_count:,} events ({initial_count - dedup_count} duplicates removed)")
    
    # Normalize value column
    df = normalize_value_column(df)
    
    # Add derived columns
    df = add_derived_columns(df)
    
    # Add ingestion metadata
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = df.withColumn("source_type", lit("batch"))
    df = df.withColumn("source_directory", lit(str(LANDING_USAGE_EVENTS_STREAM)))
    
    # Write to bronze layer partitioned by date
    df.write \
        .mode(mode) \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("event_year", "event_month", "event_day") \
        .save(str(BRONZE_USAGE_EVENTS))
    
    logger.info(f"Batch ingestion completed. Wrote {dedup_count:,} events to {BRONZE_USAGE_EVENTS}")


if __name__ == "__main__":
    import sys
    
    spark = get_spark_session("IngestUsageEventsStream")
    
    try:
        # Check if running in batch or streaming mode
        if len(sys.argv) > 1 and sys.argv[1] == "batch":
            logger.info("Running in batch mode")
            ingest_usage_events_batch(spark)
            logger.info("Batch ingestion completed successfully")
        else:
            logger.info("Running in streaming mode")
            query = ingest_usage_events_stream(spark)
            logger.info("Streaming ingestion started. Waiting for termination...")
            query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Streaming ingestion stopped by user")
    except Exception as e:
        logger.error(f"Usage events ingestion failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

