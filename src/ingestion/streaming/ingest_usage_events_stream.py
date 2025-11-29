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
    mode: str = "append"
) -> StreamingQuery:
    """
    Ingest usage events from JSONL files using Spark Structured Streaming
    
    Args:
        spark: SparkSession
        mode: Output mode (append, complete, update)
        
    Returns:
        StreamingQuery object
    """
    logger.info(f"Starting streaming ingestion from {LANDING_USAGE_EVENTS_STREAM}")
    
    # Check if source directory exists
    if not LANDING_USAGE_EVENTS_STREAM.exists():
        raise FileNotFoundError(
            f"Source directory not found: {LANDING_USAGE_EVENTS_STREAM}"
        )
    
    # Read JSONL files as streaming source
    # Using file source with maxFilesPerTrigger for batch-like processing
    stream_df = spark.readStream \
        .option("maxFilesPerTrigger", 10) \
        .schema(USAGE_EVENT_SCHEMA) \
        .json(str(LANDING_USAGE_EVENTS_STREAM / "*.jsonl"))
    
    logger.info("Streaming source configured")
    
    # Normalize value column (handle string/number inconsistencies)
    stream_df = normalize_value_column(stream_df)
    
    # Add derived columns
    stream_df = add_derived_columns(stream_df)
    
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
    
    Args:
        spark: SparkSession
        mode: Write mode (overwrite, append)
    """
    logger.info(f"Starting batch ingestion from {LANDING_USAGE_EVENTS_STREAM}")
    
    if not LANDING_USAGE_EVENTS_STREAM.exists():
        raise FileNotFoundError(
            f"Source directory not found: {LANDING_USAGE_EVENTS_STREAM}"
        )
    
    # Read all JSONL files as batch
    df = spark.read \
        .schema(USAGE_EVENT_SCHEMA) \
        .json(str(LANDING_USAGE_EVENTS_STREAM / "*.jsonl"))
    
    logger.info(f"Read {df.count()} events from JSONL files")
    
    # Normalize value column
    df = normalize_value_column(df)
    
    # Add derived columns
    df = add_derived_columns(df)
    
    # Add ingestion metadata
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = df.withColumn("source_type", lit("batch"))
    df = df.withColumn("source_directory", lit(str(LANDING_USAGE_EVENTS_STREAM)))
    
    # Write to bronze layer
    df.write \
        .mode(mode) \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("event_year", "event_month", "event_day") \
        .save(str(BRONZE_USAGE_EVENTS))
    
    logger.info(f"Batch ingestion completed. Wrote to {BRONZE_USAGE_EVENTS}")


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

