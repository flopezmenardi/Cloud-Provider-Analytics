"""
Idempotency Utilities for Lambda Architecture

Ensures pipeline re-runs produce consistent results without duplicates.
Implements idempotent write patterns for batch and streaming workloads.
"""

import logging
from pathlib import Path
from datetime import datetime
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def deduplicate_by_key(
    df: DataFrame,
    key_columns: List[str],
    order_by: str = None,
    keep: str = "last"
) -> DataFrame:
    """
    Deduplicate DataFrame by key columns, keeping first or last occurrence.
    
    Args:
        df: Input DataFrame
        key_columns: Columns that form the unique key
        order_by: Column to order by for determining first/last
        keep: "first" or "last" occurrence to keep
    
    Returns:
        Deduplicated DataFrame
    """
    if order_by and order_by in df.columns:
        window = Window.partitionBy(*key_columns).orderBy(
            F.col(order_by).desc() if keep == "last" else F.col(order_by).asc()
        )
        
        df = df.withColumn("_row_num", F.row_number().over(window))
        df = df.filter(F.col("_row_num") == 1).drop("_row_num")
    else:
        df = df.dropDuplicates(key_columns)
    
    return df


def idempotent_write_parquet(
    df: DataFrame,
    path: str,
    partition_by: List[str] = None,
    mode: str = "overwrite"
) -> int:
    """
    Write DataFrame to Parquet with idempotent semantics.
    
    For batch pipelines, uses overwrite mode to ensure re-runs
    produce the same result.
    
    Args:
        df: DataFrame to write
        path: Output path
        partition_by: Columns to partition by
        mode: Write mode (overwrite for idempotency)
    
    Returns:
        Number of records written
    """
    count = df.count()
    
    writer = df.write.mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    writer.parquet(path)
    
    logger.info(f"[IDEMPOTENT] Wrote {count:,} records to {path} (mode={mode})")
    
    return count


def merge_incremental(
    spark: SparkSession,
    new_df: DataFrame,
    target_path: str,
    key_columns: List[str],
    timestamp_col: str = None
) -> int:
    """
    Merge new data with existing data, handling upserts.
    
    For incremental loads, combines new data with existing,
    deduplicating by key and keeping the latest version.
    
    Args:
        spark: SparkSession
        new_df: New data to merge
        target_path: Path to existing data
        key_columns: Columns that form the unique key
        timestamp_col: Column to determine latest version
    
    Returns:
        Total record count after merge
    """
    target = Path(target_path)
    
    if target.exists():
        existing_df = spark.read.parquet(target_path)
        
        combined_df = existing_df.unionByName(new_df, allowMissingColumns=True)
        
        if timestamp_col:
            combined_df = deduplicate_by_key(
                combined_df, 
                key_columns, 
                order_by=timestamp_col, 
                keep="last"
            )
        else:
            combined_df = combined_df.dropDuplicates(key_columns)
        
        count = idempotent_write_parquet(combined_df, target_path)
        logger.info(f"[IDEMPOTENT] Merged {new_df.count():,} new records with existing data")
    else:
        count = idempotent_write_parquet(new_df, target_path)
        logger.info(f"[IDEMPOTENT] Created new target with {count:,} records")
    
    return count


def streaming_dedup_window(
    df: DataFrame,
    event_id_col: str,
    event_time_col: str,
    watermark_delay: str = "10 minutes"
) -> DataFrame:
    """
    Apply deduplication for streaming with watermark.
    
    Per project requirements:
    - dropDuplicates by event_id
    - withWatermark on event_time
    
    Args:
        df: Streaming DataFrame
        event_id_col: Column containing unique event ID
        event_time_col: Column containing event timestamp
        watermark_delay: Watermark delay for late data
    
    Returns:
        Deduplicated streaming DataFrame
    """
    return df.withWatermark(event_time_col, watermark_delay) \
             .dropDuplicates([event_id_col])


def ensure_exactly_once(
    df: DataFrame,
    checkpoint_path: str,
    output_path: str,
    trigger_interval: str = "1 minute"
):
    """
    Configure streaming query for exactly-once semantics.
    
    Uses checkpointing to ensure fault-tolerant, exactly-once processing.
    
    Args:
        df: Streaming DataFrame
        checkpoint_path: Path for checkpoint data
        output_path: Output path for results
        trigger_interval: Processing trigger interval
    
    Returns:
        StreamingQuery
    """
    return df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime=trigger_interval) \
        .start()


def validate_idempotency(
    spark: SparkSession,
    path: str,
    key_columns: List[str]
) -> dict:
    """
    Validate that a dataset has no duplicates by key.
    
    Args:
        spark: SparkSession
        path: Path to data
        key_columns: Expected unique key columns
    
    Returns:
        Validation result with duplicate count
    """
    df = spark.read.parquet(path)
    total_count = df.count()
    distinct_count = df.select(*key_columns).distinct().count()
    
    duplicate_count = total_count - distinct_count
    
    result = {
        "path": path,
        "total_records": total_count,
        "distinct_keys": distinct_count,
        "duplicate_count": duplicate_count,
        "is_idempotent": duplicate_count == 0
    }
    
    if duplicate_count > 0:
        logger.warning(f"[IDEMPOTENT] Found {duplicate_count} duplicates in {path}")
    else:
        logger.info(f"[IDEMPOTENT] Validated: {path} has no duplicates")
    
    return result


class BatchIdempotencyManager:
    """
    Manages idempotency for batch pipeline runs.
    
    Tracks run state and ensures clean re-runs.
    """
    
    def __init__(self, run_id: str, state_path: str = "datalake/state"):
        self.run_id = run_id
        self.state_path = Path(state_path)
        self.state_path.mkdir(parents=True, exist_ok=True)
        self.run_state_file = self.state_path / f"{run_id}_state.json"
    
    def mark_stage_complete(self, stage: str, record_count: int):
        """Mark a pipeline stage as complete."""
        import json
        
        state = self._load_state()
        state["stages"] = state.get("stages", {})
        state["stages"][stage] = {
            "status": "complete",
            "record_count": record_count,
            "completed_at": datetime.now().isoformat()
        }
        self._save_state(state)
        logger.info(f"[IDEMPOTENT] Stage {stage} marked complete ({record_count:,} records)")
    
    def is_stage_complete(self, stage: str) -> bool:
        """Check if a stage was already completed in this run."""
        state = self._load_state()
        stages = state.get("stages", {})
        return stages.get(stage, {}).get("status") == "complete"
    
    def _load_state(self) -> dict:
        import json
        if self.run_state_file.exists():
            with open(self.run_state_file) as f:
                return json.load(f)
        return {"run_id": self.run_id, "started_at": datetime.now().isoformat()}
    
    def _save_state(self, state: dict):
        import json
        with open(self.run_state_file, 'w') as f:
            json.dump(state, f, indent=2)
