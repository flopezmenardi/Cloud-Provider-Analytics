"""
Tests for lineage tracking and idempotency utilities.

Tests:
- Data lineage recording
- Pipeline evidence collection
- Deduplication logic
- Idempotent write patterns
"""

import pytest
import sys
import json
from pathlib import Path
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestLineageTracking:
    """Tests for data lineage tracking."""
    
    def test_add_lineage_columns(self, spark):
        """Test adding lineage columns to DataFrame."""
        from src.common.lineage import add_lineage_columns
        
        data = [("evt-001", 10.0), ("evt-002", 20.0)]
        df = spark.createDataFrame(data, ["event_id", "cost"])
        
        result = add_lineage_columns(df, "bronze", "silver_transform")
        
        assert "_lineage_source" in result.columns
        assert "_lineage_transformation" in result.columns
        assert "_lineage_timestamp" in result.columns
        
        row = result.collect()[0]
        assert row._lineage_source == "bronze"
        assert row._lineage_transformation == "silver_transform"
    
    def test_transformation_metrics(self, spark):
        """Test calculating transformation metrics."""
        from src.common.lineage import calculate_metrics
        
        input_data = [
            ("evt-001", 1, 10.0),
            ("evt-002", 1, 20.0),
            ("evt-003", 2, 30.0),
        ]
        input_df = spark.createDataFrame(input_data, ["event_id", "schema_version", "cost"])
        
        output_df = input_df.filter(F.col("cost") > 15)
        output_df = output_df.withColumn("is_anomaly", F.col("cost") > 25)
        
        quarantine_df = input_df.filter(F.col("cost") <= 15)
        
        metrics = calculate_metrics(input_df, output_df, quarantine_df)
        
        assert metrics.input_count == 3
        assert metrics.output_count == 2
        assert metrics.quarantine_count == 1
        assert metrics.schema_v1_count == 1
        assert metrics.schema_v2_count == 1
    
    def test_pipeline_evidence_collection(self, temp_dir):
        """Test pipeline evidence collection and saving."""
        from src.common.lineage import (
            PipelineEvidence, TransformationMetrics, LineageRecord
        )
        
        evidence = PipelineEvidence(run_id="test_run")
        evidence.evidence_path = temp_dir
        
        record = evidence.start_stage(
            stage="bronze_ingestion",
            source_layer="landing",
            target_layer="bronze",
            source_path="/landing/data.csv",
            target_path="/bronze/data",
            transformation_name="ingest_csv"
        )
        
        metrics = TransformationMetrics(
            input_count=1000,
            output_count=1000,
            quarantine_count=0
        )
        
        evidence.complete_stage(record, metrics)
        
        assert evidence.stage_metrics["bronze_ingestion"]["input_count"] == 1000
        assert evidence.stage_metrics["bronze_ingestion"]["output_count"] == 1000
    
    def test_data_quality_evidence(self, temp_dir):
        """Test data quality evidence collection."""
        from src.common.lineage import PipelineEvidence
        
        evidence = PipelineEvidence(run_id="dq_test")
        evidence.evidence_path = temp_dir
        
        evidence.add_data_quality_evidence(
            layer="silver",
            table="usage_events",
            valid_count=950,
            quarantine_count=50,
            anomaly_count=25,
            validation_rules=["not_null", "range_check", "zscore"]
        )
        
        key = "silver.usage_events"
        assert key in evidence.data_quality_summary
        assert evidence.data_quality_summary[key]["valid_records"] == 950
        assert evidence.data_quality_summary[key]["quarantine_records"] == 50
        assert evidence.data_quality_summary[key]["quality_rate"] == 95.0


class TestIdempotency:
    """Tests for idempotency utilities."""
    
    def test_deduplicate_by_key_keep_last(self, spark):
        """Test deduplication keeping last occurrence."""
        from src.common.idempotency import deduplicate_by_key
        
        data = [
            ("evt-001", "2025-07-01T10:00:00Z", 10.0),
            ("evt-001", "2025-07-01T10:00:01Z", 15.0),
            ("evt-002", "2025-07-01T10:00:00Z", 20.0),
        ]
        df = spark.createDataFrame(data, ["event_id", "timestamp", "cost"])
        
        result = deduplicate_by_key(df, ["event_id"], order_by="timestamp", keep="last")
        
        assert result.count() == 2
        
        evt1 = result.filter(F.col("event_id") == "evt-001").collect()[0]
        assert evt1.cost == 15.0
    
    def test_deduplicate_by_key_keep_first(self, spark):
        """Test deduplication keeping first occurrence."""
        from src.common.idempotency import deduplicate_by_key
        
        data = [
            ("evt-001", "2025-07-01T10:00:00Z", 10.0),
            ("evt-001", "2025-07-01T10:00:01Z", 15.0),
        ]
        df = spark.createDataFrame(data, ["event_id", "timestamp", "cost"])
        
        result = deduplicate_by_key(df, ["event_id"], order_by="timestamp", keep="first")
        
        assert result.count() == 1
        assert result.collect()[0].cost == 10.0
    
    def test_idempotent_write_overwrite(self, spark, temp_dir):
        """Test idempotent write with overwrite mode."""
        from src.common.idempotency import idempotent_write_parquet
        
        data1 = [("a", 1), ("b", 2)]
        df1 = spark.createDataFrame(data1, ["id", "value"])
        path = str(temp_dir / "idempotent_test")
        
        count1 = idempotent_write_parquet(df1, path)
        
        data2 = [("c", 3), ("d", 4), ("e", 5)]
        df2 = spark.createDataFrame(data2, ["id", "value"])
        count2 = idempotent_write_parquet(df2, path)
        
        final_df = spark.read.parquet(path)
        assert final_df.count() == 3
        
        ids = [row.id for row in final_df.collect()]
        assert "a" not in ids
        assert "c" in ids
    
    def test_merge_incremental(self, spark, temp_dir):
        """Test incremental merge with deduplication."""
        from src.common.idempotency import merge_incremental
        
        data1 = [("a", "2025-07-01", 10), ("b", "2025-07-01", 20)]
        df1 = spark.createDataFrame(data1, ["id", "timestamp", "value"])
        path = str(temp_dir / "merge_test")
        df1.write.parquet(path)
        
        data2 = [("a", "2025-07-02", 15), ("c", "2025-07-01", 30)]
        df2 = spark.createDataFrame(data2, ["id", "timestamp", "value"])
        
        count = merge_incremental(spark, df2, path, ["id"], timestamp_col="timestamp")
        
        assert count == 3
        
        result = spark.read.parquet(path)
        assert result.count() == 3
        
        a_row = result.filter(F.col("id") == "a").collect()[0]
        assert a_row.value == 15
    
    def test_validate_idempotency_no_duplicates(self, spark, temp_dir):
        """Test idempotency validation with no duplicates."""
        from src.common.idempotency import validate_idempotency
        
        data = [("a", 1), ("b", 2), ("c", 3)]
        df = spark.createDataFrame(data, ["id", "value"])
        path = str(temp_dir / "validate_test")
        df.write.parquet(path)
        
        result = validate_idempotency(spark, path, ["id"])
        
        assert result["is_idempotent"] == True
        assert result["duplicate_count"] == 0
    
    def test_validate_idempotency_with_duplicates(self, spark, temp_dir):
        """Test idempotency validation with duplicates."""
        from src.common.idempotency import validate_idempotency
        
        data = [("a", 1), ("a", 2), ("b", 3)]
        df = spark.createDataFrame(data, ["id", "value"])
        path = str(temp_dir / "validate_test_dup")
        df.write.parquet(path)
        
        result = validate_idempotency(spark, path, ["id"])
        
        assert result["is_idempotent"] == False
        assert result["duplicate_count"] == 1


class TestBatchIdempotencyManager:
    """Tests for batch idempotency manager."""
    
    def test_stage_tracking(self, temp_dir):
        """Test stage completion tracking."""
        from src.common.idempotency import BatchIdempotencyManager
        
        manager = BatchIdempotencyManager(
            run_id="test_batch",
            state_path=str(temp_dir)
        )
        
        assert manager.is_stage_complete("bronze") == False
        
        manager.mark_stage_complete("bronze", record_count=1000)
        
        assert manager.is_stage_complete("bronze") == True
        
        manager2 = BatchIdempotencyManager(
            run_id="test_batch",
            state_path=str(temp_dir)
        )
        assert manager2.is_stage_complete("bronze") == True
    
    def test_multiple_stages(self, temp_dir):
        """Test tracking multiple stages."""
        from src.common.idempotency import BatchIdempotencyManager
        
        manager = BatchIdempotencyManager(
            run_id="multi_stage",
            state_path=str(temp_dir)
        )
        
        manager.mark_stage_complete("bronze", 1000)
        manager.mark_stage_complete("silver", 950)
        manager.mark_stage_complete("gold", 100)
        
        assert manager.is_stage_complete("bronze") == True
        assert manager.is_stage_complete("silver") == True
        assert manager.is_stage_complete("gold") == True
        assert manager.is_stage_complete("serving") == False


class TestStreamingIdempotency:
    """Tests for streaming idempotency patterns."""
    
    def test_streaming_dedup_window(self, spark):
        """Test streaming deduplication setup."""
        from src.common.idempotency import streaming_dedup_window
        
        data = [
            ("evt-001", "2025-07-01T10:00:00", 10.0),
            ("evt-002", "2025-07-01T10:00:01", 20.0),
        ]
        df = spark.createDataFrame(data, ["event_id", "timestamp", "cost"])
        df = df.withColumn("event_time", F.to_timestamp("timestamp"))
        
        pass
