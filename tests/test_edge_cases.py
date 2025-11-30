"""
Edge case tests for Cloud Provider Analytics pipeline.

Tests critical edge cases as required by the assignment:
- Empty files handling
- All-null records
- Extremely large values
- Negative costs
- Missing dimension keys
- Schema v1 only data
- Schema v2 only data
- Mixed v1/v2 data
"""

import pytest
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType, TimestampType
)

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestEmptyFileHandling:
    """Tests for handling empty files."""
    
    def test_empty_dataframe_aggregation(self, spark):
        """Test aggregation of empty DataFrame produces empty result."""
        schema = StructType([
            StructField("org_id", StringType()),
            StructField("cost", DoubleType()),
        ])
        
        empty_df = spark.createDataFrame([], schema)
        
        result = empty_df.groupBy("org_id").agg(
            F.sum("cost").alias("total_cost")
        )
        
        assert result.count() == 0
    
    def test_empty_dataframe_join(self, spark, sample_customers):
        """Test joining with empty DataFrame."""
        schema = StructType([
            StructField("org_id", StringType()),
            StructField("value", DoubleType()),
        ])
        
        empty_df = spark.createDataFrame([], schema)
        
        result = empty_df.join(sample_customers, on="org_id", how="left")
        
        assert result.count() == 0
    
    def test_empty_parquet_write_read(self, spark, temp_dir):
        """Test writing and reading empty parquet files."""
        schema = StructType([
            StructField("id", StringType()),
            StructField("value", DoubleType()),
        ])
        
        empty_df = spark.createDataFrame([], schema)
        path = str(temp_dir / "empty.parquet")
        
        empty_df.write.parquet(path)
        
        read_df = spark.read.parquet(path)
        assert read_df.count() == 0
        assert read_df.columns == ["id", "value"]


class TestAllNullRecords:
    """Tests for handling records with all null values."""
    
    def test_all_null_cost_aggregation(self, spark):
        """Test aggregation when all costs are null."""
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        
        schema = StructType([
            StructField("org_id", StringType()),
            StructField("cost", DoubleType()),
        ])
        data = [
            ("org-001", None),
            ("org-001", None),
            ("org-002", None),
        ]
        df = spark.createDataFrame(data, schema)
        
        result = df.groupBy("org_id").agg(
            F.sum("cost").alias("total_cost"),
            F.count("*").alias("record_count")
        )
        
        assert result.count() == 2
        
        for row in result.collect():
            assert row.total_cost is None
            assert row.record_count > 0
    
    def test_coalesce_null_handling(self, spark):
        """Test coalesce properly replaces nulls."""
        data = [
            ("org-001", None, None),
            ("org-002", 10.0, None),
            ("org-003", None, 100),
        ]
        df = spark.createDataFrame(data, ["org_id", "carbon_kg", "genai_tokens"])
        
        result = df.withColumn(
            "carbon_safe", F.coalesce(F.col("carbon_kg"), F.lit(0.0))
        ).withColumn(
            "tokens_safe", F.coalesce(F.col("genai_tokens"), F.lit(0))
        )
        
        for row in result.collect():
            assert row.carbon_safe is not None
            assert row.tokens_safe is not None
    
    def test_null_key_in_join(self, spark, sample_customers):
        """Test that null keys are handled in joins."""
        data = [
            ("org-001", 100.0),
            (None, 50.0),
            ("org-unknown", 75.0),
        ]
        df = spark.createDataFrame(data, ["org_id", "value"])
        
        result = df.join(sample_customers, on="org_id", how="left")
        
        assert result.count() == 3
        
        null_org = result.filter(F.col("org_id").isNull())
        assert null_org.count() == 1
        assert null_org.collect()[0].org_name is None


class TestExtremeLargeValues:
    """Tests for handling extremely large values."""
    
    def test_large_cost_values(self, spark):
        """Test handling of very large cost values."""
        data = [
            ("evt-001", 0.001),
            ("evt-002", 1000000.0),
            ("evt-003", 999999999.99),
        ]
        df = spark.createDataFrame(data, ["event_id", "cost"])
        
        total = df.agg(F.sum("cost")).collect()[0][0]
        
        assert total > 1000000000
    
    def test_large_token_counts(self, spark):
        """Test handling of large GenAI token counts."""
        data = [
            ("org-001", 1000000000),
            ("org-002", 9999999999),
        ]
        df = spark.createDataFrame(data, ["org_id", "genai_tokens"])
        
        total = df.agg(F.sum("genai_tokens")).collect()[0][0]
        
        assert total > 10000000000
    
    def test_anomaly_detection_with_outliers(self, spark):
        """Test that extreme values are detected as anomalies."""
        from src.common.data_quality import detect_outliers_zscore
        
        data = [(i, float(50 + (i % 5))) for i in range(1, 100)]
        data.append((100, 1000000.0))
        df = spark.createDataFrame(data, ["id", "value"])
        
        result = detect_outliers_zscore(df, "value", threshold=3.0)
        
        extreme = result.filter(F.col("id") == 100).collect()[0]
        assert extreme.is_anomaly_zscore == True


class TestNegativeCosts:
    """Tests for handling negative cost values (credits/refunds)."""
    
    def test_negative_cost_validation(self, spark):
        """Test validation of negative costs per project spec (>= -0.01)."""
        data = [
            ("evt-001", 10.0, True),
            ("evt-002", -0.005, True),
            ("evt-003", -0.01, True),
            ("evt-004", -0.02, False),
            ("evt-005", -100.0, False),
        ]
        df = spark.createDataFrame(data, ["event_id", "cost", "expected_valid"])
        
        result = df.withColumn(
            "is_valid_cost",
            F.col("cost") >= -0.01
        )
        
        for row in result.collect():
            assert row.is_valid_cost == row.expected_valid, f"Failed for {row.event_id}"
    
    def test_negative_cost_aggregation(self, spark):
        """Test that negative costs are properly included in aggregations."""
        data = [
            ("org-001", 100.0),
            ("org-001", -10.0),
            ("org-001", 50.0),
        ]
        df = spark.createDataFrame(data, ["org_id", "cost"])
        
        result = df.groupBy("org_id").agg(F.sum("cost").alias("net_cost"))
        
        assert result.collect()[0].net_cost == 140.0


class TestMissingDimensionKeys:
    """Tests for handling missing dimension keys."""
    
    def test_missing_org_id_in_usage(self, spark, sample_customers):
        """Test handling usage events with unknown org_id."""
        data = [
            ("org-001", 100.0),
            ("org-unknown", 50.0),
            ("org-does-not-exist", 75.0),
        ]
        df = spark.createDataFrame(data, ["org_id", "value"])
        
        result = df.join(sample_customers, on="org_id", how="left")
        
        assert result.count() == 3
        
        unknown = result.filter(F.col("org_name").isNull())
        assert unknown.count() == 2
    
    def test_orphaned_records_flagging(self, spark, sample_customers):
        """Test flagging of orphaned records (missing dimension)."""
        data = [
            ("org-001", 100.0),
            ("org-orphan", 50.0),
        ]
        df = spark.createDataFrame(data, ["org_id", "value"])
        
        result = df.join(
            sample_customers.select("org_id", "org_name"),
            on="org_id",
            how="left"
        ).withColumn(
            "is_orphaned",
            F.col("org_name").isNull()
        )
        
        orphaned = result.filter(F.col("is_orphaned") == True)
        assert orphaned.count() == 1
        assert orphaned.collect()[0].org_id == "org-orphan"


class TestSchemaVersionHandling:
    """Tests for schema version (v1/v2) handling."""
    
    def test_schema_v1_only_data(self, spark):
        """Test processing data with only schema v1 events."""
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("schema_version", IntegerType()),
            StructField("cost", DoubleType()),
            StructField("carbon_kg", DoubleType(), True),
            StructField("genai_tokens", LongType(), True),
        ])
        
        data = [
            ("evt-001", 1, 10.0, None, None),
            ("evt-002", 1, 20.0, None, None),
            ("evt-003", 1, 30.0, None, None),
        ]
        df = spark.createDataFrame(data, schema)
        
        result = df.withColumn(
            "carbon_kg_safe", F.coalesce(F.col("carbon_kg"), F.lit(0.0))
        ).withColumn(
            "genai_tokens_safe", F.coalesce(F.col("genai_tokens"), F.lit(0))
        )
        
        assert result.count() == 3
        
        totals = result.agg(
            F.sum("carbon_kg_safe"),
            F.sum("genai_tokens_safe")
        ).collect()[0]
        
        assert totals[0] == 0.0
        assert totals[1] == 0
    
    def test_schema_v2_only_data(self, spark):
        """Test processing data with only schema v2 events."""
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("schema_version", IntegerType()),
            StructField("cost", DoubleType()),
            StructField("carbon_kg", DoubleType(), True),
            StructField("genai_tokens", LongType(), True),
        ])
        
        data = [
            ("evt-001", 2, 10.0, 0.05, 1000),
            ("evt-002", 2, 20.0, 0.10, 2000),
            ("evt-003", 2, 30.0, 0.15, 3000),
        ]
        df = spark.createDataFrame(data, schema)
        
        totals = df.agg(
            F.sum("carbon_kg"),
            F.sum("genai_tokens")
        ).collect()[0]
        
        assert totals[0] == 0.30
        assert totals[1] == 6000
    
    def test_mixed_v1_v2_data(self, spark):
        """Test processing mixed schema v1 and v2 data."""
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("schema_version", IntegerType()),
            StructField("cost", DoubleType()),
            StructField("carbon_kg", DoubleType(), True),
            StructField("genai_tokens", LongType(), True),
        ])
        
        data = [
            ("evt-001", 1, 10.0, None, None),
            ("evt-002", 2, 20.0, 0.10, 2000),
            ("evt-003", 1, 30.0, None, None),
            ("evt-004", 2, 40.0, 0.20, 4000),
        ]
        df = spark.createDataFrame(data, schema)
        
        v1_count = df.filter(F.col("schema_version") == 1).count()
        v2_count = df.filter(F.col("schema_version") == 2).count()
        
        assert v1_count == 2
        assert v2_count == 2
        
        result = df.withColumn(
            "carbon_kg_safe", F.coalesce(F.col("carbon_kg"), F.lit(0.0))
        ).withColumn(
            "genai_tokens_safe", F.coalesce(F.col("genai_tokens"), F.lit(0))
        )
        
        totals = result.agg(
            F.sum("cost"),
            F.sum("carbon_kg_safe"),
            F.sum("genai_tokens_safe")
        ).collect()[0]
        
        assert totals[0] == 100.0
        assert abs(totals[1] - 0.30) < 0.001  # Floating point tolerance
        assert totals[2] == 6000
    
    def test_schema_version_distribution(self, spark, sample_usage_events):
        """Test that schema version distribution is captured."""
        version_counts = sample_usage_events.groupBy("schema_version").count()
        
        assert version_counts.count() >= 1
        
        for row in version_counts.collect():
            assert row.schema_version in [1, 2]
            assert row['count'] > 0


class TestCostRangeValidation:
    """Tests for cost range validation per project spec."""
    
    def test_cost_range_validation_rule(self, spark):
        """Test cost_usd_increment >= -0.01 rule."""
        from src.common.data_quality import validate_ranges
        
        data = [
            ("evt-001", 0.0),
            ("evt-002", -0.005),
            ("evt-003", -0.01),
            ("evt-004", -0.02),
            ("evt-005", 100.0),
        ]
        df = spark.createDataFrame(data, ["event_id", "cost"])
        
        result = validate_ranges(df, "cost", min_val=-0.01)
        
        assert result.count() == 4
        
        ids = [row.event_id for row in result.collect()]
        assert "evt-004" not in ids
    
    def test_cost_outlier_flagging(self, spark):
        """Test that cost outliers are flagged, not filtered."""
        from src.common.data_quality import validate_ranges
        
        data = [
            ("evt-001", 0.10),
            ("evt-002", 0.15),
            ("evt-003", 1000.0),
        ]
        df = spark.createDataFrame(data, ["event_id", "cost"])
        
        result = validate_ranges(df, "cost", min_val=-0.01, flag_col="is_valid_cost")
        
        assert result.count() == 3
        assert "is_valid_cost" in result.columns
        
        all_valid = result.filter(F.col("is_valid_cost") == True).count()
        assert all_valid == 3


class TestIdempotencyEdgeCases:
    """Tests for idempotency edge cases."""
    
    def test_duplicate_event_ids(self, spark):
        """Test deduplication of records with same event_id."""
        data = [
            ("evt-001", "2025-07-01T10:00:00Z", 10.0),
            ("evt-001", "2025-07-01T10:00:01Z", 15.0),
            ("evt-002", "2025-07-01T10:00:00Z", 20.0),
        ]
        df = spark.createDataFrame(data, ["event_id", "timestamp", "cost"])
        
        deduped = df.dropDuplicates(["event_id"])
        
        assert deduped.count() == 2
    
    def test_rerun_produces_same_result(self, spark, temp_dir):
        """Test that running transformation twice produces same result."""
        data = [
            ("evt-001", 10.0),
            ("evt-002", 20.0),
        ]
        df = spark.createDataFrame(data, ["event_id", "cost"])
        
        path = str(temp_dir / "idempotent_test")
        
        df.write.mode("overwrite").parquet(path)
        result1 = spark.read.parquet(path).count()
        
        df.write.mode("overwrite").parquet(path)
        result2 = spark.read.parquet(path).count()
        
        assert result1 == result2
        assert result1 == 2


class TestQuarantineEdgeCases:
    """Tests for quarantine functionality edge cases."""
    
    def test_all_valid_no_quarantine(self, spark, temp_dir):
        """Test that valid data produces empty quarantine."""
        from src.common.data_quality import quarantine_records
        
        data = [(1, 50), (2, 75), (3, 100)]
        df = spark.createDataFrame(data, ["id", "value"])
        
        valid_condition = F.col("value") >= 0
        quarantine_path = str(temp_dir / "quarantine")
        
        valid_df, quarantine_df = quarantine_records(
            df, valid_condition, quarantine_path, "test"
        )
        
        assert valid_df.count() == 3
        assert quarantine_df.count() == 0
    
    def test_all_invalid_to_quarantine(self, spark, temp_dir):
        """Test that all invalid data goes to quarantine."""
        from src.common.data_quality import quarantine_records
        
        data = [(1, -10), (2, -20), (3, -30)]
        df = spark.createDataFrame(data, ["id", "value"])
        
        valid_condition = F.col("value") >= 0
        quarantine_path = str(temp_dir / "quarantine")
        
        valid_df, quarantine_df = quarantine_records(
            df, valid_condition, quarantine_path, "test"
        )
        
        assert valid_df.count() == 0
        assert quarantine_df.count() == 3
