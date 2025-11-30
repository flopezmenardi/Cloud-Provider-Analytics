"""
Pytest fixtures for Cloud Provider Analytics tests.

Provides shared fixtures for Spark sessions, sample data, and test utilities.
"""

import pytest
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import tempfile
import shutil

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    Uses local mode with minimal resources.
    """
    from pyspark.sql import SparkSession
    
    # Set Java options for compatibility
    java_opts = (
        "-Dio.netty.tryReflectionSetAccessible=true "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
    )
    
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("CloudProviderAnalytics-Tests") \
        .config("spark.driver.extraJavaOptions", java_opts) \
        .config("spark.executor.extraJavaOptions", java_opts) \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="function")
def temp_dir():
    """Create a temporary directory for test outputs."""
    temp_path = tempfile.mkdtemp(prefix="test_cloud_analytics_")
    yield Path(temp_path)
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def sample_usage_events(spark):
    """
    Create sample usage events DataFrame for testing.
    Includes both schema v1 and v2 events.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType,
        IntegerType, LongType, TimestampType
    )
    from pyspark.sql import functions as F
    
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("org_id", StringType(), False),
        StructField("resource_id", StringType(), True),
        StructField("service", StringType(), False),
        StructField("region", StringType(), False),
        StructField("metric", StringType(), False),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("cost_usd_increment", DoubleType(), True),
        StructField("schema_version", IntegerType(), False),
        StructField("carbon_kg", DoubleType(), True),
        StructField("genai_tokens", LongType(), True),
    ])
    
    # Sample data with variety
    data = [
        # Schema v1 events (no carbon_kg or genai_tokens)
        ("evt-001", "2025-07-01T10:00:00Z", "org-001", "res-001", "compute", "us-east-1",
         "cpu_utilization", 75.5, "percent", 0.15, 1, None, None),
        ("evt-002", "2025-07-01T10:05:00Z", "org-001", "res-002", "storage", "us-east-1",
         "storage", 100.0, "GB", 0.02, 1, None, None),
        ("evt-003", "2025-07-01T10:10:00Z", "org-002", "res-003", "compute", "eu-west-1",
         "requests", 1000.0, "count", 0.50, 1, None, None),
        
        # Schema v2 events (with carbon_kg and genai_tokens)
        ("evt-004", "2025-08-01T10:00:00Z", "org-001", "res-001", "ai_ml", "us-east-1",
         "inference", 500.0, "requests", 2.50, 2, 0.05, 10000),
        ("evt-005", "2025-08-01T10:05:00Z", "org-002", "res-004", "ai_ml", "eu-west-1",
         "inference", 200.0, "requests", 1.00, 2, 0.02, 5000),
        
        # Edge cases
        ("evt-006", "2025-07-15T12:00:00Z", "org-001", None, "compute", "us-east-1",
         "cpu_utilization", None, "percent", 0.0, 1, None, None),  # Null value
        ("evt-007", "2025-08-15T12:00:00Z", "org-003", "res-005", "compute", "ap-south-1",
         "cpu_utilization", 150.0, "percent", 100.0, 2, 0.1, 0),  # High cost
    ]
    
    df = spark.createDataFrame(data, schema)
    df = df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    
    return df


@pytest.fixture
def sample_customers(spark):
    """Create sample customers DataFrame for testing."""
    from pyspark.sql.types import (
        StructType, StructField, StringType
    )
    
    schema = StructType([
        StructField("org_id", StringType(), False),
        StructField("org_name", StringType(), False),
        StructField("industry", StringType(), True),
        StructField("tier", StringType(), True),
        StructField("country", StringType(), True),
    ])
    
    data = [
        ("org-001", "Acme Corp", "technology", "enterprise", "USA"),
        ("org-002", "Global Inc", "finance", "pro", "UK"),
        ("org-003", "StartupXYZ", "technology", "free", "Germany"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_billing(spark):
    """Create sample billing DataFrame for testing."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType
    )
    
    schema = StructType([
        StructField("invoice_id", StringType(), False),
        StructField("org_id", StringType(), False),
        StructField("billing_month", StringType(), False),
        StructField("total_amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("exchange_rate_to_usd", DoubleType(), True),
        StructField("credits_applied", DoubleType(), True),
        StructField("taxes", DoubleType(), True),
    ])
    
    data = [
        ("inv-001", "org-001", "2025-07", 1500.0, "USD", 1.0, 100.0, 150.0),
        ("inv-002", "org-001", "2025-08", 1800.0, "USD", 1.0, 50.0, 180.0),
        ("inv-003", "org-002", "2025-07", 800.0, "GBP", 1.25, 0.0, 80.0),
        ("inv-004", "org-002", "2025-08", 900.0, "GBP", 1.24, 25.0, 90.0),
        ("inv-005", "org-003", "2025-07", 0.0, "USD", 1.0, 0.0, 0.0),  # Free tier
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_support_tickets(spark):
    """Create sample support tickets DataFrame for testing."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType
    )
    from pyspark.sql import functions as F
    
    schema = StructType([
        StructField("ticket_id", StringType(), False),
        StructField("org_id", StringType(), False),
        StructField("created_at", StringType(), False),
        StructField("resolved_at", StringType(), True),
        StructField("severity", StringType(), False),
        StructField("status", StringType(), False),
        StructField("category", StringType(), True),
    ])
    
    data = [
        ("tkt-001", "org-001", "2025-07-01T10:00:00Z", "2025-07-01T12:00:00Z", "critical", "resolved", "billing"),
        ("tkt-002", "org-001", "2025-07-02T09:00:00Z", "2025-07-02T18:00:00Z", "high", "resolved", "technical"),
        ("tkt-003", "org-002", "2025-07-03T14:00:00Z", None, "medium", "open", "general"),
        ("tkt-004", "org-003", "2025-07-04T08:00:00Z", "2025-07-05T08:00:00Z", "low", "resolved", "billing"),
        ("tkt-005", "org-001", "2025-07-05T16:00:00Z", "2025-07-05T17:00:00Z", "critical", "resolved", "technical"),
    ]
    
    df = spark.createDataFrame(data, schema)
    df = df.withColumn(
        "created_timestamp",
        F.to_timestamp(F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    df = df.withColumn(
        "resolved_timestamp",
        F.to_timestamp(F.col("resolved_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    
    return df


@pytest.fixture
def sample_nps_surveys(spark):
    """Create sample NPS surveys DataFrame for testing."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType
    )
    
    schema = StructType([
        StructField("survey_id", StringType(), False),
        StructField("org_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("nps_score", IntegerType(), True),
        StructField("survey_date", StringType(), False),
        StructField("feedback", StringType(), True),
    ])
    
    data = [
        ("srv-001", "org-001", "usr-001", 9, "2025-07-15", "Great service!"),
        ("srv-002", "org-001", "usr-002", 8, "2025-07-16", None),
        ("srv-003", "org-002", "usr-003", 6, "2025-07-17", "Could be better"),
        ("srv-004", "org-003", "usr-004", 3, "2025-07-18", "Poor experience"),
        ("srv-005", "org-002", "usr-005", 10, "2025-07-19", "Excellent!"),
        # Invalid scores for testing validation
        ("srv-006", "org-001", "usr-006", -1, "2025-07-20", "Invalid"),
        ("srv-007", "org-002", "usr-007", 11, "2025-07-21", "Also invalid"),
        ("srv-008", "org-003", "usr-008", None, "2025-07-22", "Missing score"),
    ]
    
    return spark.createDataFrame(data, schema)


def assert_dataframe_equal(df1, df2, check_order=False):
    """
    Assert that two DataFrames are equal.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        check_order: Whether to check row order
    """
    # Check schemas match
    assert df1.schema == df2.schema, f"Schemas differ: {df1.schema} vs {df2.schema}"
    
    # Check counts match
    count1, count2 = df1.count(), df2.count()
    assert count1 == count2, f"Row counts differ: {count1} vs {count2}"
    
    if check_order:
        # Compare row by row
        rows1 = df1.collect()
        rows2 = df2.collect()
        for i, (r1, r2) in enumerate(zip(rows1, rows2)):
            assert r1 == r2, f"Row {i} differs: {r1} vs {r2}"
    else:
        # Compare as sets (order-independent)
        rows1 = set(df1.collect())
        rows2 = set(df2.collect())
        assert rows1 == rows2, f"Row contents differ"


