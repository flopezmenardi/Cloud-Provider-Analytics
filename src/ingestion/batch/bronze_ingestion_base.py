"""
Base class for bronze layer batch ingestion

Implements partitioned Parquet storage per project requirements.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pathlib import Path
from typing import Optional, List
import logging
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.paths import BRONZE_ROOT
from src.common.metadata import add_ingestion_metadata

logger = logging.getLogger(__name__)

# Partition configuration per table type
PARTITION_CONFIG = {
    "billing_monthly": ["month"],
    "support_tickets": ["created_year", "created_month"],
    "marketing_touches": ["touch_year", "touch_month"],
    "nps_surveys": ["survey_year"],
    "customers_orgs": None,  # Small dimension table, no partitioning needed
    "users": None,  # Small dimension table
    "resources": ["service"],  # Partition by service type
}


class BronzeIngestionBase:
    """Base class for bronze layer ingestion jobs with partitioning support"""
    
    def __init__(self, spark: SparkSession, source_file: Path, target_table: str):
        """
        Initialize bronze ingestion
        
        Args:
            spark: SparkSession
            source_file: Path to source CSV file
            target_table: Name of target table/directory in bronze layer
        """
        self.spark = spark
        self.source_file = Path(source_file)
        self.target_table = target_table
        self.target_path = BRONZE_ROOT / target_table
        self.partition_cols = PARTITION_CONFIG.get(target_table, None)
        
    def read_source(self) -> DataFrame:
        """
        Read source CSV file
        
        Returns:
            DataFrame with raw data
        """
        logger.info(f"Reading source file: {self.source_file}")
        
        if not self.source_file.exists():
            raise FileNotFoundError(f"Source file not found: {self.source_file}")
        
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(str(self.source_file))
        
        logger.info(f"Read {df.count()} rows from {self.source_file.name}")
        return df
    
    def add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ingestion metadata to DataFrame
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata columns
        """
        return add_ingestion_metadata(df, str(self.source_file))
    
    def add_partition_columns(self, df: DataFrame) -> DataFrame:
        """
        Add partition columns based on table configuration.
        Extracts year/month from date columns when needed.
        """
        if self.partition_cols is None:
            return df
        
        # Auto-generate partition columns from date fields
        if self.target_table == "billing_monthly":
            # month column already exists in format YYYY-MM
            pass
        elif self.target_table == "support_tickets":
            if "created_at" in df.columns:
                df = df.withColumn("created_year", F.year(F.col("created_at")))
                df = df.withColumn("created_month", F.month(F.col("created_at")))
        elif self.target_table == "marketing_touches":
            if "touch_date" in df.columns:
                df = df.withColumn("touch_year", F.year(F.col("touch_date")))
                df = df.withColumn("touch_month", F.month(F.col("touch_date")))
        elif self.target_table == "nps_surveys":
            if "survey_date" in df.columns:
                df = df.withColumn("survey_year", F.year(F.col("survey_date")))
        
        return df
    
    def write_bronze(self, df: DataFrame, mode: str = "overwrite") -> None:
        """
        Write DataFrame to bronze layer with optional partitioning.
        
        Args:
            df: DataFrame to write
            mode: Write mode (overwrite, append, etc.)
        """
        logger.info(f"Writing to bronze layer: {self.target_path}")
        
        # Add partition columns if configured
        df = self.add_partition_columns(df)
        
        # Build writer
        writer = df.write \
            .mode(mode) \
            .option("compression", "snappy")
        
        # Apply partitioning if configured
        if self.partition_cols:
            # Filter to only existing columns
            existing_partition_cols = [c for c in self.partition_cols if c in df.columns]
            if existing_partition_cols:
                writer = writer.partitionBy(*existing_partition_cols)
                logger.info(f"Partitioning by: {existing_partition_cols}")
        
        writer.parquet(str(self.target_path))
        logger.info(f"Successfully wrote to {self.target_path}")
    
    def ingest(self, mode: str = "overwrite") -> None:
        """
        Execute complete ingestion pipeline
        
        Args:
            mode: Write mode for bronze layer
        """
        logger.info(f"Starting ingestion for {self.target_table}")
        
        # Read source
        df = self.read_source()
        
        # Add metadata
        df_with_metadata = self.add_metadata(df)
        
        # Write to bronze (partitioning applied automatically)
        self.write_bronze(df_with_metadata, mode=mode)
        
        logger.info(f"Completed ingestion for {self.target_table}")

