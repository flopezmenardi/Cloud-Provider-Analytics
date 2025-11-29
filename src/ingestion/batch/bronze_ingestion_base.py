"""
Base class for bronze layer batch ingestion
"""
from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
from typing import Optional
import logging
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.paths import BRONZE_ROOT
from src.common.metadata import add_ingestion_metadata

logger = logging.getLogger(__name__)


class BronzeIngestionBase:
    """Base class for bronze layer ingestion jobs"""
    
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
    
    def write_bronze(self, df: DataFrame, mode: str = "overwrite") -> None:
        """
        Write DataFrame to bronze layer
        
        Args:
            df: DataFrame to write
            mode: Write mode (overwrite, append, etc.)
        """
        logger.info(f"Writing to bronze layer: {self.target_path}")
        
        # Write as Parquet (can be changed to Delta Lake later)
        df.write \
            .mode(mode) \
            .option("compression", "snappy") \
            .parquet(str(self.target_path))
        
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
        
        # Write to bronze
        self.write_bronze(df_with_metadata, mode=mode)
        
        logger.info(f"Completed ingestion for {self.target_table}")

