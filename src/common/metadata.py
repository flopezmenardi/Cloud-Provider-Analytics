"""
Utilities for adding metadata to ingested data
"""
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp
from pathlib import Path


def add_ingestion_metadata(df: DataFrame, source_file: str) -> DataFrame:
    """
    Add ingestion metadata columns to a DataFrame
    
    Args:
        df: Input DataFrame
        source_file: Path to the source file
        
    Returns:
        DataFrame with added metadata columns
    """
    source_path = Path(source_file)
    
    return df.withColumn("ingestion_timestamp", current_timestamp()) \
             .withColumn("source_file", lit(source_path.name)) \
             .withColumn("source_path", lit(str(source_path))) \
             .withColumn("ingestion_date", lit(datetime.now().strftime("%Y-%m-%d")))

