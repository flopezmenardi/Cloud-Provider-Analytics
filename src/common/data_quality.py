"""
Data Quality Framework
Provides utilities for data validation, outlier detection, and quarantine management.
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)


def validate_ranges(df: DataFrame, col: str, min_val: float = None, max_val: float = None,
                   flag_col: str = None) -> DataFrame:
    """
    Validate that a column's values are within a specified range.

    Args:
        df: Input DataFrame
        col: Column name to validate
        min_val: Minimum valid value (inclusive). None means no lower bound.
        max_val: Maximum valid value (inclusive). None means no upper bound.
        flag_col: Optional column name to store validation flag. If None, returns filtered DF.

    Returns:
        DataFrame with validation results
    """
    condition = F.col(col).isNotNull()

    if min_val is not None:
        condition = condition & (F.col(col) >= min_val)

    if max_val is not None:
        condition = condition & (F.col(col) <= max_val)

    if flag_col:
        return df.withColumn(flag_col, condition)
    else:
        return df.filter(condition)


def detect_outliers_zscore(df: DataFrame, col: str, threshold: float = 3.0,
                          flag_col: str = "is_anomaly_zscore") -> DataFrame:
    """
    Detect outliers using Z-score method.

    Args:
        df: Input DataFrame
        col: Column name to analyze
        threshold: Z-score threshold (default 3.0)
        flag_col: Column name for anomaly flag

    Returns:
        DataFrame with anomaly flag column added
    """
    # Calculate mean and stddev
    stats = df.select(
        F.mean(col).alias("mean"),
        F.stddev(col).alias("stddev")
    ).first()

    mean_val = stats["mean"] if stats["mean"] is not None else 0
    stddev_val = stats["stddev"] if stats["stddev"] is not None else 1

    # Avoid division by zero
    if stddev_val == 0:
        stddev_val = 1

    # Calculate Z-score and flag outliers
    df = df.withColumn(
        "z_score",
        F.abs((F.col(col) - F.lit(mean_val)) / F.lit(stddev_val))
    )

    df = df.withColumn(
        flag_col,
        F.when(F.col("z_score") > threshold, True).otherwise(False)
    )

    return df.drop("z_score")


def detect_outliers_mad(df: DataFrame, col: str, threshold: float = 3.0,
                       flag_col: str = "is_anomaly_mad") -> DataFrame:
    """
    Detect outliers using Median Absolute Deviation (MAD) method.
    More robust to extreme values than Z-score.

    Args:
        df: Input DataFrame
        col: Column name to analyze
        threshold: MAD threshold (default 3.0)
        flag_col: Column name for anomaly flag

    Returns:
        DataFrame with anomaly flag column added
    """
    # Calculate median
    median_val = df.approxQuantile(col, [0.5], 0.01)[0]

    # Calculate absolute deviations from median
    df_with_dev = df.withColumn(
        "abs_deviation",
        F.abs(F.col(col) - F.lit(median_val))
    )

    # Calculate MAD (median of absolute deviations)
    mad_val = df_with_dev.approxQuantile("abs_deviation", [0.5], 0.01)[0]

    # Avoid division by zero
    if mad_val == 0:
        mad_val = 1e-6

    # Modified Z-score = 0.6745 * (x - median) / MAD
    # Flag if modified Z-score > threshold
    df = df_with_dev.withColumn(
        "mad_score",
        F.lit(0.6745) * F.col("abs_deviation") / F.lit(mad_val)
    )

    df = df.withColumn(
        flag_col,
        F.when(F.col("mad_score") > threshold, True).otherwise(False)
    )

    return df.drop("abs_deviation", "mad_score")


def detect_outliers_percentile(df: DataFrame, col: str,
                               lower: float = 0.01, upper: float = 0.99,
                               flag_col: str = "is_anomaly_percentile") -> DataFrame:
    """
    Detect outliers using percentile method.
    Values below lower percentile or above upper percentile are flagged.

    Args:
        df: Input DataFrame
        col: Column name to analyze
        lower: Lower percentile threshold (default 0.01 = 1st percentile)
        upper: Upper percentile threshold (default 0.99 = 99th percentile)
        flag_col: Column name for anomaly flag

    Returns:
        DataFrame with anomaly flag column added
    """
    # Calculate percentiles
    percentiles = df.approxQuantile(col, [lower, upper], 0.01)
    lower_bound = percentiles[0]
    upper_bound = percentiles[1]

    # Flag outliers
    df = df.withColumn(
        flag_col,
        F.when(
            (F.col(col) < lower_bound) | (F.col(col) > upper_bound),
            True
        ).otherwise(False)
    )

    return df


def add_anomaly_flags(df: DataFrame, col: str,
                     methods: List[str] = ['zscore', 'mad', 'percentile'],
                     zscore_threshold: float = 3.0,
                     mad_threshold: float = 3.0,
                     percentile_lower: float = 0.01,
                     percentile_upper: float = 0.99) -> DataFrame:
    """
    Add multiple anomaly detection flags to DataFrame.

    Args:
        df: Input DataFrame
        col: Column name to analyze
        methods: List of methods to apply ('zscore', 'mad', 'percentile')
        zscore_threshold: Threshold for Z-score method
        mad_threshold: Threshold for MAD method
        percentile_lower: Lower percentile for percentile method
        percentile_upper: Upper percentile for percentile method

    Returns:
        DataFrame with anomaly flag columns added
    """
    result_df = df

    if 'zscore' in methods:
        result_df = detect_outliers_zscore(
            result_df, col, threshold=zscore_threshold
        )

    if 'mad' in methods:
        result_df = detect_outliers_mad(
            result_df, col, threshold=mad_threshold
        )

    if 'percentile' in methods:
        result_df = detect_outliers_percentile(
            result_df, col, lower=percentile_lower, upper=percentile_upper
        )

    # Add combined anomaly flag (any method flagged as anomaly)
    anomaly_cols = [
        "is_anomaly_zscore", "is_anomaly_mad", "is_anomaly_percentile"
    ]
    existing_anomaly_cols = [c for c in anomaly_cols if c in result_df.columns]

    if existing_anomaly_cols:
        result_df = result_df.withColumn(
            "is_anomaly",
            F.array_contains(
                F.array(*[F.col(c) for c in existing_anomaly_cols]),
                True
            )
        )

    return result_df


def quarantine_records(df: DataFrame, valid_condition,
                      quarantine_path: str,
                      source_name: str = "unknown") -> Tuple[DataFrame, DataFrame]:
    """
    Split DataFrame into valid and quarantine (invalid) records.
    Writes quarantine records to specified path.

    Args:
        df: Input DataFrame
        valid_condition: Boolean condition defining valid records
        quarantine_path: Path to write quarantine records
        source_name: Name of source for logging

    Returns:
        Tuple of (valid_df, quarantine_df)
    """
    # Split data
    valid_df = df.filter(valid_condition)
    quarantine_df = df.filter(~valid_condition)

    # Count records
    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()
    total_count = valid_count + quarantine_count

    logger.info(f"{source_name} - Total records: {total_count}")
    logger.info(f"{source_name} - Valid records: {valid_count} ({100*valid_count/total_count:.2f}%)")
    logger.info(f"{source_name} - Quarantine records: {quarantine_count} ({100*quarantine_count/total_count:.2f}%)")

    # Write quarantine if not empty
    if quarantine_count > 0:
        quarantine_df.write.mode("overwrite").parquet(str(quarantine_path))
        logger.info(f"{source_name} - Quarantine records written to {quarantine_path}")
    else:
        logger.info(f"{source_name} - No quarantine records")

    return valid_df, quarantine_df


def add_data_quality_metrics(df: DataFrame,
                             required_cols: List[str] = None,
                             numeric_cols: List[str] = None) -> DataFrame:
    """
    Add data quality metrics columns to DataFrame.

    Args:
        df: Input DataFrame
        required_cols: List of columns that should not be null
        numeric_cols: List of numeric columns to validate

    Returns:
        DataFrame with quality metrics added
    """
    # Initialize quality issues array
    quality_issues = []

    # Check for nulls in required columns
    if required_cols:
        for col in required_cols:
            if col in df.columns:
                quality_issues.append(
                    F.when(F.col(col).isNull(), F.lit(f"null_{col}"))
                )

    # Check for negative values in numeric columns
    if numeric_cols:
        for col in numeric_cols:
            if col in df.columns:
                quality_issues.append(
                    F.when(F.col(col) < 0, F.lit(f"negative_{col}"))
                )

    # Combine quality issues
    if quality_issues:
        df = df.withColumn(
            "quality_issues",
            F.array_compact(F.array(*quality_issues))
        )

        # Add quality score (1 - (number of issues / total possible issues))
        df = df.withColumn(
            "quality_score",
            1 - (F.size(F.col("quality_issues")) / F.lit(len(quality_issues)))
        )

        # Is valid flag (no quality issues)
        df = df.withColumn(
            "is_valid",
            F.size(F.col("quality_issues")) == 0
        )
    else:
        df = df.withColumn("quality_issues", F.array())
        df = df.withColumn("quality_score", F.lit(1.0))
        df = df.withColumn("is_valid", F.lit(True))

    return df
