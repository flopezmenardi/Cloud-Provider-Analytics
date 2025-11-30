"""
Common utilities for the Cloud Provider Analytics project.

Modules:
- data_quality: Data validation, anomaly detection, quarantine management
- metadata: Ingestion metadata utilities
- lineage: Data lineage and pipeline evidence tracking
- idempotency: Idempotent write patterns for batch and streaming
"""

from src.common.data_quality import (
    validate_ranges,
    detect_outliers_zscore,
    detect_outliers_mad,
    detect_outliers_percentile,
    add_anomaly_flags,
    quarantine_records,
    add_data_quality_metrics
)

from src.common.metadata import add_ingestion_metadata

from src.common.lineage import (
    PipelineEvidence,
    TransformationMetrics,
    LineageRecord,
    add_lineage_columns,
    calculate_metrics
)

from src.common.idempotency import (
    deduplicate_by_key,
    idempotent_write_parquet,
    merge_incremental,
    streaming_dedup_window,
    validate_idempotency,
    BatchIdempotencyManager
)

__all__ = [
    'validate_ranges',
    'detect_outliers_zscore',
    'detect_outliers_mad',
    'detect_outliers_percentile',
    'add_anomaly_flags',
    'quarantine_records',
    'add_data_quality_metrics',
    'add_ingestion_metadata',
    'PipelineEvidence',
    'TransformationMetrics',
    'LineageRecord',
    'add_lineage_columns',
    'calculate_metrics',
    'deduplicate_by_key',
    'idempotent_write_parquet',
    'merge_incremental',
    'streaming_dedup_window',
    'validate_idempotency',
    'BatchIdempotencyManager',
]
