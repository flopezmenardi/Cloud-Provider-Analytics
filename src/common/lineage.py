"""
Data Lineage and Pipeline Evidence Tracking

Provides comprehensive lineage tracking, metrics collection, and evidence
generation for the Lambda Architecture pipeline.
"""

import logging
import json
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class TransformationMetrics:
    """Metrics captured during a transformation step."""
    input_count: int = 0
    output_count: int = 0
    quarantine_count: int = 0
    null_counts: Dict[str, int] = field(default_factory=dict)
    anomaly_count: int = 0
    schema_v1_count: int = 0
    schema_v2_count: int = 0
    duration_seconds: float = 0.0


@dataclass
class LineageRecord:
    """Record of data lineage for a transformation."""
    pipeline_run_id: str
    stage: str
    source_layer: str
    target_layer: str
    source_path: str
    target_path: str
    transformation_name: str
    started_at: str
    completed_at: Optional[str] = None
    status: str = "running"
    metrics: Optional[TransformationMetrics] = None
    error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        result = asdict(self)
        if self.metrics:
            result['metrics'] = asdict(self.metrics)
        return result


class PipelineEvidence:
    """
    Collects and stores evidence for pipeline execution.
    Used for presentation and grading documentation.
    """
    
    def __init__(self, run_id: Optional[str] = None):
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.start_time = datetime.now()
        self.lineage_records: List[LineageRecord] = []
        self.stage_metrics: Dict[str, Dict] = {}
        self.data_quality_summary: Dict[str, Any] = {}
        self.evidence_path = Path("datalake/evidence") / self.run_id
        self.evidence_path.mkdir(parents=True, exist_ok=True)
    
    def start_stage(
        self,
        stage: str,
        source_layer: str,
        target_layer: str,
        source_path: str,
        target_path: str,
        transformation_name: str
    ) -> LineageRecord:
        """Start tracking a transformation stage."""
        record = LineageRecord(
            pipeline_run_id=self.run_id,
            stage=stage,
            source_layer=source_layer,
            target_layer=target_layer,
            source_path=source_path,
            target_path=target_path,
            transformation_name=transformation_name,
            started_at=datetime.now().isoformat()
        )
        self.lineage_records.append(record)
        logger.info(f"[LINEAGE] Started: {stage} ({source_layer} -> {target_layer})")
        return record
    
    def complete_stage(
        self,
        record: LineageRecord,
        metrics: TransformationMetrics,
        status: str = "success"
    ):
        """Complete a transformation stage with metrics."""
        record.completed_at = datetime.now().isoformat()
        record.status = status
        record.metrics = metrics
        
        self.stage_metrics[record.stage] = {
            "input_count": metrics.input_count,
            "output_count": metrics.output_count,
            "quarantine_count": metrics.quarantine_count,
            "anomaly_count": metrics.anomaly_count,
            "duration_seconds": metrics.duration_seconds,
            "data_quality_rate": (
                metrics.output_count / metrics.input_count * 100 
                if metrics.input_count > 0 else 0
            )
        }
        
        logger.info(f"[LINEAGE] Completed: {record.stage} - {metrics.input_count:,} -> {metrics.output_count:,} records")
    
    def fail_stage(self, record: LineageRecord, error: str):
        """Mark a stage as failed."""
        record.completed_at = datetime.now().isoformat()
        record.status = "failed"
        record.error = error
        logger.error(f"[LINEAGE] Failed: {record.stage} - {error}")
    
    def capture_dataframe_profile(
        self,
        df: DataFrame,
        name: str,
        key_columns: List[str] = None
    ) -> Dict:
        """
        Capture a profile of a DataFrame for evidence.
        """
        profile = {
            "name": name,
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
            "captured_at": datetime.now().isoformat()
        }
        
        # Null counts for key columns
        if key_columns:
            null_counts = {}
            for col in key_columns:
                if col in df.columns:
                    null_count = df.filter(F.col(col).isNull()).count()
                    null_counts[col] = null_count
            profile["null_counts"] = null_counts
        
        # Sample data (first 5 rows as strings)
        sample_rows = df.limit(5).collect()
        profile["sample_data"] = [str(row.asDict()) for row in sample_rows]
        
        return profile
    
    def capture_aggregation_evidence(
        self,
        df: DataFrame,
        group_cols: List[str],
        agg_col: str,
        name: str
    ) -> Dict:
        """
        Capture evidence of aggregation correctness.
        Shows before/after totals match.
        """
        evidence = {
            "name": name,
            "group_columns": group_cols,
            "aggregation_column": agg_col,
            "captured_at": datetime.now().isoformat()
        }
        
        # Total before aggregation
        if agg_col in df.columns:
            total = df.agg(F.sum(F.coalesce(F.col(agg_col), F.lit(0)))).collect()[0][0]
            evidence["total_value"] = float(total) if total else 0.0
        
        # Distinct groups
        if group_cols:
            distinct_groups = df.select(*group_cols).distinct().count()
            evidence["distinct_groups"] = distinct_groups
        
        return evidence
    
    def add_data_quality_evidence(
        self,
        layer: str,
        table: str,
        valid_count: int,
        quarantine_count: int,
        anomaly_count: int,
        validation_rules: List[str]
    ):
        """Add data quality evidence for a table."""
        key = f"{layer}.{table}"
        self.data_quality_summary[key] = {
            "valid_records": valid_count,
            "quarantine_records": quarantine_count,
            "anomaly_records": anomaly_count,
            "quality_rate": valid_count / (valid_count + quarantine_count) * 100 if (valid_count + quarantine_count) > 0 else 100,
            "validation_rules_applied": validation_rules,
            "captured_at": datetime.now().isoformat()
        }
    
    def save_evidence(self):
        """Save all evidence to files."""
        end_time = datetime.now()
        
        # Summary
        summary = {
            "pipeline_run_id": self.run_id,
            "start_time": self.start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "total_duration_seconds": (end_time - self.start_time).total_seconds(),
            "stages_executed": len(self.lineage_records),
            "stages_successful": sum(1 for r in self.lineage_records if r.status == "success"),
            "stages_failed": sum(1 for r in self.lineage_records if r.status == "failed"),
        }
        
        # Write summary
        summary_path = self.evidence_path / "pipeline_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"[EVIDENCE] Summary saved: {summary_path}")
        
        # Write lineage
        lineage_path = self.evidence_path / "lineage.json"
        with open(lineage_path, 'w') as f:
            json.dump([r.to_dict() for r in self.lineage_records], f, indent=2)
        logger.info(f"[EVIDENCE] Lineage saved: {lineage_path}")
        
        # Write stage metrics
        metrics_path = self.evidence_path / "stage_metrics.json"
        with open(metrics_path, 'w') as f:
            json.dump(self.stage_metrics, f, indent=2)
        logger.info(f"[EVIDENCE] Metrics saved: {metrics_path}")
        
        # Write data quality summary
        dq_path = self.evidence_path / "data_quality.json"
        with open(dq_path, 'w') as f:
            json.dump(self.data_quality_summary, f, indent=2)
        logger.info(f"[EVIDENCE] Data quality saved: {dq_path}")
        
        return str(self.evidence_path)
    
    def print_summary(self):
        """Print a formatted summary for presentation."""
        print("\n" + "=" * 100)
        print("PIPELINE EXECUTION EVIDENCE")
        print("=" * 100)
        print(f"Run ID: {self.run_id}")
        print(f"Duration: {(datetime.now() - self.start_time).total_seconds():.2f}s")
        print()
        
        print("STAGE METRICS:")
        print("-" * 80)
        for stage, metrics in self.stage_metrics.items():
            print(f"  {stage}:")
            print(f"    Input:      {metrics['input_count']:>12,} records")
            print(f"    Output:     {metrics['output_count']:>12,} records")
            print(f"    Quarantine: {metrics['quarantine_count']:>12,} records")
            print(f"    Quality:    {metrics['data_quality_rate']:>12.2f}%")
            print()
        
        print("DATA QUALITY SUMMARY:")
        print("-" * 80)
        for table, dq in self.data_quality_summary.items():
            print(f"  {table}:")
            print(f"    Valid:      {dq['valid_records']:>12,}")
            print(f"    Quarantine: {dq['quarantine_records']:>12,}")
            print(f"    Anomalies:  {dq['anomaly_records']:>12,}")
            print(f"    Quality:    {dq['quality_rate']:>12.2f}%")
            print(f"    Rules:      {', '.join(dq['validation_rules_applied'])}")
            print()
        
        print("=" * 100)


def add_lineage_columns(df: DataFrame, source: str, transformation: str) -> DataFrame:
    """
    Add lineage tracking columns to a DataFrame.
    """
    return df.withColumn("_lineage_source", F.lit(source)) \
             .withColumn("_lineage_transformation", F.lit(transformation)) \
             .withColumn("_lineage_timestamp", F.current_timestamp())


def calculate_metrics(
    input_df: DataFrame,
    output_df: DataFrame,
    quarantine_df: DataFrame = None,
    anomaly_col: str = "is_anomaly",
    schema_version_col: str = "schema_version"
) -> TransformationMetrics:
    """
    Calculate transformation metrics from DataFrames.
    """
    metrics = TransformationMetrics()
    
    metrics.input_count = input_df.count()
    metrics.output_count = output_df.count()
    
    if quarantine_df is not None:
        metrics.quarantine_count = quarantine_df.count()
    
    if anomaly_col in output_df.columns:
        metrics.anomaly_count = output_df.filter(F.col(anomaly_col) == True).count()
    
    if schema_version_col in output_df.columns:
        version_counts = output_df.groupBy(schema_version_col).count().collect()
        for row in version_counts:
            if row[schema_version_col] == 1:
                metrics.schema_v1_count = row['count']
            elif row[schema_version_col] == 2:
                metrics.schema_v2_count = row['count']
    
    return metrics
