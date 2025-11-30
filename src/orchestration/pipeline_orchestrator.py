"""
Pipeline Orchestrator

Orchestrates the full data pipeline execution with dependency management.
Implements the Lambda Architecture batch path: Bronze -> Silver -> Gold -> Serving
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Callable
from enum import Enum
from dataclasses import dataclass, field

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineStage(Enum):
    """Pipeline execution stages."""
    BRONZE_BATCH = "bronze_batch"
    BRONZE_STREAMING = "bronze_streaming"
    SILVER = "silver"
    GOLD = "gold"
    SERVING = "serving"
    SPEED = "speed"


class StageStatus(Enum):
    """Stage execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class StageResult:
    """Result of a pipeline stage execution."""
    stage: PipelineStage
    status: StageStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    error: Optional[str] = None
    records_processed: int = 0
    
    def to_dict(self) -> Dict:
        return {
            "stage": self.stage.value,
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
            "records_processed": self.records_processed
        }


@dataclass
class PipelineConfig:
    """Pipeline configuration."""
    run_bronze_batch: bool = True
    run_bronze_streaming: bool = True
    run_silver: bool = True
    run_gold: bool = True
    run_serving: bool = False  # Requires AstraDB config
    run_speed: bool = False    # Long-running streaming
    speed_duration: Optional[int] = 60  # Speed layer duration in seconds
    fail_fast: bool = False    # Stop on first failure


class PipelineOrchestrator:
    """
    Orchestrates pipeline execution with dependency management.
    
    Execution order:
    1. Bronze (Batch) - Ingest CSV files
    2. Bronze (Streaming) - Ingest JSONL files
    3. Silver - Data quality and conformance
    4. Gold - Business aggregations
    5. Serving - Load to AstraDB (optional)
    6. Speed - Real-time streaming (optional, long-running)
    """
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self.results: Dict[PipelineStage, StageResult] = {}
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    def _run_stage(
        self,
        stage: PipelineStage,
        func: Callable,
        *args,
        **kwargs
    ) -> StageResult:
        """
        Run a pipeline stage with timing and error handling.
        """
        start_time = datetime.now()
        result = StageResult(
            stage=stage,
            status=StageStatus.RUNNING,
            start_time=start_time
        )
        
        logger.info("")
        logger.info("=" * 100)
        logger.info(f"STAGE: {stage.value.upper()}")
        logger.info(f"Started: {start_time}")
        logger.info("=" * 100)
        
        try:
            func(*args, **kwargs)
            
            result.status = StageStatus.SUCCESS
            logger.info(f"Stage {stage.value} completed successfully")
            
        except Exception as e:
            result.status = StageStatus.FAILED
            result.error = str(e)
            logger.error(f"Stage {stage.value} FAILED: {e}", exc_info=True)
            
            if self.config.fail_fast:
                raise
        
        finally:
            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - start_time).total_seconds()
            self.results[stage] = result
        
        return result
    
    def _run_bronze_batch(self):
        """Run Bronze batch ingestion."""
        from src.ingestion.batch.ingest_all_batch import run_all_batch_ingestion
        run_all_batch_ingestion()
    
    def _run_bronze_streaming(self):
        """Run Bronze streaming ingestion in batch mode."""
        from src.ingestion.streaming.ingest_usage_events_stream import (
            ingest_usage_events_batch
        )
        from src.config.spark_config import get_spark_session
        
        spark = get_spark_session("Bronze-StreamingBatch")
        try:
            ingest_usage_events_batch(spark)
        finally:
            spark.stop()
    
    def _run_silver(self):
        """Run Silver transformations."""
        from src.transformations.transform_all import run_all_silver_transformations
        run_all_silver_transformations()
    
    def _run_gold(self):
        """Run Gold mart creation."""
        from src.aggregations.create_all_marts import create_all_gold_marts
        create_all_gold_marts()
    
    def _run_serving(self):
        """Load data to AstraDB serving layer."""
        from src.serving.load_to_astradb import main as load_to_astradb
        load_to_astradb()
    
    def _run_speed(self, duration: Optional[int] = None):
        """Run Speed layer streaming."""
        from src.speed.streaming_aggregations import run_streaming_aggregations
        run_streaming_aggregations(
            mode="all",
            duration_seconds=duration or self.config.speed_duration
        )
    
    def run(self) -> Dict[PipelineStage, StageResult]:
        """
        Execute the full pipeline according to configuration.
        
        Returns:
            Dictionary of stage results
        """
        self.start_time = datetime.now()
        self.results = {}
        
        logger.info("")
        logger.info("*" * 100)
        logger.info("CLOUD PROVIDER ANALYTICS - PIPELINE ORCHESTRATOR")
        logger.info(f"Start time: {self.start_time}")
        logger.info("*" * 100)
        
        # Stage 1: Bronze Batch
        if self.config.run_bronze_batch:
            result = self._run_stage(
                PipelineStage.BRONZE_BATCH,
                self._run_bronze_batch
            )
            if result.status == StageStatus.FAILED and self.config.fail_fast:
                return self._finalize()
        
        # Stage 2: Bronze Streaming (batch mode for initial load)
        if self.config.run_bronze_streaming:
            result = self._run_stage(
                PipelineStage.BRONZE_STREAMING,
                self._run_bronze_streaming
            )
            if result.status == StageStatus.FAILED and self.config.fail_fast:
                return self._finalize()
        
        # Stage 3: Silver
        if self.config.run_silver:
            # Check dependencies
            bronze_ok = True
            if self.config.run_bronze_batch:
                bronze_ok = bronze_ok and self.results.get(
                    PipelineStage.BRONZE_BATCH, 
                    StageResult(PipelineStage.BRONZE_BATCH, StageStatus.SUCCESS, datetime.now())
                ).status == StageStatus.SUCCESS
            if self.config.run_bronze_streaming:
                bronze_ok = bronze_ok and self.results.get(
                    PipelineStage.BRONZE_STREAMING,
                    StageResult(PipelineStage.BRONZE_STREAMING, StageStatus.SUCCESS, datetime.now())
                ).status == StageStatus.SUCCESS
            
            if bronze_ok or not (self.config.run_bronze_batch or self.config.run_bronze_streaming):
                result = self._run_stage(
                    PipelineStage.SILVER,
                    self._run_silver
                )
                if result.status == StageStatus.FAILED and self.config.fail_fast:
                    return self._finalize()
            else:
                logger.warning("Skipping Silver: Bronze layer failed")
                self.results[PipelineStage.SILVER] = StageResult(
                    stage=PipelineStage.SILVER,
                    status=StageStatus.SKIPPED,
                    start_time=datetime.now()
                )
        
        # Stage 4: Gold
        if self.config.run_gold:
            silver_ok = self.results.get(
                PipelineStage.SILVER,
                StageResult(PipelineStage.SILVER, StageStatus.SUCCESS, datetime.now())
            ).status == StageStatus.SUCCESS
            
            if silver_ok or not self.config.run_silver:
                result = self._run_stage(
                    PipelineStage.GOLD,
                    self._run_gold
                )
                if result.status == StageStatus.FAILED and self.config.fail_fast:
                    return self._finalize()
            else:
                logger.warning("Skipping Gold: Silver layer failed")
                self.results[PipelineStage.GOLD] = StageResult(
                    stage=PipelineStage.GOLD,
                    status=StageStatus.SKIPPED,
                    start_time=datetime.now()
                )
        
        # Stage 5: Serving (optional)
        if self.config.run_serving:
            gold_ok = self.results.get(
                PipelineStage.GOLD,
                StageResult(PipelineStage.GOLD, StageStatus.SUCCESS, datetime.now())
            ).status == StageStatus.SUCCESS
            
            if gold_ok or not self.config.run_gold:
                self._run_stage(
                    PipelineStage.SERVING,
                    self._run_serving
                )
            else:
                logger.warning("Skipping Serving: Gold layer failed")
                self.results[PipelineStage.SERVING] = StageResult(
                    stage=PipelineStage.SERVING,
                    status=StageStatus.SKIPPED,
                    start_time=datetime.now()
                )
        
        # Stage 6: Speed (optional, runs for configured duration)
        if self.config.run_speed:
            self._run_stage(
                PipelineStage.SPEED,
                self._run_speed,
                self.config.speed_duration
            )
        
        return self._finalize()
    
    def _finalize(self) -> Dict[PipelineStage, StageResult]:
        """Finalize pipeline execution and print summary."""
        self.end_time = datetime.now()
        total_duration = (self.end_time - self.start_time).total_seconds()
        
        logger.info("")
        logger.info("*" * 100)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("*" * 100)
        logger.info(f"Start time:     {self.start_time}")
        logger.info(f"End time:       {self.end_time}")
        logger.info(f"Total duration: {total_duration:.2f}s ({total_duration/60:.2f} minutes)")
        logger.info("")
        
        # Count statuses
        success = sum(1 for r in self.results.values() if r.status == StageStatus.SUCCESS)
        failed = sum(1 for r in self.results.values() if r.status == StageStatus.FAILED)
        skipped = sum(1 for r in self.results.values() if r.status == StageStatus.SKIPPED)
        
        logger.info(f"Stages: {success} success, {failed} failed, {skipped} skipped")
        logger.info("")
        
        # Print each stage result
        for stage, result in self.results.items():
            symbol = {
                StageStatus.SUCCESS: "[OK]",
                StageStatus.FAILED: "[FAIL]",
                StageStatus.SKIPPED: "[SKIP]"
            }.get(result.status, "[???]")
            
            logger.info(f"  {symbol} {stage.value:20s} - {result.duration_seconds:8.2f}s")
            if result.error:
                logger.info(f"        Error: {result.error[:80]}")
        
        logger.info("")
        logger.info("*" * 100)
        
        if failed > 0:
            logger.warning(f"{failed} stage(s) failed. Please review logs above.")
        else:
            logger.info("Pipeline completed successfully!")
        
        return self.results


def run_full_pipeline(
    include_serving: bool = False,
    include_speed: bool = False,
    speed_duration: int = 60,
    fail_fast: bool = False
) -> int:
    """
    Run the full pipeline with all stages.
    
    Args:
        include_serving: Whether to load data to AstraDB
        include_speed: Whether to run speed layer streaming
        speed_duration: How long to run speed layer (seconds)
        fail_fast: Stop on first failure
    
    Returns:
        Exit code (0 = success, 1 = failure)
    """
    config = PipelineConfig(
        run_bronze_batch=True,
        run_bronze_streaming=True,
        run_silver=True,
        run_gold=True,
        run_serving=include_serving,
        run_speed=include_speed,
        speed_duration=speed_duration,
        fail_fast=fail_fast
    )
    
    orchestrator = PipelineOrchestrator(config)
    results = orchestrator.run()
    
    # Check for failures
    failed = any(r.status == StageStatus.FAILED for r in results.values())
    return 1 if failed else 0


def run_batch_pipeline(
    bronze: bool = True,
    silver: bool = True,
    gold: bool = True,
    fail_fast: bool = False
) -> int:
    """
    Run only the batch pipeline stages.
    
    Args:
        bronze: Run bronze ingestion
        silver: Run silver transformations
        gold: Run gold aggregations
        fail_fast: Stop on first failure
    
    Returns:
        Exit code (0 = success, 1 = failure)
    """
    config = PipelineConfig(
        run_bronze_batch=bronze,
        run_bronze_streaming=bronze,
        run_silver=silver,
        run_gold=gold,
        run_serving=False,
        run_speed=False,
        fail_fast=fail_fast
    )
    
    orchestrator = PipelineOrchestrator(config)
    results = orchestrator.run()
    
    failed = any(r.status == StageStatus.FAILED for r in results.values())
    return 1 if failed else 0


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Cloud Provider Analytics Pipeline Orchestrator"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full pipeline (bronze -> silver -> gold)"
    )
    parser.add_argument(
        "--serving",
        action="store_true",
        help="Include serving layer (load to AstraDB)"
    )
    parser.add_argument(
        "--speed",
        action="store_true",
        help="Include speed layer (streaming aggregations)"
    )
    parser.add_argument(
        "--speed-duration",
        type=int,
        default=60,
        help="Speed layer duration in seconds (default: 60)"
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on first failure"
    )
    parser.add_argument(
        "--bronze-only",
        action="store_true",
        help="Run only bronze ingestion"
    )
    parser.add_argument(
        "--silver-only",
        action="store_true",
        help="Run only silver transformations (assumes bronze exists)"
    )
    parser.add_argument(
        "--gold-only",
        action="store_true",
        help="Run only gold aggregations (assumes silver exists)"
    )
    
    args = parser.parse_args()
    
    if args.bronze_only:
        exit_code = run_batch_pipeline(bronze=True, silver=False, gold=False)
    elif args.silver_only:
        exit_code = run_batch_pipeline(bronze=False, silver=True, gold=False)
    elif args.gold_only:
        exit_code = run_batch_pipeline(bronze=False, silver=False, gold=True)
    else:
        exit_code = run_full_pipeline(
            include_serving=args.serving,
            include_speed=args.speed,
            speed_duration=args.speed_duration,
            fail_fast=args.fail_fast
        )
    
    sys.exit(exit_code)


