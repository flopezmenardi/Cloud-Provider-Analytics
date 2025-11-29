"""
Gold Layer Orchestrator
Executes all Gold mart creations in the correct order.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session

# Import all mart creation modules
from src.aggregations import (
    create_org_daily_usage_by_service,
    create_revenue_by_org_month,
    create_tickets_by_org_date,
    create_genai_tokens_by_org_date,
    create_cost_anomaly_mart
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_all_gold_marts():
    """
    Execute all Gold mart creations in sequence.

    Order:
    1. org_daily_usage_by_service (from usage_events)
    2. revenue_by_org_month (from billing + customers_orgs)
    3. tickets_by_org_date (from support_tickets)
    4. genai_tokens_by_org_date (from usage_events - v2 only)
    5. cost_anomaly_mart (from usage_events - anomalies only)
    """
    start_time = datetime.now()
    logger.info("=" * 100)
    logger.info("STARTING GOLD LAYER ORCHESTRATION")
    logger.info(f"Start time: {start_time}")
    logger.info("=" * 100)

    marts = [
        ("org_daily_usage_by_service", create_org_daily_usage_by_service.create_org_daily_usage_by_service),
        ("revenue_by_org_month", create_revenue_by_org_month.create_revenue_by_org_month),
        ("tickets_by_org_date", create_tickets_by_org_date.create_tickets_by_org_date),
        ("genai_tokens_by_org_date", create_genai_tokens_by_org_date.create_genai_tokens_by_org_date),
        ("cost_anomaly_mart", create_cost_anomaly_mart.create_cost_anomaly_mart),
    ]

    results = {}
    spark = get_spark_session("Gold Layer Orchestrator")

    try:
        for i, (name, create_func) in enumerate(marts, 1):
            logger.info("")
            logger.info("=" * 100)
            logger.info(f"[{i}/{len(marts)}] Creating mart: {name}")
            logger.info("=" * 100)

            try:
                step_start = datetime.now()
                create_func(spark)
                step_duration = (datetime.now() - step_start).total_seconds()

                results[name] = {
                    "status": "SUCCESS",
                    "duration_seconds": step_duration
                }
                logger.info(f"✓ {name} created successfully in {step_duration:.2f}s")

            except Exception as e:
                step_duration = (datetime.now() - step_start).total_seconds()
                results[name] = {
                    "status": "FAILED",
                    "error": str(e),
                    "duration_seconds": step_duration
                }
                logger.error(f"✗ {name} FAILED after {step_duration:.2f}s: {e}", exc_info=True)
                # Continue with other marts even if one fails
                continue

    finally:
        spark.stop()

    # Print summary
    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds()

    logger.info("")
    logger.info("=" * 100)
    logger.info("GOLD LAYER ORCHESTRATION SUMMARY")
    logger.info("=" * 100)
    logger.info(f"Start time:  {start_time}")
    logger.info(f"End time:    {end_time}")
    logger.info(f"Total duration: {total_duration:.2f}s ({total_duration/60:.2f} minutes)")
    logger.info("")

    successful = sum(1 for r in results.values() if r["status"] == "SUCCESS")
    failed = sum(1 for r in results.values() if r["status"] == "FAILED")

    logger.info(f"Marts: {successful} successful, {failed} failed")
    logger.info("")

    for name, result in results.items():
        status_symbol = "✓" if result["status"] == "SUCCESS" else "✗"
        duration = result["duration_seconds"]
        logger.info(f"  {status_symbol} {name:35s} - {result['status']:10s} ({duration:6.2f}s)")

        if result["status"] == "FAILED":
            logger.info(f"      Error: {result['error']}")

    logger.info("=" * 100)

    if failed > 0:
        logger.warning(f"⚠ {failed} mart(s) failed. Please review logs above.")
        return 1
    else:
        logger.info("✓ All Gold marts created successfully!")
        return 0


def main():
    exit_code = create_all_gold_marts()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
