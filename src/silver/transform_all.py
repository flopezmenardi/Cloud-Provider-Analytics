"""
Silver Layer Orchestrator
Executes all Bronze → Silver transformations in the correct order.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.spark_config import get_spark_session

# Import all transformation modules
from src.silver import (
    transform_customers_orgs_silver,
    transform_users_silver,
    transform_resources_silver,
    transform_billing_monthly_silver,
    transform_support_tickets_silver,
    transform_marketing_touches_silver,
    transform_nps_surveys_silver,
    transform_usage_events_silver
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_all_silver_transformations():
    """
    Execute all Silver transformations in dependency order:
    1. Customers/Orgs first (referenced by others)
    2. Resources second (referenced by usage_events)
    3. Rest of batch sources
    4. Usage events last (enriched with resources and customers)
    """
    start_time = datetime.now()
    logger.info("=" * 100)
    logger.info("STARTING SILVER LAYER ORCHESTRATION")
    logger.info(f"Start time: {start_time}")
    logger.info("=" * 100)

    transformations = [
        ("customers_orgs", transform_customers_orgs_silver.transform_customers_orgs_to_silver),
        ("resources", transform_resources_silver.transform_resources_to_silver),
        ("users", transform_users_silver.transform_users_to_silver),
        ("billing_monthly", transform_billing_monthly_silver.transform_billing_to_silver),
        ("support_tickets", transform_support_tickets_silver.transform_support_tickets_to_silver),
        ("marketing_touches", transform_marketing_touches_silver.transform_marketing_touches_to_silver),
        ("nps_surveys", transform_nps_surveys_silver.transform_nps_surveys_to_silver),
        ("usage_events", transform_usage_events_silver.transform_usage_events_to_silver),
    ]

    results = {}
    spark = get_spark_session("Silver Layer Orchestrator")

    try:
        for i, (name, transform_func) in enumerate(transformations, 1):
            logger.info("")
            logger.info("=" * 100)
            logger.info(f"[{i}/{len(transformations)}] Running transformation: {name}")
            logger.info("=" * 100)

            try:
                step_start = datetime.now()
                transform_func(spark)
                step_duration = (datetime.now() - step_start).total_seconds()

                results[name] = {
                    "status": "SUCCESS",
                    "duration_seconds": step_duration
                }
                logger.info(f"✓ {name} completed successfully in {step_duration:.2f}s")

            except Exception as e:
                step_duration = (datetime.now() - step_start).total_seconds()
                results[name] = {
                    "status": "FAILED",
                    "error": str(e),
                    "duration_seconds": step_duration
                }
                logger.error(f"✗ {name} FAILED after {step_duration:.2f}s: {e}", exc_info=True)
                # Continue with other transformations even if one fails
                continue

    finally:
        spark.stop()

    # Print summary
    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds()

    logger.info("")
    logger.info("=" * 100)
    logger.info("SILVER LAYER ORCHESTRATION SUMMARY")
    logger.info("=" * 100)
    logger.info(f"Start time:  {start_time}")
    logger.info(f"End time:    {end_time}")
    logger.info(f"Total duration: {total_duration:.2f}s ({total_duration/60:.2f} minutes)")
    logger.info("")

    successful = sum(1 for r in results.values() if r["status"] == "SUCCESS")
    failed = sum(1 for r in results.values() if r["status"] == "FAILED")

    logger.info(f"Transformations: {successful} successful, {failed} failed")
    logger.info("")

    for name, result in results.items():
        status_symbol = "✓" if result["status"] == "SUCCESS" else "✗"
        duration = result["duration_seconds"]
        logger.info(f"  {status_symbol} {name:25s} - {result['status']:10s} ({duration:6.2f}s)")

        if result["status"] == "FAILED":
            logger.info(f"      Error: {result['error']}")

    logger.info("=" * 100)

    if failed > 0:
        logger.warning(f"⚠ {failed} transformation(s) failed. Please review logs above.")
        return 1
    else:
        logger.info("✓ All Silver transformations completed successfully!")
        return 0


def main():
    exit_code = run_all_silver_transformations()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
