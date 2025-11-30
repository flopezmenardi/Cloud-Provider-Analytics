"""
AstraDB Setup Script
Creates collections for serving layer using Data API.
"""

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.astradb_config import get_astradb_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_collections(db):
    """
    Create all serving layer collections.
    Collections are schema-less, similar to MongoDB.
    """
    logger.info("Creating serving layer collections...")

    collections = [
        "org_daily_usage",
        "org_service_costs",
        "tickets_critical_daily",
        "revenue_monthly",
        "genai_tokens_daily",
        "cost_anomalies"
    ]

    for collection_name in collections:
        try:
            logger.info(f"  Creating collection: {collection_name}")
            db.create_collection(collection_name)
            logger.info(f"  ✓ {collection_name} created")
        except Exception as e:
            # Collection might already exist
            if "already exists" in str(e).lower():
                logger.info(f"  → {collection_name} already exists (skipping)")
            else:
                raise

    logger.info(f"✓ All {len(collections)} collections ready")


def verify_collections(db):
    """
    Verify that all collections were created successfully.
    """
    logger.info("Verifying collections...")

    collection_names = db.list_collection_names()
    expected = [
        "org_daily_usage",
        "org_service_costs",
        "tickets_critical_daily",
        "revenue_monthly",
        "genai_tokens_daily",
        "cost_anomalies"
    ]

    for name in expected:
        if name in collection_names:
            logger.info(f"  ✓ {name} - OK")
        else:
            logger.error(f"  ✗ {name} - MISSING")
            raise ValueError(f"Collection {name} was not created")

    logger.info(f"✓ All {len(expected)} collections verified")


def main():
    """
    Main setup function.
    """
    logger.info("=" * 80)
    logger.info("AstraDB Setup for Cloud Provider Analytics")
    logger.info("=" * 80)

    try:
        # Connect to AstraDB
        logger.info("Connecting to AstraDB...")
        db = get_astradb_client()
        logger.info("✓ Connected to AstraDB")

        # Create collections
        create_collections(db)

        # Verify collections
        verify_collections(db)

        logger.info("=" * 80)
        logger.info("✓ AstraDB setup completed successfully!")
        logger.info("=" * 80)
        logger.info("")
        logger.info("Next steps:")
        logger.info("1. Run Gold layer marts creation: ./scripts/run_gold_marts.sh")
        logger.info("2. Load data into AstraDB: ./scripts/load_to_astradb.sh")
        logger.info("3. Run demo queries: ./scripts/run_demo_queries.sh")

    except Exception as e:
        logger.error(f"✗ Setup failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
