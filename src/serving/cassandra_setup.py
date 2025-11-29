"""
Cassandra/AstraDB Setup Script
Creates keyspace and tables for serving layer.
"""

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from src.config.cassandra_config import get_cassandra_config, validate_astradb_config, CASSANDRA_MODE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_cassandra_session():
    """
    Create and return a Cassandra session.

    Returns:
        session: Cassandra session object
    """
    config = get_cassandra_config()

    if config["mode"] == "astradb":
        logger.info("Connecting to AstraDB...")
        validate_astradb_config()

        cloud_config = {
            'secure_connect_bundle': config["secure_connect_bundle"]
        }

        auth_provider = PlainTextAuthProvider(
            config["client_id"],
            config["client_secret"]
        )

        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()

        logger.info(f"✓ Connected to AstraDB")

    else:
        logger.info("Connecting to local Cassandra...")
        cluster = Cluster(
            contact_points=config["contact_points"],
            port=config["port"]
        )
        session = cluster.connect()
        logger.info(f"✓ Connected to local Cassandra")

    return session, config["keyspace"]


def create_keyspace(session, keyspace_name):
    """
    Create keyspace if it doesn't exist.
    For AstraDB, keyspace is created via web UI, so this just verifies it exists.
    """
    if CASSANDRA_MODE == "astradb":
        logger.info(f"Using AstraDB keyspace: {keyspace_name}")
        logger.info("  (Keyspace must be created via AstraDB web UI)")
        session.set_keyspace(keyspace_name)
    else:
        logger.info(f"Creating keyspace: {keyspace_name}")
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        session.set_keyspace(keyspace_name)

    logger.info(f"✓ Using keyspace: {keyspace_name}")


def create_tables(session):
    """
    Create all serving layer tables with query-first design.
    """
    logger.info("Creating serving layer tables...")

    # Table 1: org_daily_usage - For Query 1 (Costos diarios por org y servicio)
    logger.info("Creating table: org_daily_usage")
    session.execute("""
        CREATE TABLE IF NOT EXISTS org_daily_usage (
            org_id text,
            usage_date date,
            service text,
            total_cost_usd decimal,
            total_requests bigint,
            cpu_hours decimal,
            storage_gb_hours decimal,
            carbon_kg decimal,
            genai_tokens bigint,
            PRIMARY KEY ((org_id), usage_date, service)
        ) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC)
    """)

    # Table 2: org_service_costs - For Query 2 (Top-N servicios por costo)
    logger.info("Creating table: org_service_costs")
    session.execute("""
        CREATE TABLE IF NOT EXISTS org_service_costs (
            org_id text,
            window_days int,
            service text,
            total_cost_usd decimal,
            PRIMARY KEY ((org_id, window_days), total_cost_usd, service)
        ) WITH CLUSTERING ORDER BY (total_cost_usd DESC, service ASC)
    """)

    # Table 3: tickets_critical_daily - For Query 3 (Tickets críticos y SLA breach)
    logger.info("Creating table: tickets_critical_daily")
    session.execute("""
        CREATE TABLE IF NOT EXISTS tickets_critical_daily (
            date date,
            severity text,
            total_tickets int,
            open_tickets int,
            resolved_tickets int,
            sla_breach_rate decimal,
            avg_resolution_hours decimal,
            PRIMARY KEY ((date), severity)
        ) WITH CLUSTERING ORDER BY (severity ASC)
    """)

    # Table 4: revenue_monthly - For Query 4 (Revenue mensual)
    logger.info("Creating table: revenue_monthly")
    session.execute("""
        CREATE TABLE IF NOT EXISTS revenue_monthly (
            org_id text,
            year_month text,
            org_name text,
            total_billed_usd decimal,
            total_credits decimal,
            total_taxes decimal,
            net_revenue decimal,
            invoice_count int,
            PRIMARY KEY ((org_id), year_month)
        ) WITH CLUSTERING ORDER BY (year_month DESC)
    """)

    # Table 5: genai_tokens_daily - For Query 5 (Tokens GenAI)
    logger.info("Creating table: genai_tokens_daily")
    session.execute("""
        CREATE TABLE IF NOT EXISTS genai_tokens_daily (
            org_id text,
            usage_date date,
            total_genai_tokens bigint,
            total_cost_usd decimal,
            cost_per_million_tokens decimal,
            PRIMARY KEY ((org_id), usage_date)
        ) WITH CLUSTERING ORDER BY (usage_date DESC)
    """)

    logger.info("✓ All tables created successfully")


def verify_tables(session):
    """
    Verify that all tables were created successfully.
    """
    logger.info("Verifying tables...")

    tables = [
        "org_daily_usage",
        "org_service_costs",
        "tickets_critical_daily",
        "revenue_monthly",
        "genai_tokens_daily"
    ]

    for table in tables:
        result = session.execute(f"SELECT * FROM {table} LIMIT 1")
        logger.info(f"  ✓ {table} - OK")

    logger.info(f"✓ All {len(tables)} tables verified")


def main():
    """
    Main setup function.
    """
    logger.info("=" * 80)
    logger.info("Cassandra/AstraDB Setup for Cloud Provider Analytics")
    logger.info("=" * 80)

    try:
        # Connect to Cassandra
        session, keyspace = get_cassandra_session()

        # Create/use keyspace
        create_keyspace(session, keyspace)

        # Create tables
        create_tables(session)

        # Verify tables
        verify_tables(session)

        logger.info("=" * 80)
        logger.info("✓ Cassandra setup completed successfully!")
        logger.info("=" * 80)
        logger.info("")
        logger.info("Next steps:")
        logger.info("1. Run Silver layer transformations to generate data")
        logger.info("2. Run Gold layer marts creation")
        logger.info("3. Load data into Cassandra with load_to_cassandra.py")
        logger.info("4. Run demo queries with demo_queries.py")

    except Exception as e:
        logger.error(f"✗ Setup failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
