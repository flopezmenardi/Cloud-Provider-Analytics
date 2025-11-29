"""
Demo Queries for Cloud Provider Analytics Serving Layer

Implements the 5 required demo queries using AstraDB Data API.
"""

import logging
import sys
from pathlib import Path
from tabulate import tabulate

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.astradb_config import get_astradb_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def query_1_daily_costs_by_service(db, org_id="org_001", limit=10):
    """
    Query 1: Costos diarios por organización y servicio

    Retrieve daily cost breakdown by service for a specific organization.
    Shows the most recent days first.

    Use case: FinOps team tracking daily spend by service for cost attribution.
    """
    logger.info("")
    logger.info("=" * 100)
    logger.info("QUERY 1: Daily Costs by Organization and Service")
    logger.info("=" * 100)
    logger.info(f"Parameters: org_id={org_id}, limit={limit}")
    logger.info("")

    collection = db.get_collection("org_daily_usage")

    # Find documents matching org_id, sort by date desc, limit
    cursor = collection.find(
        filter={"org_id": org_id},
        sort={"usage_date": -1},
        limit=limit
    )

    results = []
    for doc in cursor:
        results.append([
            doc.get('org_id', ''),
            doc.get('usage_date', ''),
            doc.get('service', ''),
            f"${float(doc.get('total_cost_usd', 0)):,.2f}",
            f"{int(doc.get('total_requests', 0)):,}",
            f"{float(doc.get('cpu_hours', 0)):,.2f}",
            f"{float(doc.get('storage_gb_hours', 0)):,.2f}"
        ])

    headers = ["Org ID", "Date", "Service", "Cost USD", "Requests", "CPU Hours", "Storage GB-Hrs"]
    print(tabulate(results, headers=headers, tablefmt="grid"))

    logger.info(f"✓ Retrieved {len(results)} rows")
    logger.info(f"Business insight: Shows daily service-level cost breakdown for organization {org_id}")

    return results


def query_2_top_services_by_cost(db, org_id="org_001", window_days=30, top_n=5):
    """
    Query 2: Top-N servicios por costo en una ventana de tiempo

    Find the most expensive services for an organization in a time window.

    Use case: Identify which services are driving the highest costs to optimize spend.
    """
    logger.info("")
    logger.info("=" * 100)
    logger.info("QUERY 2: Top Services by Cost (Time Window)")
    logger.info("=" * 100)
    logger.info(f"Parameters: org_id={org_id}, window_days={window_days}, top_n={top_n}")
    logger.info("")

    collection = db.get_collection("org_service_costs")

    # Find documents matching org_id and window, sort by cost desc, limit
    cursor = collection.find(
        filter={"org_id": org_id, "window_days": window_days},
        sort={"total_cost_usd": -1},
        limit=top_n
    )

    results = []
    for doc in cursor:
        results.append([
            doc.get('org_id', ''),
            f"{doc.get('window_days', 0)} days",
            doc.get('service', ''),
            f"${float(doc.get('total_cost_usd', 0)):,.2f}"
        ])

    headers = ["Org ID", "Time Window", "Service", "Total Cost USD"]
    print(tabulate(results, headers=headers, tablefmt="grid"))

    logger.info(f"✓ Retrieved {len(results)} services")
    logger.info(f"Business insight: Top {top_n} most expensive services for org {org_id} in last {window_days} days")

    return results


def query_3_critical_tickets_sla_breach(db, date_str=None, severity="critical"):
    """
    Query 3: Tickets críticos y tasa de incumplimiento SLA por fecha

    Monitor critical support tickets and SLA breach rates by date.

    Use case: Support operations tracking critical ticket volume and SLA compliance.
    """
    logger.info("")
    logger.info("=" * 100)
    logger.info("QUERY 3: Critical Tickets & SLA Breach Rate")
    logger.info("=" * 100)

    if date_str is None:
        # Use a recent date
        date_str = "2025-07-20"

    logger.info(f"Parameters: date={date_str}, severity={severity}")
    logger.info("")

    collection = db.get_collection("tickets_critical_daily")

    # Find documents matching date and severity
    cursor = collection.find(
        filter={"date": date_str, "severity": severity}
    )

    results = []
    for doc in cursor:
        results.append([
            doc.get('date', ''),
            doc.get('severity', ''),
            int(doc.get('total_tickets', 0)),
            f"{float(doc.get('sla_breach_rate', 0)) * 100:.1f}%",
            f"{float(doc.get('avg_resolution_hours', 0)):.1f}h"
        ])

    headers = ["Date", "Severity", "Total", "SLA Breach Rate", "Avg Resolution"]
    print(tabulate(results, headers=headers, tablefmt="grid"))

    logger.info(f"✓ Retrieved {len(results)} rows")
    logger.info(f"Business insight: {severity} ticket metrics for {date_str} - monitoring support operations health")

    return results


def query_4_monthly_revenue(db, org_id="org_001", limit=6):
    """
    Query 4: Revenue mensual por organización

    Track monthly revenue trends for an organization.

    Use case: Finance team monitoring revenue trends and billing metrics.
    """
    logger.info("")
    logger.info("=" * 100)
    logger.info("QUERY 4: Monthly Revenue by Organization")
    logger.info("=" * 100)
    logger.info(f"Parameters: org_id={org_id}, limit={limit} months")
    logger.info("")

    collection = db.get_collection("revenue_monthly")

    # Find documents matching org_id, sort by month desc, limit
    cursor = collection.find(
        filter={"org_id": org_id},
        sort={"year_month": -1},
        limit=limit
    )

    results = []
    for doc in cursor:
        results.append([
            doc.get('org_id', ''),
            doc.get('year_month', ''),
            doc.get('org_name', 'N/A'),
            f"${float(doc.get('total_billed_usd', 0)):,.2f}",
            f"${float(doc.get('total_credits_usd', 0)):,.2f}",
            f"${float(doc.get('total_taxes_usd', 0)):,.2f}",
            f"${float(doc.get('net_revenue', 0)):,.2f}",
            int(doc.get('invoice_count', 0))
        ])

    headers = ["Org ID", "Month", "Org Name", "Billed USD", "Credits", "Taxes", "Net Revenue", "Invoices"]
    print(tabulate(results, headers=headers, tablefmt="grid"))

    logger.info(f"✓ Retrieved {len(results)} months")
    logger.info(f"Business insight: Monthly revenue trend for {org_id} - tracking financial performance")

    return results


def query_5_genai_token_usage(db, org_id="org_001", limit=10):
    """
    Query 5: Uso de tokens GenAI por organización y fecha

    Track GenAI/LLM token consumption and costs for an organization.

    Use case: Product team monitoring GenAI feature adoption and associated costs.
    """
    logger.info("")
    logger.info("=" * 100)
    logger.info("QUERY 5: GenAI Token Usage by Organization")
    logger.info("=" * 100)
    logger.info(f"Parameters: org_id={org_id}, limit={limit}")
    logger.info("")

    collection = db.get_collection("genai_tokens_daily")

    # Find documents matching org_id, sort by date desc, limit
    cursor = collection.find(
        filter={"org_id": org_id},
        sort={"usage_date": -1},
        limit=limit
    )

    results = []
    for doc in cursor:
        results.append([
            doc.get('org_id', ''),
            doc.get('usage_date', ''),
            f"{int(doc.get('total_genai_tokens', 0)):,}",
            f"${float(doc.get('total_cost_usd', 0)):,.2f}",
            f"${float(doc.get('cost_per_million_tokens', 0)):,.2f}/M"
        ])

    headers = ["Org ID", "Date", "Total Tokens", "Cost USD", "Cost per Million"]
    print(tabulate(results, headers=headers, tablefmt="grid"))

    logger.info(f"✓ Retrieved {len(results)} rows")
    logger.info(f"Business insight: GenAI token consumption for {org_id} - tracking AI feature usage and costs")

    return results


def run_all_queries(db):
    """Run all 5 demo queries with sample parameters."""

    logger.info("")
    logger.info("=" * 100)
    logger.info("RUNNING ALL 5 DEMO QUERIES")
    logger.info("=" * 100)

    queries = [
        ("Query 1: Daily Costs by Service", lambda: query_1_daily_costs_by_service(db, org_id="org_001", limit=10)),
        ("Query 2: Top Services by Cost", lambda: query_2_top_services_by_cost(db, org_id="org_001", window_days=30, top_n=5)),
        ("Query 3: Critical Tickets & SLA", lambda: query_3_critical_tickets_sla_breach(db, date_str="2025-07-20", severity="critical")),
        ("Query 4: Monthly Revenue", lambda: query_4_monthly_revenue(db, org_id="org_001", limit=6)),
        ("Query 5: GenAI Token Usage", lambda: query_5_genai_token_usage(db, org_id="org_001", limit=10))
    ]

    results = {}

    for name, query_func in queries:
        try:
            result = query_func()
            results[name] = {"status": "SUCCESS", "rows": len(result)}
        except Exception as e:
            logger.error(f"✗ {name} failed: {e}", exc_info=True)
            results[name] = {"status": "FAILED", "error": str(e)}

    # Print summary
    logger.info("")
    logger.info("=" * 100)
    logger.info("QUERY EXECUTION SUMMARY")
    logger.info("=" * 100)

    for name, result in results.items():
        if result["status"] == "SUCCESS":
            logger.info(f"✓ {name}: {result['rows']} rows")
        else:
            logger.info(f"✗ {name}: FAILED - {result.get('error', 'Unknown error')}")

    logger.info("=" * 100)

    return results


def main():
    """Main function to run demo queries."""

    logger.info("=" * 100)
    logger.info("CLOUD PROVIDER ANALYTICS - DEMO QUERIES")
    logger.info("Serving Layer (AstraDB Data API)")
    logger.info("=" * 100)

    try:
        # Connect to AstraDB
        db = get_astradb_client()
        logger.info("✓ Connected to AstraDB")

        # Run all queries
        run_all_queries(db)

        logger.info("")
        logger.info("✓ All demo queries completed!")
        logger.info("")

    except Exception as e:
        logger.error(f"✗ Demo queries failed: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
