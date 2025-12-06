"""
Demo Queries for Cloud Provider Analytics Serving Layer

Implements the 5 required demo queries using AstraDB Data API.

Required Queries (from TP specification):
1. Costos y requests diarios por org y servicio en un rango de fechas.
2. Top-N servicios por costo acumulado en los últimos 14 días para una organización.
3. Evolución de tickets críticos y tasa de SLA breach por día (últimos 30 días).
4. Revenue mensual con créditos/impuestos aplicados (normalizado a USD).
5. Tokens GenAI y costo estimado por día (si existen).
"""

import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
from tabulate import tabulate

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.astradb_config import get_astradb_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_sample_org_id(db):
    """
    Get a sample org_id from the data.

    This helper function dynamically retrieves a valid org_id from the database,
    making queries work with any dataset without hardcoding specific org IDs.
    """
    collection = db.get_collection("org_daily_usage")
    sample_doc = collection.find_one({})
    if sample_doc and 'org_id' in sample_doc:
        return sample_doc['org_id']
    return None


def get_date_range_from_data(db):
    """
    Get the available date range from the data.
    Returns (min_date, max_date) as strings.
    """
    collection = db.get_collection("org_daily_usage")
    
    # Get a few documents to find date range
    cursor = collection.find({}, limit=100)
    dates = [doc.get('usage_date') for doc in cursor if doc.get('usage_date')]
    
    if dates:
        dates_sorted = sorted(dates)
        return dates_sorted[0], dates_sorted[-1]
    return None, None


def query_1_daily_costs_by_org_service_daterange(db, org_id=None, start_date=None, end_date=None, limit=50):
    """
    Query 1: Costos y requests diarios por org y servicio en un rango de fechas.
    
    TP Requirement: "Costos y requests diarios por org y servicio en un rango de fechas."

    Retrieve daily cost and request breakdown by service for a specific organization
    within a specified date range.

    Use case: FinOps team tracking daily spend by service for cost attribution.
    
    CQL Equivalent:
    SELECT org_id, usage_date, service, total_cost_usd, total_requests 
    FROM org_daily_usage 
    WHERE org_id = ? AND usage_date >= ? AND usage_date <= ?
    ORDER BY usage_date DESC;
    """
    # Get a valid org_id if not provided
    if org_id is None:
        org_id = get_sample_org_id(db)
        if org_id is None:
            logger.error("No data found in org_daily_usage collection")
            return []
    
    # Get date range from data if not provided
    if start_date is None or end_date is None:
        data_start, data_end = get_date_range_from_data(db)
        if data_start and data_end:
            end_date = end_date or data_end
            # Default to last 7 days of data
            start_date = start_date or data_start
        else:
            # Fallback dates
            end_date = end_date or "2025-07-31"
            start_date = start_date or "2025-07-01"

    logger.info("")
    logger.info("=" * 100)
    logger.info("QUERY 1: Costos y requests diarios por org y servicio en un rango de fechas")
    logger.info("=" * 100)
    logger.info(f"Parameters: org_id={org_id}, start_date={start_date}, end_date={end_date}")
    logger.info("")
    logger.info("CQL Equivalent:")
    logger.info(f"  SELECT org_id, usage_date, service, total_cost_usd, total_requests")
    logger.info(f"  FROM org_daily_usage")
    logger.info(f"  WHERE org_id = '{org_id}' AND usage_date >= '{start_date}' AND usage_date <= '{end_date}'")
    logger.info(f"  ORDER BY usage_date DESC;")
    logger.info("")

    collection = db.get_collection("org_daily_usage")

    # Find documents matching org_id and date range
    # AstraDB Data API uses MongoDB-style queries
    cursor = collection.find(
        filter={
            "org_id": org_id,
            "usage_date": {"$gte": start_date, "$lte": end_date}
        },
        sort={"usage_date": -1},
        limit=limit
    )

    results = []
    for doc in cursor:
        results.append({
            'org_id': doc.get('org_id', ''),
            'usage_date': doc.get('usage_date', ''),
            'service': doc.get('service', ''),
            'total_cost_usd': float(doc.get('total_cost_usd', 0)),
            'total_requests': int(doc.get('total_requests', 0)),
            'cpu_hours': float(doc.get('cpu_hours', 0)),
            'storage_gb_hours': float(doc.get('storage_gb_hours', 0))
        })

    # Print formatted table
    table_data = [
        [r['org_id'], r['usage_date'], r['service'], 
         f"${r['total_cost_usd']:,.2f}", f"{r['total_requests']:,}",
         f"{r['cpu_hours']:,.2f}", f"{r['storage_gb_hours']:,.2f}"]
        for r in results
    ]
    headers = ["Org ID", "Date", "Service", "Cost USD", "Requests", "CPU Hours", "Storage GB-Hrs"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

    logger.info(f"✓ Retrieved {len(results)} rows")
    logger.info(f"Business insight: Daily service-level cost breakdown for org {org_id} from {start_date} to {end_date}")

    return results


def query_2_top_n_services_by_cost_14days(db, org_id=None, top_n=5):
    """
    Query 2: Top-N servicios por costo acumulado en los últimos 14 días para una organización.
    
    TP Requirement: "Top-N servicios por costo acumulado en los últimos 14 días para una organización."

    Find the most expensive services for an organization in the last 14 days.

    Use case: Identify which services are driving the highest costs to optimize spend.
    
    CQL Equivalent:
    SELECT org_id, service, total_cost_usd 
    FROM org_service_costs 
    WHERE org_id = ? AND window_days = 14
    ORDER BY total_cost_usd DESC
    LIMIT ?;
    """
    # Get a valid org_id if not provided
    if org_id is None:
        org_id = get_sample_org_id(db)
        if org_id is None:
            logger.error("No data found in collections")
            return []
    
    # Fixed to 14 days as per TP requirement
    window_days = 14

    logger.info("")
    logger.info("=" * 100)
    logger.info("QUERY 2: Top-N servicios por costo acumulado en los últimos 14 días")
    logger.info("=" * 100)
    logger.info(f"Parameters: org_id={org_id}, window_days={window_days}, top_n={top_n}")
    logger.info("")
    logger.info("CQL Equivalent:")
    logger.info(f"  SELECT org_id, service, total_cost_usd")
    logger.info(f"  FROM org_service_costs")
    logger.info(f"  WHERE org_id = '{org_id}' AND window_days = {window_days}")
    logger.info(f"  ORDER BY total_cost_usd DESC LIMIT {top_n};")
    logger.info("")

    collection = db.get_collection("org_service_costs")

    # Try 14 days first, fall back to 7 or 30 if no data
    cursor = collection.find(
        filter={"org_id": org_id, "window_days": window_days},
        sort={"total_cost_usd": -1},
        limit=top_n
    )

    results = []
    for doc in cursor:
        results.append({
            'org_id': doc.get('org_id', ''),
            'window_days': doc.get('window_days', 0),
            'service': doc.get('service', ''),
            'total_cost_usd': float(doc.get('total_cost_usd', 0))
        })
    
    # If no 14-day data, try other windows
    if not results:
        for fallback_window in [7, 30, 90]:
            cursor = collection.find(
                filter={"org_id": org_id, "window_days": fallback_window},
                sort={"total_cost_usd": -1},
                limit=top_n
            )
            for doc in cursor:
                results.append({
                    'org_id': doc.get('org_id', ''),
                    'window_days': doc.get('window_days', 0),
                    'service': doc.get('service', ''),
                    'total_cost_usd': float(doc.get('total_cost_usd', 0))
                })
            if results:
                logger.info(f"Note: Using {fallback_window}-day window (14-day data not available)")
                break

    # Print formatted table
    table_data = [
        [r['org_id'], f"{r['window_days']} days", r['service'], f"${r['total_cost_usd']:,.2f}"]
        for r in results
    ]
    headers = ["Org ID", "Time Window", "Service", "Total Cost USD"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

    logger.info(f"✓ Retrieved {len(results)} services")
    logger.info(f"Business insight: Top {top_n} most expensive services for org {org_id}")

    return results


def query_3_critical_tickets_sla_breach_30days(db, severity="critical", limit=30):
    """
    Query 3: Evolución de tickets críticos y tasa de SLA breach por día (últimos 30 días).
    
    TP Requirement: "Evolución de tickets críticos y tasa de SLA breach por día (últimos 30 días)."

    Monitor critical support tickets and SLA breach rates evolution over the last 30 days.

    Use case: Support operations tracking critical ticket volume and SLA compliance trends.
    
    CQL Equivalent:
    SELECT date, severity, total_tickets, sla_breach_rate, avg_resolution_hours
    FROM tickets_critical_daily
    WHERE severity = 'critical'
    ORDER BY date DESC
    LIMIT 30;
    """
    logger.info("")
    logger.info("=" * 100)
    logger.info("QUERY 3: Evolución de tickets críticos y tasa de SLA breach (últimos 30 días)")
    logger.info("=" * 100)
    logger.info(f"Parameters: severity={severity}, limit={limit} days")
    logger.info("")
    logger.info("CQL Equivalent:")
    logger.info(f"  SELECT date, severity, total_tickets, sla_breach_rate, avg_resolution_hours")
    logger.info(f"  FROM tickets_critical_daily")
    logger.info(f"  WHERE severity = '{severity}'")
    logger.info(f"  ORDER BY date DESC LIMIT {limit};")
    logger.info("")

    collection = db.get_collection("tickets_critical_daily")

    # Find all critical tickets, sorted by date descending, limited to 30 days
    cursor = collection.find(
        filter={"severity": severity},
        sort={"date": -1},
        limit=limit
    )

    results = []
    for doc in cursor:
        results.append({
            'date': doc.get('date', ''),
            'severity': doc.get('severity', ''),
            'total_tickets': int(doc.get('total_tickets', 0)),
            'sla_breach_rate': float(doc.get('sla_breach_rate', 0)),
            'avg_resolution_hours': float(doc.get('avg_resolution_hours', 0))
        })

    # Print formatted table
    table_data = [
        [r['date'], r['severity'], r['total_tickets'], 
         f"{r['sla_breach_rate'] * 100:.1f}%", f"{r['avg_resolution_hours']:.1f}h"]
        for r in results
    ]
    headers = ["Date", "Severity", "Total Tickets", "SLA Breach Rate", "Avg Resolution"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

    # Calculate summary stats
    if results:
        total_tickets = sum(r['total_tickets'] for r in results)
        avg_breach_rate = sum(r['sla_breach_rate'] for r in results) / len(results)
        logger.info(f"✓ Retrieved {len(results)} days of data")
        logger.info(f"Summary: {total_tickets} total tickets, {avg_breach_rate*100:.1f}% avg SLA breach rate")
    else:
        logger.info("✓ No critical tickets found in the last 30 days")

    logger.info(f"Business insight: {severity} ticket evolution - monitoring support operations health over time")

    return results


def query_4_monthly_revenue_normalized_usd(db, org_id=None, limit=12):
    """
    Query 4: Revenue mensual con créditos/impuestos aplicados (normalizado a USD).
    
    TP Requirement: "Revenue mensual con créditos/impuestos aplicados (normalizado a USD)."

    Track monthly revenue trends for an organization with full financial breakdown.
    All amounts are normalized to USD.

    Use case: Finance team monitoring revenue trends and billing metrics.
    
    CQL Equivalent:
    SELECT org_id, year_month, org_name, total_billed_usd, total_credits_usd, 
           total_taxes_usd, net_revenue, invoice_count
    FROM revenue_monthly
    WHERE org_id = ?
    ORDER BY year_month DESC
    LIMIT ?;
    """
    # Get a valid org_id if not provided
    if org_id is None:
        org_id = get_sample_org_id(db)
        if org_id is None:
            logger.error("No data found in collections")
            return []

    logger.info("")
    logger.info("=" * 100)
    logger.info("QUERY 4: Revenue mensual con créditos/impuestos (normalizado a USD)")
    logger.info("=" * 100)
    logger.info(f"Parameters: org_id={org_id}, limit={limit} months")
    logger.info("")
    logger.info("CQL Equivalent:")
    logger.info(f"  SELECT org_id, year_month, org_name, total_billed_usd, total_credits_usd,")
    logger.info(f"         total_taxes_usd, net_revenue, invoice_count")
    logger.info(f"  FROM revenue_monthly")
    logger.info(f"  WHERE org_id = '{org_id}'")
    logger.info(f"  ORDER BY year_month DESC LIMIT {limit};")
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
        results.append({
            'org_id': doc.get('org_id', ''),
            'year_month': doc.get('year_month', ''),
            'org_name': doc.get('org_name', 'N/A'),
            'total_billed_usd': float(doc.get('total_billed_usd', 0)),
            'total_credits_usd': float(doc.get('total_credits_usd', 0)),
            'total_taxes_usd': float(doc.get('total_taxes_usd', 0)),
            'net_revenue': float(doc.get('net_revenue', 0)),
            'invoice_count': int(doc.get('invoice_count', 0))
        })

    # Print formatted table
    table_data = [
        [r['org_id'], r['year_month'], r['org_name'][:20] if r['org_name'] else 'N/A',
         f"${r['total_billed_usd']:,.2f}", f"${r['total_credits_usd']:,.2f}",
         f"${r['total_taxes_usd']:,.2f}", f"${r['net_revenue']:,.2f}", r['invoice_count']]
        for r in results
    ]
    headers = ["Org ID", "Month", "Org Name", "Billed USD", "Credits USD", "Taxes USD", "Net Revenue", "Invoices"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

    # Calculate totals
    if results:
        total_billed = sum(r['total_billed_usd'] for r in results)
        total_credits = sum(r['total_credits_usd'] for r in results)
        total_taxes = sum(r['total_taxes_usd'] for r in results)
        total_net = sum(r['net_revenue'] for r in results)
        logger.info(f"✓ Retrieved {len(results)} months")
        logger.info(f"Totals: Billed=${total_billed:,.2f}, Credits=${total_credits:,.2f}, Taxes=${total_taxes:,.2f}, Net=${total_net:,.2f}")
    
    logger.info(f"Business insight: Monthly revenue trend for {org_id} - all values normalized to USD")

    return results


def query_5_genai_tokens_cost_by_day(db, org_id=None, limit=30):
    """
    Query 5: Tokens GenAI y costo estimado por día (si existen).
    
    TP Requirement: "Tokens GenAI y costo estimado por día (si existen)."

    Track GenAI/LLM token consumption and costs for an organization.
    Note: GenAI tokens only exist in schema_version 2 events (after 2025-07-18).

    Use case: Product team monitoring GenAI feature adoption and associated costs.
    
    CQL Equivalent:
    SELECT org_id, usage_date, total_genai_tokens, total_cost_usd, cost_per_million_tokens
    FROM genai_tokens_daily
    WHERE org_id = ?
    ORDER BY usage_date DESC
    LIMIT ?;
    """
    # Get a valid org_id if not provided
    if org_id is None:
        org_id = get_sample_org_id(db)
        if org_id is None:
            logger.error("No data found in collections")
            return []

    logger.info("")
    logger.info("=" * 100)
    logger.info("QUERY 5: Tokens GenAI y costo estimado por día")
    logger.info("=" * 100)
    logger.info(f"Parameters: org_id={org_id}, limit={limit}")
    logger.info("Note: GenAI tokens only exist in schema v2 events (after 2025-07-18)")
    logger.info("")
    logger.info("CQL Equivalent:")
    logger.info(f"  SELECT org_id, usage_date, total_genai_tokens, total_cost_usd, cost_per_million_tokens")
    logger.info(f"  FROM genai_tokens_daily")
    logger.info(f"  WHERE org_id = '{org_id}'")
    logger.info(f"  ORDER BY usage_date DESC LIMIT {limit};")
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
        results.append({
            'org_id': doc.get('org_id', ''),
            'usage_date': doc.get('usage_date', ''),
            'total_genai_tokens': int(doc.get('total_genai_tokens', 0)),
            'total_cost_usd': float(doc.get('total_cost_usd', 0)),
            'cost_per_million_tokens': float(doc.get('cost_per_million_tokens', 0))
        })

    if results:
        # Print formatted table
        table_data = [
            [r['org_id'], r['usage_date'], f"{r['total_genai_tokens']:,}",
             f"${r['total_cost_usd']:,.2f}", f"${r['cost_per_million_tokens']:,.2f}/M"]
            for r in results
        ]
        headers = ["Org ID", "Date", "Total Tokens", "Cost USD", "Cost per Million"]
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

        # Calculate totals
        total_tokens = sum(r['total_genai_tokens'] for r in results)
        total_cost = sum(r['total_cost_usd'] for r in results)
        logger.info(f"✓ Retrieved {len(results)} days of GenAI usage")
        logger.info(f"Totals: {total_tokens:,} tokens, ${total_cost:,.2f} cost")
    else:
        logger.info("⚠ No GenAI token data found for this organization")
        logger.info("  This is expected if data only contains schema v1 events (before 2025-07-18)")
        print("No GenAI data available")

    logger.info(f"Business insight: GenAI token consumption for {org_id} - tracking AI feature usage and costs")

    return results


def query_6_cost_anomalies(db, severity="critical", limit=10):
    """
    Query 6: Cost anomalies by severity

    Show detected cost anomalies with severity classification.
    Demonstrates 3-method anomaly detection (z-score, MAD, percentile).

    Use case: FinOps team investigating unusual spending patterns.
    """
    logger.info("")
    logger.info("=" * 100)
    logger.info("QUERY 6: Cost Anomalies by Severity")
    logger.info("=" * 100)
    logger.info(f"Parameters: severity={severity}, limit={limit}")
    logger.info("")

    collection = db.get_collection("cost_anomalies")

    cursor = collection.find(
        filter={"severity": severity},
        sort={"max_cost_spike": -1},
        limit=limit
    )

    results = []
    for doc in cursor:
        results.append([
            doc.get('org_id', ''),
            doc.get('usage_date', ''),
            doc.get('service', ''),
            doc.get('severity', ''),
            f"${float(doc.get('total_anomalous_cost', 0)):,.2f}",
            f"${float(doc.get('max_cost_spike', 0)):,.2f}",
            int(doc.get('zscore_detections', 0)),
            int(doc.get('mad_detections', 0)),
            int(doc.get('percentile_detections', 0))
        ])

    headers = ["Org ID", "Date", "Service", "Severity", "Total Cost", "Max Spike", "Z-Score", "MAD", "Percentile"]
    print(tabulate(results, headers=headers, tablefmt="grid"))

    logger.info(f"Retrieved {len(results)} anomaly records")
    logger.info(f"Business insight: {severity} cost anomalies detected using 3 statistical methods")

    return results


def run_all_queries(db):
    """Run all 5 required demo queries with sample parameters."""

    logger.info("")
    logger.info("=" * 100)
    logger.info("RUNNING ALL 5 REQUIRED DEMO QUERIES")
    logger.info("=" * 100)
    logger.info("")
    logger.info("Required Queries (from TP specification):")
    logger.info("  1. Costos y requests diarios por org y servicio en un rango de fechas.")
    logger.info("  2. Top-N servicios por costo acumulado en los últimos 14 días para una organización.")
    logger.info("  3. Evolución de tickets críticos y tasa de SLA breach por día (últimos 30 días).")
    logger.info("  4. Revenue mensual con créditos/impuestos aplicados (normalizado a USD).")
    logger.info("  5. Tokens GenAI y costo estimado por día (si existen).")
    logger.info("")

    # Get a valid org_id from the data
    sample_org_id = get_sample_org_id(db)

    if sample_org_id:
        logger.info(f"Using sample org_id: {sample_org_id}")
    else:
        logger.warning("No org_id found in data - some queries may return 0 rows")

    queries = [
        ("Query 1: Costos y requests diarios (rango fechas)", 
         lambda: query_1_daily_costs_by_org_service_daterange(db, org_id=sample_org_id, limit=20)),
        ("Query 2: Top-N servicios por costo (14 días)", 
         lambda: query_2_top_n_services_by_cost_14days(db, org_id=sample_org_id, top_n=5)),
        ("Query 3: Tickets críticos + SLA breach (30 días)", 
         lambda: query_3_critical_tickets_sla_breach_30days(db, severity="critical", limit=30)),
        ("Query 4: Revenue mensual (USD normalizado)", 
         lambda: query_4_monthly_revenue_normalized_usd(db, org_id=sample_org_id, limit=12)),
        ("Query 5: Tokens GenAI y costo por día", 
         lambda: query_5_genai_tokens_cost_by_day(db, org_id=sample_org_id, limit=30))
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
