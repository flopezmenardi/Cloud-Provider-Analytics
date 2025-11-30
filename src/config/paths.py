"""
Configuration for data lake paths
"""
import os
from pathlib import Path

# Base paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATALAKE_ROOT = PROJECT_ROOT / "datalake"

# Landing zone paths
LANDING_ROOT = DATALAKE_ROOT / "landing"
LANDING_USERS = LANDING_ROOT / "users.csv"
LANDING_CUSTOMERS_ORGS = LANDING_ROOT / "customers_orgs.csv"
LANDING_RESOURCES = LANDING_ROOT / "resources.csv"
LANDING_BILLING_MONTHLY = LANDING_ROOT / "billing_monthly.csv"
LANDING_SUPPORT_TICKETS = LANDING_ROOT / "support_tickets.csv"
LANDING_MARKETING_TOUCHES = LANDING_ROOT / "marketing_touches.csv"
LANDING_NPS_SURVEYS = LANDING_ROOT / "nps_surveys.csv"
LANDING_USAGE_EVENTS_STREAM = LANDING_ROOT / "usage_events_stream"

# Bronze zone paths
BRONZE_ROOT = DATALAKE_ROOT / "bronze"
BRONZE_USERS = BRONZE_ROOT / "users"
BRONZE_CUSTOMERS_ORGS = BRONZE_ROOT / "customers_orgs"
BRONZE_RESOURCES = BRONZE_ROOT / "resources"
BRONZE_BILLING_MONTHLY = BRONZE_ROOT / "billing_monthly"
BRONZE_SUPPORT_TICKETS = BRONZE_ROOT / "support_tickets"
BRONZE_MARKETING_TOUCHES = BRONZE_ROOT / "marketing_touches"
BRONZE_NPS_SURVEYS = BRONZE_ROOT / "nps_surveys"
BRONZE_USAGE_EVENTS = BRONZE_ROOT / "usage_events"
BRONZE_CHECKPOINT = BRONZE_ROOT / "checkpoints" / "usage_events"

# Silver zone paths
SILVER_ROOT = DATALAKE_ROOT / "silver"
SILVER_QUARANTINE = DATALAKE_ROOT / "silver" / "quarantine"

# Gold zone paths
GOLD_ROOT = DATALAKE_ROOT / "gold"

# Evidence and state paths (for lineage tracking and idempotency)
EVIDENCE_ROOT = DATALAKE_ROOT / "evidence"
STATE_ROOT = DATALAKE_ROOT / "state"

# Speed layer paths
SPEED_ROOT = DATALAKE_ROOT / "speed"

# Helper functions for Silver and Quarantine paths
def get_silver_path(source_name: str) -> Path:
    """Get Silver path for a source"""
    return SILVER_ROOT / source_name

def get_quarantine_path(source_name: str) -> Path:
    """Get Quarantine path for a source"""
    return SILVER_QUARANTINE / source_name

def get_gold_path(mart_name: str) -> Path:
    """Get Gold path for a mart"""
    return GOLD_ROOT / mart_name

# Ensure bronze directories exist
for path in [
    BRONZE_USERS, BRONZE_CUSTOMERS_ORGS, BRONZE_RESOURCES,
    BRONZE_BILLING_MONTHLY, BRONZE_SUPPORT_TICKETS,
    BRONZE_MARKETING_TOUCHES, BRONZE_NPS_SURVEYS,
    BRONZE_USAGE_EVENTS, BRONZE_CHECKPOINT
]:
    path.mkdir(parents=True, exist_ok=True)

# Ensure all root directories exist
SILVER_ROOT.mkdir(parents=True, exist_ok=True)
SILVER_QUARANTINE.mkdir(parents=True, exist_ok=True)
GOLD_ROOT.mkdir(parents=True, exist_ok=True)
EVIDENCE_ROOT.mkdir(parents=True, exist_ok=True)
STATE_ROOT.mkdir(parents=True, exist_ok=True)
SPEED_ROOT.mkdir(parents=True, exist_ok=True)
