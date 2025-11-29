"""
AstraDB Configuration using Data API
Loads credentials from .env file for security
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# AstraDB Data API Configuration
ASTRADB_CONFIG = {
    "token": os.getenv("ASTRADB_TOKEN"),
    "api_endpoint": os.getenv(
        "ASTRADB_API_ENDPOINT",
        "https://d323e707-167e-4364-bd05-7aff587c5012-us-east-2.apps.astra.datastax.com"
    ),
    "namespace": os.getenv("ASTRADB_NAMESPACE", "cloud_analytics"),
}


def get_astradb_client():
    """Get AstraDB client using Data API"""
    from astrapy import DataAPIClient

    validate_astradb_config()

    client = DataAPIClient(ASTRADB_CONFIG["token"])
    db = client.get_database_by_api_endpoint(
        ASTRADB_CONFIG["api_endpoint"]
    )
    return db


def validate_astradb_config():
    """Validate configuration"""
    if not ASTRADB_CONFIG["token"]:
        raise ValueError(
            "ASTRADB_TOKEN not configured. "
            "Please create a .env file in project root with your token. "
            "See ASTRADB_SETUP.md for instructions."
        )

    if not ASTRADB_CONFIG["api_endpoint"]:
        raise ValueError("ASTRADB_API_ENDPOINT not configured in .env")

    return True


# Validation check when module loads
if __name__ == "__main__":
    print("AstraDB Configuration Check")
    print("=" * 60)
    print(f"API Endpoint: {ASTRADB_CONFIG['api_endpoint']}")
    print(f"Namespace: {ASTRADB_CONFIG['namespace']}")
    print(f"Token: {'✓ Configured' if ASTRADB_CONFIG['token'] else '✗ NOT SET'}")
    print()

    try:
        validate_astradb_config()
        print("✓ Configuration is valid!")
    except ValueError as e:
        print(f"✗ Configuration error: {e}")
