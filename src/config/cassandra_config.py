"""
Cassandra/AstraDB Configuration

Setup Instructions:
1. Create free AstraDB account at: https://astra.datastax.com/
2. Create a new database (name: cloud_analytics)
3. Download the secure connect bundle (secure-connect-cloud_analytics.zip)
4. Create an application token (Client ID + Client Secret)
5. Update the paths and credentials below
"""

import os
from pathlib import Path

# AstraDB Configuration
ASTRADB_CONFIG = {
    # Secure Connect Bundle path
    # Download from: AstraDB Dashboard > Your Database > Connect > Driver
    "secure_connect_bundle": os.getenv(
        "ASTRADB_SECURE_CONNECT_BUNDLE",
        str(Path.home() / "secure-connect-cloud_analytics.zip")
    ),

    # Application Token credentials
    # Create at: AstraDB Dashboard > Your Database > Settings > Application Tokens
    "client_id": os.getenv(
        "ASTRADB_CLIENT_ID",
        "REPLACE_WITH_YOUR_CLIENT_ID"  # ← UPDATE THIS
    ),

    "client_secret": os.getenv(
        "ASTRADB_CLIENT_SECRET",
        "REPLACE_WITH_YOUR_CLIENT_SECRET"  # ← UPDATE THIS
    ),

    # Keyspace name
    "keyspace": "cloud_analytics",
}

# Local Cassandra Configuration (fallback)
LOCAL_CASSANDRA_CONFIG = {
    "contact_points": ["127.0.0.1"],
    "port": 9042,
    "keyspace": "cloud_analytics",
}

# Mode selection: "astradb" or "local"
CASSANDRA_MODE = os.getenv("CASSANDRA_MODE", "astradb")


def get_cassandra_config():
    """
    Get Cassandra configuration based on mode.

    Returns:
        dict: Configuration dictionary for chosen mode
    """
    if CASSANDRA_MODE == "astradb":
        return {"mode": "astradb", **ASTRADB_CONFIG}
    else:
        return {"mode": "local", **LOCAL_CASSANDRA_CONFIG}


def validate_astradb_config():
    """
    Validate that AstraDB configuration is properly set.

    Raises:
        ValueError: If configuration is incomplete
    """
    if ASTRADB_CONFIG["client_id"] == "REPLACE_WITH_YOUR_CLIENT_ID":
        raise ValueError(
            "AstraDB Client ID not configured. Please update src/config/cassandra_config.py or set ASTRADB_CLIENT_ID environment variable."
        )

    if ASTRADB_CONFIG["client_secret"] == "REPLACE_WITH_YOUR_CLIENT_SECRET":
        raise ValueError(
            "AstraDB Client Secret not configured. Please update src/config/cassandra_config.py or set ASTRADB_CLIENT_SECRET environment variable."
        )

    bundle_path = Path(ASTRADB_CONFIG["secure_connect_bundle"])
    if not bundle_path.exists():
        raise ValueError(
            f"Secure connect bundle not found at: {bundle_path}. "
            f"Please download from AstraDB Dashboard and update the path."
        )


# Quick setup check
if __name__ == "__main__":
    print("Cassandra Configuration Check")
    print("=" * 60)
    print(f"Mode: {CASSANDRA_MODE}")
    print()

    if CASSANDRA_MODE == "astradb":
        print("AstraDB Configuration:")
        print(f"  Keyspace: {ASTRADB_CONFIG['keyspace']}")
        print(f"  Secure Connect Bundle: {ASTRADB_CONFIG['secure_connect_bundle']}")
        print(f"  Client ID: {ASTRADB_CONFIG['client_id'][:20]}..." if ASTRADB_CONFIG['client_id'] != "REPLACE_WITH_YOUR_CLIENT_ID" else "  Client ID: NOT CONFIGURED")
        print(f"  Client Secret: {'***' if ASTRADB_CONFIG['client_secret'] != 'REPLACE_WITH_YOUR_CLIENT_SECRET' else 'NOT CONFIGURED'}")

        try:
            validate_astradb_config()
            print()
            print("✓ AstraDB configuration is valid")
        except ValueError as e:
            print()
            print(f"✗ Configuration error: {e}")
    else:
        print("Local Cassandra Configuration:")
        print(f"  Contact Points: {LOCAL_CASSANDRA_CONFIG['contact_points']}")
        print(f"  Port: {LOCAL_CASSANDRA_CONFIG['port']}")
        print(f"  Keyspace: {LOCAL_CASSANDRA_CONFIG['keyspace']}")
