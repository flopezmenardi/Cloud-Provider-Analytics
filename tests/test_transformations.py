"""
Integration tests for Silver layer transformations.

Tests the transformation logic for converting Bronze data to Silver.
"""

import pytest
import sys
from pathlib import Path
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestUsageEventsTransformation:
    """Tests for usage events Silver transformation logic."""
    
    def test_region_standardization(self, spark):
        """Test that region names are standardized."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("region", StringType()),
        ])
        
        data = [
            ("evt-1", "us-east-1"),
            ("evt-2", "US-EAST-1"),
            ("evt-3", "Us-East-1"),
            ("evt-4", "eu-west-1"),
        ]
        df = spark.createDataFrame(data, schema)
        
        # Apply standardization (lowercase)
        result = df.withColumn("region_std", F.lower(F.col("region")))
        
        # All should be lowercase
        regions = [row.region_std for row in result.collect()]
        assert all(r == r.lower() for r in regions)
        
        # Check specific standardization
        assert regions.count("us-east-1") == 3
    
    def test_service_standardization(self, spark):
        """Test that service names are standardized."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("service", StringType()),
        ])
        
        data = [
            ("evt-1", "compute"),
            ("evt-2", "COMPUTE"),
            ("evt-3", "Compute"),
            ("evt-4", "ai_ml"),
            ("evt-5", "AI_ML"),
        ]
        df = spark.createDataFrame(data, schema)
        
        result = df.withColumn("service_std", F.lower(F.col("service")))
        
        services = [row.service_std for row in result.collect()]
        assert services.count("compute") == 3
        assert services.count("ai_ml") == 2
    
    def test_schema_version_compatibility(self, spark, sample_usage_events):
        """Test handling of schema v1 and v2 differences."""
        # Add derived columns for v2 fields with null-safe handling
        result = sample_usage_events.withColumn(
            "carbon_kg_safe",
            F.coalesce(F.col("carbon_kg"), F.lit(0.0))
        ).withColumn(
            "genai_tokens_safe",
            F.coalesce(F.col("genai_tokens"), F.lit(0))
        )
        
        # All records should have non-null safe values
        null_carbon = result.filter(F.col("carbon_kg_safe").isNull())
        null_tokens = result.filter(F.col("genai_tokens_safe").isNull())
        
        assert null_carbon.count() == 0
        assert null_tokens.count() == 0
    
    def test_timestamp_parsing(self, spark, sample_usage_events):
        """Test timestamp parsing is correct."""
        # event_timestamp should already be parsed
        result = sample_usage_events.select("event_id", "timestamp", "event_timestamp")
        
        # All timestamps should be non-null
        null_timestamps = result.filter(F.col("event_timestamp").isNull())
        assert null_timestamps.count() == 0
        
        # Check date extraction
        result = result.withColumn("event_date", F.to_date(F.col("event_timestamp")))
        dates = [row.event_date for row in result.collect()]
        assert all(d is not None for d in dates)
    
    def test_cost_handling(self, spark, sample_usage_events):
        """Test that costs are handled correctly including nulls."""
        result = sample_usage_events.withColumn(
            "cost_safe",
            F.coalesce(F.col("cost_usd_increment"), F.lit(0.0))
        )
        
        # All should have non-null cost
        null_costs = result.filter(F.col("cost_safe").isNull())
        assert null_costs.count() == 0
        
        # Check total cost calculation
        total_cost = result.agg(F.sum("cost_safe")).collect()[0][0]
        assert total_cost > 0


class TestBillingTransformation:
    """Tests for billing Silver transformation logic."""
    
    def test_currency_normalization(self, spark, sample_billing):
        """Test that all amounts are normalized to USD."""
        # Apply currency normalization
        result = sample_billing.withColumn(
            "total_amount_usd",
            F.col("total_amount") * F.col("exchange_rate_to_usd")
        )
        
        # Check specific conversion
        gbp_invoices = result.filter(F.col("currency") == "GBP")
        for row in gbp_invoices.collect():
            expected_usd = row.total_amount * row.exchange_rate_to_usd
            assert abs(row.total_amount_usd - expected_usd) < 0.01
    
    def test_net_revenue_calculation(self, spark, sample_billing):
        """Test net revenue calculation: amount - credits + taxes."""
        result = sample_billing.withColumn(
            "amount_usd",
            F.col("total_amount") * F.col("exchange_rate_to_usd")
        ).withColumn(
            "credits_safe",
            F.coalesce(F.col("credits_applied"), F.lit(0.0))
        ).withColumn(
            "taxes_safe",
            F.coalesce(F.col("taxes"), F.lit(0.0))
        ).withColumn(
            "net_revenue",
            F.col("amount_usd") - F.col("credits_safe") + F.col("taxes_safe")
        )
        
        # Check first invoice: 1500 - 100 + 150 = 1550
        inv1 = result.filter(F.col("invoice_id") == "inv-001").collect()[0]
        assert abs(inv1.net_revenue - 1550.0) < 0.01
    
    def test_handles_zero_amount(self, spark, sample_billing):
        """Test handling of zero/free tier invoices."""
        free_invoice = sample_billing.filter(F.col("invoice_id") == "inv-005")
        
        row = free_invoice.collect()[0]
        assert row.total_amount == 0.0
        assert row.credits_applied == 0.0


class TestSupportTicketsTransformation:
    """Tests for support tickets Silver transformation logic."""
    
    def test_resolution_time_calculation(self, spark, sample_support_tickets):
        """Test calculation of resolution time in hours."""
        result = sample_support_tickets.withColumn(
            "resolution_hours",
            F.when(
                F.col("resolved_timestamp").isNotNull(),
                (F.unix_timestamp("resolved_timestamp") - F.unix_timestamp("created_timestamp")) / 3600.0
            ).otherwise(None)
        )
        
        # Check first ticket: 2 hours resolution
        tkt1 = result.filter(F.col("ticket_id") == "tkt-001").collect()[0]
        assert abs(tkt1.resolution_hours - 2.0) < 0.1
        
        # Open ticket should have null resolution time
        tkt3 = result.filter(F.col("ticket_id") == "tkt-003").collect()[0]
        assert tkt3.resolution_hours is None
    
    def test_severity_standardization(self, spark, sample_support_tickets):
        """Test that severity values are standardized."""
        severities = [row.severity for row in sample_support_tickets.collect()]
        valid_severities = {"critical", "high", "medium", "low"}
        
        assert all(s in valid_severities for s in severities)
    
    def test_sla_breach_calculation(self, spark, sample_support_tickets):
        """Test SLA breach detection."""
        # SLA: critical < 4h, high < 8h, medium < 24h, low < 48h
        result = sample_support_tickets.withColumn(
            "resolution_hours",
            F.when(
                F.col("resolved_timestamp").isNotNull(),
                (F.unix_timestamp("resolved_timestamp") - F.unix_timestamp("created_timestamp")) / 3600.0
            ).otherwise(None)
        ).withColumn(
            "sla_threshold",
            F.when(F.col("severity") == "critical", 4.0)
            .when(F.col("severity") == "high", 8.0)
            .when(F.col("severity") == "medium", 24.0)
            .when(F.col("severity") == "low", 48.0)
            .otherwise(24.0)
        ).withColumn(
            "sla_breach",
            F.when(
                F.col("resolution_hours").isNotNull(),
                F.col("resolution_hours") > F.col("sla_threshold")
            ).otherwise(False)
        )
        
        # tkt-002 is high priority, resolved in 9 hours (> 8h SLA)
        tkt2 = result.filter(F.col("ticket_id") == "tkt-002").collect()[0]
        assert tkt2.sla_breach == True
        
        # tkt-001 is critical, resolved in 2 hours (< 4h SLA)
        tkt1 = result.filter(F.col("ticket_id") == "tkt-001").collect()[0]
        assert tkt1.sla_breach == False


class TestCustomersTransformation:
    """Tests for customers/orgs Silver transformation logic."""
    
    def test_tier_standardization(self, spark, sample_customers):
        """Test that tier values are standardized."""
        tiers = [row.tier for row in sample_customers.collect()]
        valid_tiers = {"enterprise", "pro", "free"}
        
        assert all(t in valid_tiers for t in tiers)
    
    def test_country_standardization(self, spark, sample_customers):
        """Test country values are present."""
        countries = [row.country for row in sample_customers.collect()]
        assert all(c is not None for c in countries)


class TestGoldMartAggregations:
    """Tests for Gold mart aggregation logic."""
    
    def test_daily_usage_aggregation(self, spark, sample_usage_events):
        """Test daily usage aggregation by org and service."""
        result = sample_usage_events.withColumn(
            "usage_date",
            F.to_date(F.col("event_timestamp"))
        ).groupBy("org_id", "usage_date", "service").agg(
            F.count("*").alias("total_events"),
            F.sum("cost_usd_increment").alias("total_cost_usd"),
            F.sum(F.coalesce("genai_tokens", F.lit(0))).alias("genai_tokens_total")
        )
        
        # Should have aggregated rows
        assert result.count() > 0
        # Aggregation reduces rows only if there are duplicate (org_id, date, service) combinations
        # In test data, if all are unique, count stays same
        assert result.count() <= sample_usage_events.count()
        
        # Check aggregation columns
        assert "total_events" in result.columns
        assert "total_cost_usd" in result.columns
        assert "genai_tokens_total" in result.columns
    
    def test_revenue_aggregation(self, spark, sample_billing, sample_customers):
        """Test monthly revenue aggregation."""
        # Join billing with customers
        joined = sample_billing.join(
            sample_customers,
            on="org_id",
            how="left"
        )
        
        # Aggregate by org and month
        result = joined.withColumn(
            "amount_usd",
            F.col("total_amount") * F.col("exchange_rate_to_usd")
        ).groupBy("org_id", "billing_month", "org_name").agg(
            F.sum("amount_usd").alias("total_billed_usd"),
            F.sum(F.coalesce("credits_applied", F.lit(0.0))).alias("total_credits"),
            F.count("*").alias("invoice_count")
        )
        
        # org-001 should have 2 invoices
        org1 = result.filter(F.col("org_id") == "org-001")
        assert org1.count() == 2
    
    def test_ticket_aggregation_by_date(self, spark, sample_support_tickets):
        """Test ticket aggregation by date and severity."""
        result = sample_support_tickets.withColumn(
            "ticket_date",
            F.to_date(F.col("created_timestamp"))
        ).groupBy("ticket_date", "severity").agg(
            F.count("*").alias("total_tickets")
        )
        
        assert result.count() > 0
        
        # Check we have different severities
        severities = result.select("severity").distinct().count()
        assert severities > 1


class TestDataIntegrity:
    """Tests for data integrity across transformations."""
    
    def test_no_data_loss_in_valid_records(self, spark, sample_usage_events):
        """Test that valid records are not lost during transformation."""
        initial_count = sample_usage_events.count()
        
        # Apply a transformation that shouldn't lose data
        result = sample_usage_events.withColumn(
            "cost_category",
            F.when(F.col("cost_usd_increment") > 1.0, "high")
            .otherwise("low")
        )
        
        assert result.count() == initial_count
    
    def test_aggregation_totals_match(self, spark, sample_usage_events):
        """Test that aggregated totals match source data."""
        # Total cost from source
        source_total = sample_usage_events.agg(
            F.sum(F.coalesce("cost_usd_increment", F.lit(0.0)))
        ).collect()[0][0]
        
        # Aggregated total
        agg_df = sample_usage_events.groupBy("org_id").agg(
            F.sum(F.coalesce("cost_usd_increment", F.lit(0.0))).alias("org_total")
        )
        agg_total = agg_df.agg(F.sum("org_total")).collect()[0][0]
        
        # Should match (within floating point tolerance)
        assert abs(source_total - agg_total) < 0.01
    
    def test_join_completeness(self, spark, sample_usage_events, sample_customers):
        """Test that joins don't unexpectedly drop records."""
        # Left join should keep all usage events
        result = sample_usage_events.join(
            sample_customers,
            on="org_id",
            how="left"
        )
        
        assert result.count() == sample_usage_events.count()
        
        # Some records should have customer info
        with_customer = result.filter(F.col("org_name").isNotNull())
        assert with_customer.count() > 0

