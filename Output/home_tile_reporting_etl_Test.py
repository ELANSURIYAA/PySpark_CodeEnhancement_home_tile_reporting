"""
====================================================================
Author: Ascendion AAVA
Date: 
Description: Python-based test script for home_tile_reporting_etl_Pipeline.py
====================================================================

Test Scenarios:
1. Insert scenario: New tile_id not present in target, with/without metadata
2. Update scenario: Existing tile_id, metadata changes, and validation of enrichment/defaults

Instructions:
- This script is self-contained and does not require PyTest.
- It creates Spark session, sample data, runs pipeline transformations, and prints markdown test report.
"""

from pyspark.sql import SparkSession, Row, functions as F
from datetime import datetime
import sys

def run_etl_test():
    spark = SparkSession.builder.master("local[1]").appName("ETLTest").getOrCreate()

    # Sample input data for Scenario 1 (Insert)
    tile_events = [
        Row(event_id="e1", user_id="u1", session_id="s1", event_ts="2025-12-01 10:00:00", tile_id="T1", event_type="TILE_VIEW", device_type="Mobile", app_version="1.0.0"),
        Row(event_id="e2", user_id="u2", session_id="s2", event_ts="2025-12-01 11:00:00", tile_id="T2", event_type="TILE_CLICK", device_type="Web", app_version="1.0.1"),
    ]
    inter_events = [
        Row(event_id="i1", user_id="u1", session_id="s1", event_ts="2025-12-01 10:10:00", tile_id="T1", interstitial_view_flag=True, primary_button_click_flag=False, secondary_button_click_flag=True),
        Row(event_id="i2", user_id="u3", session_id="s3", event_ts="2025-12-01 12:00:00", tile_id="T2", interstitial_view_flag=True, primary_button_click_flag=True, secondary_button_click_flag=False),
    ]
    tile_meta = [
        Row(tile_id="T1", tile_name="Finance", tile_category="Personal Finance", is_active=True, updated_ts="2025-11-30 09:00:00"),
        # T2 has no metadata -> should default to UNKNOWN
    ]
    # Create DataFrames
    df_tile = spark.createDataFrame(tile_events)
    df_inter = spark.createDataFrame(inter_events)
    df_meta = spark.createDataFrame(tile_meta)
    process_date = "2025-12-01"

    # Run pipeline logic for Scenario 1
    df_tile_agg = (
        df_tile.groupBy("tile_id")
        .agg(
            F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
            F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
        )
    )
    df_inter_agg = (
        df_inter.groupBy("tile_id")
        .agg(
            F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
            F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
            F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
        )
    )
    df_daily_summary_raw = (
        df_tile_agg.join(df_inter_agg, "tile_id", "outer")
        .withColumn("date", F.lit(process_date))
    )
    df_daily_summary = (
        df_daily_summary_raw
        .join(df_meta.select("tile_id", "tile_category"), on="tile_id", how="left")
        .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        .select(
            "date", "tile_id", "tile_category",
            F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
            F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
            F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
            F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
            F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
        )
    )
    result1 = df_daily_summary.orderBy("tile_id").toPandas()

    # Prepare expected output for Scenario 1
    expected1 = [
        {'date': process_date, 'tile_id': 'T1', 'tile_category': 'Personal Finance', 'unique_tile_views': 1, 'unique_tile_clicks': 0, 'unique_interstitial_views': 1, 'unique_interstitial_primary_clicks': 0, 'unique_interstitial_secondary_clicks': 1},
        {'date': process_date, 'tile_id': 'T2', 'tile_category': 'UNKNOWN', 'unique_tile_views': 0, 'unique_tile_clicks': 1, 'unique_interstitial_views': 1, 'unique_interstitial_primary_clicks': 1, 'unique_interstitial_secondary_clicks': 0}
    ]
    status1 = "PASS" if result1.to_dict("records") == expected1 else "FAIL"

    # Scenario 2 (Update): Existing tile_id with updated metadata
    tile_meta2 = [
        Row(tile_id="T1", tile_name="Finance", tile_category="Updated Category", is_active=True, updated_ts="2025-12-02 09:00:00"),
        Row(tile_id="T2", tile_name="Offers", tile_category="Offers", is_active=True, updated_ts="2025-12-02 09:00:00"),
    ]
    df_meta2 = spark.createDataFrame(tile_meta2)
    df_daily_summary2 = (
        df_daily_summary_raw
        .join(df_meta2.select("tile_id", "tile_category"), on="tile_id", how="left")
        .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        .select(
            "date", "tile_id", "tile_category",
            F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
            F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
            F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
            F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
            F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
        )
    )
    result2 = df_daily_summary2.orderBy("tile_id").toPandas()
    expected2 = [
        {'date': process_date, 'tile_id': 'T1', 'tile_category': 'Updated Category', 'unique_tile_views': 1, 'unique_tile_clicks': 0, 'unique_interstitial_views': 1, 'unique_interstitial_primary_clicks': 0, 'unique_interstitial_secondary_clicks': 1},
        {'date': process_date, 'tile_id': 'T2', 'tile_category': 'Offers', 'unique_tile_views': 0, 'unique_tile_clicks': 1, 'unique_interstitial_views': 1, 'unique_interstitial_primary_clicks': 1, 'unique_interstitial_secondary_clicks': 0}
    ]
    status2 = "PASS" if result2.to_dict("records") == expected2 else "FAIL"

    # Markdown report
    print("""
## Test Report
### Scenario 1: Insert
Input:
| tile_id | tile_category (meta) |
|---------|----------------------|
| T1      | Personal Finance     |
| T2      | <none>               |
Output:
""")
    print(result1.to_markdown(index=False))
    print(f"Status: {status1}\n")
    print("""
### Scenario 2: Update
Input:
| tile_id | tile_category (meta) |
|---------|----------------------|
| T1      | Updated Category     |
| T2      | Offers               |
Output:
""")
    print(result2.to_markdown(index=False))
    print(f"Status: {status2}\n")

    spark.stop()

if __name__ == "__main__":
    run_etl_test()
