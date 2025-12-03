'''
====================================================================
Author: Ascendion AAVA
Date: 
Description: Python test script for home_tile_reporting_etl_Pipeline.py. Validates insert and update scenarios for tile_category enrichment.
====================================================================

CHANGE MANAGEMENT / REVISION HISTORY
-------------------------------------------------------------------------------
File Name       : home_tile_reporting_etl_Test.py
Author          : Ascendion AAVA
Created Date    : 
Last Modified   : <Auto-updated>
Version         : 2.0.0
Release         : R2 – Tile Category Enrichment Testing
-------------------------------------------------------------------------------

Functional Description:
    - Tests ETL logic for correct enrichment and update of tile_category
    - Scenario 1: Insert new records and validate output
    - Scenario 2: Update existing records and validate output
    - Prints markdown-formatted test report
-------------------------------------------------------------------------------
'''

import sys
from pyspark.sql import SparkSession, functions as F

# ------------------------------------------------------------------------------
# Helper function to simulate the ETL logic from Pipeline
# ------------------------------------------------------------------------------
def run_etl(df_tile, df_inter, df_tile_metadata, process_date):
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
    df_daily_summary = (
        df_tile_agg.join(df_inter_agg, "tile_id", "outer")
        .join(df_tile_metadata.select("tile_id", "tile_category"), "tile_id", "left")
        .withColumn("date", F.lit(process_date))
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
    return df_daily_summary

# ------------------------------------------------------------------------------
# Scenario 1: Insert
# ------------------------------------------------------------------------------
def scenario_insert(spark):
    process_date = "2025-12-01"
    df_tile = spark.createDataFrame([
        {"event_id": "e1", "user_id": "u1", "session_id": "s1", "event_ts": "2025-12-01 08:00:00", "tile_id": "TILE1", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
        {"event_id": "e2", "user_id": "u2", "session_id": "s2", "event_ts": "2025-12-01 08:01:00", "tile_id": "TILE1", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0"}
    ]).withColumn("event_ts", F.to_timestamp("event_ts"))
    df_inter = spark.createDataFrame([
        {"event_id": "i1", "user_id": "u1", "session_id": "s1", "event_ts": "2025-12-01 08:05:00", "tile_id": "TILE1", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True}
    ]).withColumn("event_ts", F.to_timestamp("event_ts"))
    df_tile_metadata = spark.createDataFrame([
        {"tile_id": "TILE1", "tile_name": "Payments", "tile_category": "Personal Finance", "is_active": True, "updated_ts": "2025-11-30 12:00:00"}
    ]).withColumn("updated_ts", F.to_timestamp("updated_ts"))
    df_out = run_etl(df_tile, df_inter, df_tile_metadata, process_date)
    return df_tile, df_inter, df_tile_metadata, df_out

# ------------------------------------------------------------------------------
# Scenario 2: Update
# ------------------------------------------------------------------------------
def scenario_update(spark):
    process_date = "2025-12-01"
    # Existing record for TILE1
    df_tile = spark.createDataFrame([
        {"event_id": "e1", "user_id": "u1", "session_id": "s1", "event_ts": "2025-12-01 08:00:00", "tile_id": "TILE1", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
        {"event_id": "e2", "user_id": "u1", "session_id": "s2", "event_ts": "2025-12-01 08:01:00", "tile_id": "TILE1", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0"}
    ]).withColumn("event_ts", F.to_timestamp("event_ts"))
    df_inter = spark.createDataFrame([
        {"event_id": "i1", "user_id": "u1", "session_id": "s1", "event_ts": "2025-12-01 08:05:00", "tile_id": "TILE1", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False}
    ]).withColumn("event_ts", F.to_timestamp("event_ts"))
    df_tile_metadata = spark.createDataFrame([
        {"tile_id": "TILE1", "tile_name": "Payments", "tile_category": "Personal Finance", "is_active": True, "updated_ts": "2025-11-30 12:00:00"}
    ]).withColumn("updated_ts", F.to_timestamp("updated_ts"))
    df_out = run_etl(df_tile, df_inter, df_tile_metadata, process_date)
    return df_tile, df_inter, df_tile_metadata, df_out

# ------------------------------------------------------------------------------
# Markdown Report Generator
# ------------------------------------------------------------------------------
def markdown_table(df, columns):
    rows = df.select(*columns).collect()
    header = "| " + " | ".join(columns) + " |"
    sep = "|" + "|".join(["----"] * len(columns)) + "|"
    lines = [header, sep]
    for row in rows:
        lines.append("| " + " | ".join([str(row[c]) for c in columns]) + " |")
    return "\n".join(lines)

def run_tests():
    spark = SparkSession.builder.master("local[1]").appName("TestHomeTileReportingETL").getOrCreate()
    print("## Test Report\n")
    # Scenario 1: Insert
    df_tile, df_inter, df_tile_metadata, df_out = scenario_insert(spark)
    print("### Scenario 1: Insert\nInput (Tile):")
    print(markdown_table(df_tile, ["tile_id", "event_type", "user_id"]))
    print("Input (Metadata):")
    print(markdown_table(df_tile_metadata, ["tile_id", "tile_category"]))
    print("Output:")
    print(markdown_table(df_out, ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks"]))
    # Validate output
    expected = {"tile_id": "TILE1", "tile_category": "Personal Finance", "unique_tile_views": 1, "unique_tile_clicks": 1}
    out_row = df_out.filter(F.col("tile_id") == "TILE1").collect()[0]
    status = "PASS" if out_row.tile_category == expected["tile_category"] and out_row.unique_tile_views == expected["unique_tile_views"] and out_row.unique_tile_clicks == expected["unique_tile_clicks"] else "FAIL"
    print(f"Status: {status}\n")

    # Scenario 2: Update
    df_tile, df_inter, df_tile_metadata, df_out = scenario_update(spark)
    print("### Scenario 2: Update\nInput (Tile):")
    print(markdown_table(df_tile, ["tile_id", "event_type", "user_id"]))
    print("Input (Metadata):")
    print(markdown_table(df_tile_metadata, ["tile_id", "tile_category"]))
    print("Output:")
    print(markdown_table(df_out, ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks", "unique_interstitial_primary_clicks"]))
    # Validate output
    expected = {"tile_id": "TILE1", "tile_category": "Personal Finance", "unique_tile_views": 1, "unique_tile_clicks": 1, "unique_interstitial_primary_clicks": 1}
    out_row = df_out.filter(F.col("tile_id") == "TILE1").collect()[0]
    status = "PASS" if out_row.tile_category == expected["tile_category"] and out_row.unique_tile_views == expected["unique_tile_views"] and out_row.unique_tile_clicks == expected["unique_tile_clicks"] and out_row.unique_interstitial_primary_clicks == expected["unique_interstitial_primary_clicks"] else "FAIL"
    print(f"Status: {status}\n")

if __name__ == "__main__":
    run_tests()
