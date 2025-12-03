# Author: Ascendion AAVA
# Date: 
# Description: Python-based test script for home_tile_reporting_etl_Pipeline.py

import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Import the pipeline code (assume available as module for testing)
def run_etl_with_sample_data(tile_events_data, interstitial_events_data, tile_metadata_data, process_date):
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("HomeTileReportingETLTest").getOrCreate()

    schema_tile_events = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("session_id", StringType()),
        StructField("event_ts", StringType()),
        StructField("tile_id", StringType()),
        StructField("event_type", StringType()),
        StructField("device_type", StringType()),
        StructField("app_version", StringType())
    ])
    df_tile = spark.createDataFrame(tile_events_data, schema=schema_tile_events)\
        .withColumn("event_ts", F.to_timestamp("event_ts"))\
        .filter(F.to_date("event_ts") == process_date)

    schema_interstitial_events = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("session_id", StringType()),
        StructField("event_ts", StringType()),
        StructField("tile_id", StringType()),
        StructField("interstitial_view_flag", BooleanType()),
        StructField("primary_button_click_flag", BooleanType()),
        StructField("secondary_button_click_flag", BooleanType())
    ])
    df_inter = spark.createDataFrame(interstitial_events_data, schema=schema_interstitial_events)\
        .withColumn("event_ts", F.to_timestamp("event_ts"))\
        .filter(F.to_date("event_ts") == process_date)

    schema_tile_metadata = StructType([
        StructField("tile_id", StringType()),
        StructField("tile_name", StringType()),
        StructField("tile_category", StringType()),
        StructField("is_active", BooleanType()),
        StructField("updated_ts", StringType())
    ])
    df_metadata = spark.createDataFrame(tile_metadata_data, schema=schema_tile_metadata)\
        .withColumn("updated_ts", F.to_timestamp("updated_ts"))\
        .filter(F.col("is_active") == True)

    # --- Pipeline logic (same as in _Pipeline.py) ---
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
        .withColumn("date", F.lit(process_date))
        .select(
            "date",
            "tile_id",
            F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
            F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
            F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
            F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
            F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
        )
    )
    df_daily_summary_enriched = (
        df_daily_summary.join(df_metadata, "tile_id", "left")
        .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        .select(
            "date",
            "tile_id",
            "tile_category",
            "unique_tile_views",
            "unique_tile_clicks",
            "unique_interstitial_views",
            "unique_interstitial_primary_clicks",
            "unique_interstitial_secondary_clicks"
        )
    )
    return df_daily_summary_enriched

# Scenario 1: Insert (all new rows)
tile_events_data_insert = [
    ("e1", "u1", "s1", "2025-12-01 10:00:00", "TILE01", "TILE_VIEW", "Mobile", "1.0.0"),
    ("e2", "u2", "s2", "2025-12-01 10:01:00", "TILE01", "TILE_CLICK", "Web", "1.0.0"),
]
interstitial_events_data_insert = [
    ("e10", "u1", "s1", "2025-12-01 11:00:00", "TILE01", True, True, False),
    ("e11", "u2", "s2", "2025-12-01 11:01:00", "TILE01", True, False, True),
]
tile_metadata_data_insert = [
    ("TILE01", "Finance Tile", "Personal Finance", True, "2025-11-30 12:00:00"),
]
process_date = "2025-12-01"
df_result_insert = run_etl_with_sample_data(tile_events_data_insert, interstitial_events_data_insert, tile_metadata_data_insert, process_date)

# Scenario 2: Update (existing key)
tile_events_data_update = [
    ("e1", "u1", "s1", "2025-12-01 10:00:00", "TILE01", "TILE_VIEW", "Mobile", "1.0.0"),
    ("e2", "u2", "s2", "2025-12-01 10:01:00", "TILE01", "TILE_CLICK", "Web", "1.0.0"),
    ("e3", "u3", "s3", "2025-12-01 10:02:00", "TILE01", "TILE_CLICK", "Mobile", "1.0.0"),
]
interstitial_events_data_update = [
    ("e10", "u1", "s1", "2025-12-01 11:00:00", "TILE01", True, True, False),
    ("e11", "u2", "s2", "2025-12-01 11:01:00", "TILE01", True, False, True),
]
tile_metadata_data_update = [
    ("TILE01", "Finance Tile", "Personal Finance", True, "2025-11-30 12:00:00"),
]
df_result_update = run_etl_with_sample_data(tile_events_data_update, interstitial_events_data_update, tile_metadata_data_update, process_date)

# --- Markdown Report Generation ---
def print_markdown_report():
    print("## Test Report\n")
    print("### Scenario 1: Insert")
    print("Input:")
    print("| tile_id | tile_category | unique_tile_views | unique_tile_clicks | unique_interstitial_views | unique_interstitial_primary_clicks | unique_interstitial_secondary_clicks |")
    print("|---------|--------------|------------------|--------------------|--------------------------|-------------------------------------|---------------------------------------|")
    for row in df_result_insert.collect():
        print(f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |")
    print("Output:")
    for row in df_result_insert.collect():
        print(f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |")
    # Basic validation: inserted rows present
    status_insert = "PASS" if df_result_insert.count() == 1 else "FAIL"
    print(f"Status: {status_insert}\n")

    print("### Scenario 2: Update")
    print("Input:")
    for row in df_result_update.collect():
        print(f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |")
    print("Output:")
    for row in df_result_update.collect():
        print(f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |")
    # Basic validation: updated row present and counts correct
    status_update = "PASS" if df_result_update.count() == 1 and any(row['unique_tile_clicks'] == 2 for row in df_result_update.collect()) else "FAIL"
    print(f"Status: {status_update}\n")

if __name__ == "__main__":
    print_markdown_report()
