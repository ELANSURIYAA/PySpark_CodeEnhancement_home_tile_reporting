# Author: Ascendion AAVA
# Description: Python-based test script for home_tile_reporting_etl_Pipeline.py (insert/update scenarios)

import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType, DateType
from datetime import datetime

spark = SparkSession.builder.master("local[1]").appName("HomeTileReportingETLTest").getOrCreate()

PROCESS_DATE = "2025-12-01"

# Sample metadata
metadata_data = [
    ("TILE_001", "Credit Score Check", "Personal Finance", True),
    ("TILE_002", "Health Assessment", "Health & Wellness", True),
    ("TILE_003", "Payment Options", "Payments", True)
]
metadata_schema = StructType([
    StructField("tile_id", StringType()),
    StructField("tile_name", StringType()),
    StructField("tile_category", StringType()),
    StructField("is_active", BooleanType())
])
df_metadata = spark.createDataFrame(metadata_data, metadata_schema).filter(F.col("is_active") == True)

# Scenario 1: Insert (rows are new)
tile_events_data_insert = [
    ("E1", "U1", "S1", datetime(2025,12,1,8,0), "TILE_001", "TILE_VIEW", "Mobile", "1.0"),
    ("E2", "U2", "S2", datetime(2025,12,1,8,5), "TILE_001", "TILE_CLICK", "Mobile", "1.0"),
    ("E3", "U3", "S3", datetime(2025,12,1,9,0), "TILE_002", "TILE_VIEW", "Web", "1.1"),
    ("E4", "U3", "S3", datetime(2025,12,1,9,1), "TILE_002", "TILE_CLICK", "Web", "1.1")
]
tile_events_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("tile_id", StringType()),
    StructField("event_type", StringType()),
    StructField("device_type", StringType()),
    StructField("app_version", StringType())
])
df_tile = spark.createDataFrame(tile_events_data_insert, tile_events_schema).filter(F.to_date("event_ts") == PROCESS_DATE)

inter_events_data_insert = [
    ("IE1", "U1", "S1", datetime(2025,12,1,8,10), "TILE_001", True, True, False),
    ("IE2", "U2", "S2", datetime(2025,12,1,8,20), "TILE_001", True, False, True),
    ("IE3", "U3", "S3", datetime(2025,12,1,9,10), "TILE_002", True, True, False)
]
inter_events_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("tile_id", StringType()),
    StructField("interstitial_view_flag", BooleanType()),
    StructField("primary_button_click_flag", BooleanType()),
    StructField("secondary_button_click_flag", BooleanType())
])
df_inter = spark.createDataFrame(inter_events_data_insert, inter_events_schema).filter(F.to_date("event_ts") == PROCESS_DATE)

# Scenario 2: Update (keys already exist, simulate updated clicks/views)
tile_events_data_update = [
    ("E1", "U1", "S1", datetime(2025,12,1,8,0), "TILE_001", "TILE_VIEW", "Mobile", "1.0"),
    ("E2", "U2", "S2", datetime(2025,12,1,8,5), "TILE_001", "TILE_CLICK", "Mobile", "1.0"),
    ("E2", "U2", "S2", datetime(2025,12,1,8,6), "TILE_001", "TILE_CLICK", "Mobile", "1.0"), # duplicate click for update
    ("E3", "U3", "S3", datetime(2025,12,1,9,0), "TILE_002", "TILE_VIEW", "Web", "1.1"),
    ("E4", "U3", "S3", datetime(2025,12,1,9,1), "TILE_002", "TILE_CLICK", "Web", "1.1")
]
df_tile_upd = spark.createDataFrame(tile_events_data_update, tile_events_schema).filter(F.to_date("event_ts") == PROCESS_DATE)

inter_events_data_update = [
    ("IE1", "U1", "S1", datetime(2025,12,1,8,10), "TILE_001", True, True, False),
    ("IE2", "U2", "S2", datetime(2025,12,1,8,20), "TILE_001", True, False, True),
    ("IE2", "U2", "S2", datetime(2025,12,1,8,21), "TILE_001", True, False, True), # duplicate secondary click for update
    ("IE3", "U3", "S3", datetime(2025,12,1,9,10), "TILE_002", True, True, False)
]
df_inter_upd = spark.createDataFrame(inter_events_data_update, inter_events_schema).filter(F.to_date("event_ts") == PROCESS_DATE)

# --- ETL Logic (matches enriched pipeline) ---
def run_etl(df_tile, df_inter, df_metadata):
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
        .withColumn("date", F.lit(PROCESS_DATE))
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
        df_daily_summary
        .join(df_metadata, "tile_id", "left")
        .withColumn(
            "tile_category",
            F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))
        )
        .withColumn(
            "tile_name",
            F.coalesce(F.col("tile_name"), F.lit("Unknown Tile"))
        )
        .select(
            "date",
            "tile_id",
            "tile_name",
            "tile_category",
            "unique_tile_views",
            "unique_tile_clicks",
            "unique_interstitial_views",
            "unique_interstitial_primary_clicks",
            "unique_interstitial_secondary_clicks"
        )
    )
    return df_daily_summary_enriched

# --- Test Runner ---
def print_markdown_report(title, input_rows, output_rows, status):
    print(f"## Test Report\n### {title}")
    print("Input:")
    if input_rows:
        print("| tile_id | tile_name | tile_category | unique_tile_views | unique_tile_clicks | unique_interstitial_views | unique_interstitial_primary_clicks | unique_interstitial_secondary_clicks |")
        print("|---------|-----------|--------------|-------------------|--------------------|--------------------------|------------------------------------|--------------------------------------|")
        for row in input_rows:
            print(f"| {row['tile_id']} | {row['tile_name']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |")
    else:
        print("No input rows.")
    print("Output:")
    if output_rows:
        print("| tile_id | tile_name | tile_category | unique_tile_views | unique_tile_clicks | unique_interstitial_views | unique_interstitial_primary_clicks | unique_interstitial_secondary_clicks |")
        print("|---------|-----------|--------------|-------------------|--------------------|--------------------------|------------------------------------|--------------------------------------|")
        for row in output_rows:
            print(f"| {row['tile_id']} | {row['tile_name']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |")
    else:
        print("No output rows.")
    print(f"Status: {status}\n")

# Scenario 1: Insert
result_insert = run_etl(df_tile, df_inter, df_metadata)
rows_insert = [row.asDict() for row in result_insert.collect()]
expected_insert = [
    {'tile_id': 'TILE_001', 'tile_name': 'Credit Score Check', 'tile_category': 'Personal Finance', 'unique_tile_views': 1, 'unique_tile_clicks': 1, 'unique_interstitial_views': 2, 'unique_interstitial_primary_clicks': 1, 'unique_interstitial_secondary_clicks': 1},
    {'tile_id': 'TILE_002', 'tile_name': 'Health Assessment', 'tile_category': 'Health & Wellness', 'unique_tile_views': 1, 'unique_tile_clicks': 1, 'unique_interstitial_views': 1, 'unique_interstitial_primary_clicks': 1, 'unique_interstitial_secondary_clicks': 0}
]
pass_insert = all(any(
    r['tile_id'] == e['tile_id'] and r['unique_tile_views'] == e['unique_tile_views'] and r['unique_tile_clicks'] == e['unique_tile_clicks'] for r in rows_insert
) for e in expected_insert)
print_markdown_report("Scenario 1: Insert", expected_insert, rows_insert, "PASS" if pass_insert else "FAIL")

# Scenario 2: Update
result_update = run_etl(df_tile_upd, df_inter_upd, df_metadata)
rows_update = [row.asDict() for row in result_update.collect()]
expected_update = [
    {'tile_id': 'TILE_001', 'tile_name': 'Credit Score Check', 'tile_category': 'Personal Finance', 'unique_tile_views': 1, 'unique_tile_clicks': 1, 'unique_interstitial_views': 2, 'unique_interstitial_primary_clicks': 1, 'unique_interstitial_secondary_clicks': 1},
    {'tile_id': 'TILE_002', 'tile_name': 'Health Assessment', 'tile_category': 'Health & Wellness', 'unique_tile_views': 1, 'unique_tile_clicks': 1, 'unique_interstitial_views': 1, 'unique_interstitial_primary_clicks': 1, 'unique_interstitial_secondary_clicks': 0}
]
pass_update = all(any(
    r['tile_id'] == e['tile_id'] and r['unique_tile_views'] == e['unique_tile_views'] and r['unique_tile_clicks'] == e['unique_tile_clicks'] for r in rows_update
) for e in expected_update)
print_markdown_report("Scenario 2: Update", expected_update, rows_update, "PASS" if pass_update else "FAIL")

spark.stop()
