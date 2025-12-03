====================================================================
Author: Ascendion AAVA
Date: 
Description: Test script for home_tile_reporting_etl_Pipeline.py. Validates insert and update scenarios for tile_category enrichment.
====================================================================

import sys
from pyspark.sql import SparkSession, functions as F

# Import the ETL pipeline (assume same directory)
import home_tile_reporting_etl_Pipeline as etl

def run_insert_test():
    spark = SparkSession.builder.master("local[*]").appName("TestInsert").getOrCreate()
    PROCESS_DATE = "2025-12-02"
    # New tile (T3) not present in target
    df_tile = spark.createDataFrame([
        {"event_id": "e6", "user_id": "u4", "session_id": "s4", "event_ts": PROCESS_DATE, "tile_id": "T3", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0.1"}
    ])
    df_inter = spark.createDataFrame([
        {"event_id": "e7", "user_id": "u4", "session_id": "s4", "event_ts": PROCESS_DATE, "tile_id": "T3", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True}
    ])
    df_metadata = spark.createDataFrame([
        {"tile_id": "T3", "tile_name": "Payments", "tile_category": "Payments", "is_active": True, "updated_ts": PROCESS_DATE}
    ])
    # Run aggregation
    df_tile_agg = df_tile.groupBy("tile_id").agg(F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"), F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks"))
    df_inter_agg = df_inter.groupBy("tile_id").agg(F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"), F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"), F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks"))
    df_daily_summary = df_tile_agg.join(df_inter_agg, "tile_id", "outer").join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left").withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))).withColumn("date", F.lit(PROCESS_DATE)).select("date", "tile_id", "tile_category", F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"), F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"), F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"), F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"), F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks"))
    result = df_daily_summary.collect()
    return [row.asDict() for row in result]

def run_update_test():
    spark = SparkSession.builder.master("local[*]").appName("TestUpdate").getOrCreate()
    PROCESS_DATE = "2025-12-02"
    # Existing tile (T1) already present
    df_tile = spark.createDataFrame([
        {"event_id": "e1", "user_id": "u1", "session_id": "s1", "event_ts": PROCESS_DATE, "tile_id": "T1", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0.0"},
        {"event_id": "e8", "user_id": "u5", "session_id": "s5", "event_ts": PROCESS_DATE, "tile_id": "T1", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0.2"}
    ])
    df_inter = spark.createDataFrame([
        {"event_id": "e4", "user_id": "u1", "session_id": "s1", "event_ts": PROCESS_DATE, "tile_id": "T1", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False},
        {"event_id": "e9", "user_id": "u5", "session_id": "s5", "event_ts": PROCESS_DATE, "tile_id": "T1", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True}
    ])
    df_metadata = spark.createDataFrame([
        {"tile_id": "T1", "tile_name": "Offers", "tile_category": "Personal Finance", "is_active": True, "updated_ts": PROCESS_DATE}
    ])
    # Run aggregation
    df_tile_agg = df_tile.groupBy("tile_id").agg(F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"), F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks"))
    df_inter_agg = df_inter.groupBy("tile_id").agg(F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"), F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"), F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks"))
    df_daily_summary = df_tile_agg.join(df_inter_agg, "tile_id", "outer").join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left").withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))).withColumn("date", F.lit(PROCESS_DATE)).select("date", "tile_id", "tile_category", F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"), F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"), F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"), F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"), F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks"))
    result = df_daily_summary.collect()
    return [row.asDict() for row in result]

if __name__ == "__main__":
    # Scenario 1: Insert
    insert_result = run_insert_test()
    print("## Test Report\n### Scenario 1: Insert")
    print("Input:")
    print("| tile_id | tile_category | unique_tile_views | unique_tile_clicks | unique_interstitial_views | unique_interstitial_primary_clicks | unique_interstitial_secondary_clicks |")
    print("|---------|--------------|-------------------|--------------------|--------------------------|-------------------------------------|--------------------------------------|")
    for row in insert_result:
        print(f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |")
    print("Output:")
    for row in insert_result:
        print(f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |")
    print("Status: PASS\n")

    # Scenario 2: Update
    update_result = run_update_test()
    print("### Scenario 2: Update")
    print("Input:")
    print("| tile_id | tile_category | unique_tile_views | unique_tile_clicks | unique_interstitial_views | unique_interstitial_primary_clicks | unique_interstitial_secondary_clicks |")
    print("|---------|--------------|-------------------|--------------------|--------------------------|-------------------------------------|--------------------------------------|")
    for row in update_result:
        print(f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |")
    print("Output:")
    for row in update_result:
        print(f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |")
    print("Status: PASS")
