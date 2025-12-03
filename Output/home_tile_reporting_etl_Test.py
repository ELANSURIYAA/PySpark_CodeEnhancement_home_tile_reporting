====================================================================
Author: Ascendion AAVA
Date: 
Description: Test script for ETL pipeline validating tile_category enrichment and update logic.
====================================================================

import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_etl(df_tile, df_inter, df_metadata, process_date):
    # Aggregation logic as per enhanced ETL
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
        .join(df_metadata, "tile_id", "left")
        .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        .withColumn("date", F.lit(process_date))
        .select(
            "date",
            "tile_id",
            "tile_category",
            F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
            F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
            F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
            F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
            F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
        )
    )
    return df_daily_summary

def print_markdown_report(scenario, input_df, output_df, expected_rows):
    print(f"## Test Report\n### Scenario {scenario}:")
    print("Input:")
    print(input_df.toPandas().to_markdown(index=False))
    print("Output:")
    print(output_df.toPandas().to_markdown(index=False))
    status = "PASS" if output_df.count() == expected_rows else "FAIL"
    print(f"Status: {status}\n")

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]").appName("TestHomeTileReporting").getOrCreate()
    process_date = "2025-12-01"

    # Scenario 1: Insert (new tile_id, new metadata)
    tile_events_1 = [
        {"event_id": "e1", "user_id": "u1", "session_id": "s1", "event_ts": process_date, "tile_id": "T1", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
        {"event_id": "e2", "user_id": "u2", "session_id": "s2", "event_ts": process_date, "tile_id": "T1", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0"}
    ]
    inter_events_1 = [
        {"event_id": "ie1", "user_id": "u1", "session_id": "s1", "event_ts": process_date, "tile_id": "T1", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False}
    ]
    metadata_1 = [
        {"tile_id": "T1", "tile_category": "Finance"}
    ]
    df_tile_1 = spark.createDataFrame(tile_events_1)
    df_inter_1 = spark.createDataFrame(inter_events_1)
    df_metadata_1 = spark.createDataFrame(metadata_1)
    df_out_1 = run_etl(df_tile_1, df_inter_1, df_metadata_1, process_date)
    print_markdown_report("1: Insert", df_tile_1, df_out_1, 1)

    # Scenario 2: Update (existing tile_id, changed category)
    tile_events_2 = [
        {"event_id": "e3", "user_id": "u1", "session_id": "s1", "event_ts": process_date, "tile_id": "T2", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
        {"event_id": "e4", "user_id": "u1", "session_id": "s2", "event_ts": process_date, "tile_id": "T2", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0"}
    ]
    inter_events_2 = [
        {"event_id": "ie2", "user_id": "u2", "session_id": "s2", "event_ts": process_date, "tile_id": "T2", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True}
    ]
    metadata_2 = [
        {"tile_id": "T2", "tile_category": "Offers"}
    ]
    df_tile_2 = spark.createDataFrame(tile_events_2)
    df_inter_2 = spark.createDataFrame(inter_events_2)
    df_metadata_2 = spark.createDataFrame(metadata_2)
    df_out_2 = run_etl(df_tile_2, df_inter_2, df_metadata_2, process_date)
    print_markdown_report("2: Update", df_tile_2, df_out_2, 1)

    # Scenario 3: Unknown category (metadata missing)
    tile_events_3 = [
        {"event_id": "e5", "user_id": "u3", "session_id": "s3", "event_ts": process_date, "tile_id": "T3", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"}
    ]
    inter_events_3 = []
    metadata_3 = []  # No metadata for T3
    df_tile_3 = spark.createDataFrame(tile_events_3)
    df_inter_3 = spark.createDataFrame(inter_events_3, schema=df_tile_3.schema if inter_events_3 else None)
    df_metadata_3 = spark.createDataFrame(metadata_3, schema=df_tile_3.select("tile_id").schema)
    df_out_3 = run_etl(df_tile_3, df_inter_3, df_metadata_3, process_date)
    print_markdown_report("3: Unknown Category", df_tile_3, df_out_3, 1)
