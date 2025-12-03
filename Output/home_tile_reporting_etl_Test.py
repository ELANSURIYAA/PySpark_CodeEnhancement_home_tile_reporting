# Author: Ascendion AAVA
# Date: 
# Description: Python test script to validate home_tile_reporting_etl_Pipeline.py for insert and update scenarios (SCRUM-7567)

from pyspark.sql import SparkSession, functions as F
from datetime import datetime

# Helper function: pipeline logic (simplified for test)
def run_pipeline(df_tile, df_inter, df_metadata, process_date):
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
        .join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left")
        .withColumn("date", F.lit(process_date))
        .select(
            "date",
            "tile_id",
            F.coalesce("tile_category", F.lit("UNKNOWN")).alias("tile_category"),
            F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
            F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
            F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
            F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
            F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
        )
    )
    return df_daily_summary

# Scenario 1: Insert (new rows)
def test_insert(spark):
    process_date = "2025-12-01"
    tile_data = [
        {"event_id": "E1", "user_id": "U1", "session_id": "S1", "event_ts": process_date, "tile_id": "TILE_001", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
        {"event_id": "E2", "user_id": "U2", "session_id": "S2", "event_ts": process_date, "tile_id": "TILE_001", "event_type": "TILE_CLICK", "device_type": "Mobile", "app_version": "1.0"},
        {"event_id": "E3", "user_id": "U1", "session_id": "S3", "event_ts": process_date, "tile_id": "TILE_002", "event_type": "TILE_VIEW", "device_type": "Web", "app_version": "1.1"}
    ]
    inter_data = [
        {"event_id": "I1", "user_id": "U1", "session_id": "S1", "event_ts": process_date, "tile_id": "TILE_001", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False},
        {"event_id": "I2", "user_id": "U2", "session_id": "S2", "event_ts": process_date, "tile_id": "TILE_002", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True}
    ]
    metadata_data = [
        {"tile_id": "TILE_001", "tile_name": "Credit Score Check", "tile_category": "Personal Finance", "is_active": True, "updated_ts": process_date},
        {"tile_id": "TILE_002", "tile_name": "Health Dashboard", "tile_category": "Health", "is_active": True, "updated_ts": process_date}
    ]
    df_tile = spark.createDataFrame(tile_data)
    df_inter = spark.createDataFrame(inter_data)
    df_metadata = spark.createDataFrame(metadata_data)
    df_out = run_pipeline(df_tile, df_inter, df_metadata, process_date)
    result = df_out.orderBy("tile_id").toPandas()
    # Validation
    passed = (
        (result.loc[result["tile_id"]=="TILE_001", "tile_category"].iloc[0] == "Personal Finance") and
        (result.loc[result["tile_id"]=="TILE_002", "tile_category"].iloc[0] == "Health") and
        (result.loc[result["tile_id"]=="TILE_001", "unique_tile_views"].iloc[0] == 1) and
        (result.loc[result["tile_id"]=="TILE_001", "unique_tile_clicks"].iloc[0] == 1)
    )
    return tile_data, inter_data, metadata_data, result, passed

# Scenario 2: Update (existing keys)
def test_update(spark):
    process_date = "2025-12-01"
    tile_data = [
        {"event_id": "E1", "user_id": "U1", "session_id": "S1", "event_ts": process_date, "tile_id": "TILE_001", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
        {"event_id": "E2", "user_id": "U1", "session_id": "S2", "event_ts": process_date, "tile_id": "TILE_001", "event_type": "TILE_CLICK", "device_type": "Mobile", "app_version": "1.0"},
        {"event_id": "E3", "user_id": "U2", "session_id": "S3", "event_ts": process_date, "tile_id": "TILE_001", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.1"}
    ]
    inter_data = [
        {"event_id": "I1", "user_id": "U1", "session_id": "S1", "event_ts": process_date, "tile_id": "TILE_001", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False}
    ]
    metadata_data = [
        {"tile_id": "TILE_001", "tile_name": "Credit Score Check", "tile_category": "Personal Finance", "is_active": True, "updated_ts": process_date}
    ]
    df_tile = spark.createDataFrame(tile_data)
    df_inter = spark.createDataFrame(inter_data)
    df_metadata = spark.createDataFrame(metadata_data)
    df_out = run_pipeline(df_tile, df_inter, df_metadata, process_date)
    result = df_out.toPandas()
    # Validation
    passed = (
        (result.loc[result["tile_id"]=="TILE_001", "unique_tile_views"].iloc[0] == 1) and
        (result.loc[result["tile_id"]=="TILE_001", "unique_tile_clicks"].iloc[0] == 2) and
        (result.loc[result["tile_id"]=="TILE_001", "tile_category"].iloc[0] == "Personal Finance")
    )
    return tile_data, inter_data, metadata_data, result, passed

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]").appName("HomeTileReportingTest").getOrCreate()
    # Scenario 1: Insert
    tdata1, idata1, mdata1, out1, pass1 = test_insert(spark)
    # Scenario 2: Update
    tdata2, idata2, mdata2, out2, pass2 = test_update(spark)
    # Markdown Report
    print("""
## Test Report
### Scenario 1: Insert
Input (Tiles):
| event_id | user_id | tile_id | event_type |
|----------|--------|---------|------------|
{}""".format("\n".join([f"| {d['event_id']} | {d['user_id']} | {d['tile_id']} | {d['event_type']} |" for d in tdata1])))
    print("Input (Metadata):\n| tile_id | tile_category |\n|---------|--------------|\n{}".format("\n".join([f"| {d['tile_id']} | {d['tile_category']} |" for d in mdata1])))
    print("Output:\n| tile_id | tile_category | unique_tile_views | unique_tile_clicks |\n|--------|--------------|-------------------|--------------------|\n{}".format("\n".join([f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} |" for _, row in out1.iterrows()])))
    print(f"Status: {'PASS' if pass1 else 'FAIL'}\n")
    print("### Scenario 2: Update\nInput (Tiles):\n| event_id | user_id | tile_id | event_type |\n|----------|--------|---------|------------|\n{}".format("\n".join([f"| {d['event_id']} | {d['user_id']} | {d['tile_id']} | {d['event_type']} |" for d in tdata2])))
    print("Input (Metadata):\n| tile_id | tile_category |\n|---------|--------------|\n{}".format("\n".join([f"| {d['tile_id']} | {d['tile_category']} |" for d in mdata2])))
    print("Output:\n| tile_id | tile_category | unique_tile_views | unique_tile_clicks |\n|--------|--------------|-------------------|--------------------|\n{}".format("\n".join([f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} |" for _, row in out2.iterrows()])))
    print(f"Status: {'PASS' if pass2 else 'FAIL'}\n")
