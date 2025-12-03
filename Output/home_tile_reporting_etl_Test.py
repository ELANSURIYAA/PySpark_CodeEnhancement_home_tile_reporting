# Author: Ascendion AAVA
# Date: 
# Description: Python test script for home_tile_reporting_etl_Pipeline.py - validates insert and update logic using sample PySpark DataFrames (no PyTest, Databricks compatible).

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, TimestampType, DateType
import pandas as pd

# Helper to pretty-print DataFrame as markdown table
def df_to_markdown(df, columns):
    data = df.select(columns).toPandas()
    header = '| ' + ' | '.join(columns) + ' |\n'
    sep = '| ' + ' | '.join(['---']*len(columns)) + ' |\n'
    rows = '\n'.join(['| ' + ' | '.join(str(x) for x in row) + ' |' for row in data.values])
    return header + sep + rows

spark = SparkSession.builder.master('local[*]').appName('TestHomeTileReportingETL').getOrCreate()

# Sample data for scenario 1 (insert)
home_tile_events_data = [
    {"event_id": "E1", "user_id": "U1", "session_id": "S1", "event_ts": "2025-12-01 09:00:00", "tile_id": "TILE_A", "event_type": "TILE_VIEW", "device_type": "Web", "app_version": "1.0"},
    {"event_id": "E2", "user_id": "U2", "session_id": "S2", "event_ts": "2025-12-01 09:05:00", "tile_id": "TILE_A", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0"},
    {"event_id": "E3", "user_id": "U3", "session_id": "S3", "event_ts": "2025-12-01 09:10:00", "tile_id": "TILE_B", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.1"},
]
interstitial_events_data = [
    {"event_id": "IE1", "user_id": "U1", "session_id": "S1", "event_ts": "2025-12-01 09:00:00", "tile_id": "TILE_A", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": False},
    {"event_id": "IE2", "user_id": "U2", "session_id": "S2", "event_ts": "2025-12-01 09:05:00", "tile_id": "TILE_B", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False},
]
tile_metadata_data = [
    {"tile_id": "TILE_A", "tile_name": "Offers", "tile_category": "OFFERS", "is_active": True, "updated_ts": "2025-11-30 10:00:00"},
    {"tile_id": "TILE_B", "tile_name": "Health", "tile_category": "HEALTH_CHECKS", "is_active": True, "updated_ts": "2025-11-30 10:00:00"},
]

# Sample data for scenario 2 (update)
home_tile_events_update = [
    {"event_id": "E4", "user_id": "U2", "session_id": "S2", "event_ts": "2025-12-01 09:15:00", "tile_id": "TILE_A", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0"},
]
interstitial_events_update = [
    {"event_id": "IE3", "user_id": "U2", "session_id": "S2", "event_ts": "2025-12-01 09:15:00", "tile_id": "TILE_A", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False},
]

# Schemas
tile_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("tile_id", StringType()),
    StructField("event_type", StringType()),
    StructField("device_type", StringType()),
    StructField("app_version", StringType()),
])
interstitial_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("tile_id", StringType()),
    StructField("interstitial_view_flag", BooleanType()),
    StructField("primary_button_click_flag", BooleanType()),
    StructField("secondary_button_click_flag", BooleanType()),
])
metadata_schema = StructType([
    StructField("tile_id", StringType()),
    StructField("tile_name", StringType()),
    StructField("tile_category", StringType()),
    StructField("is_active", BooleanType()),
    StructField("updated_ts", StringType()),
])

# Helper to run ETL logic as in Pipeline

def run_etl(df_tile, df_inter, df_metadata, process_date):
    df_tile = df_tile.filter(F.to_date("event_ts") == process_date)
    df_inter = df_inter.filter(F.to_date("event_ts") == process_date)
    df_metadata = df_metadata.filter(F.col("is_active") == True)
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
    df_daily_summary_enhanced = (
        df_daily_summary
        .join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left")
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
    return df_daily_summary_enhanced

# Scenario 1: Insert
process_date = "2025-12-01"
df_tile = spark.createDataFrame(home_tile_events_data, tile_schema)
df_inter = spark.createDataFrame(interstitial_events_data, interstitial_schema)
df_metadata = spark.createDataFrame(tile_metadata_data, metadata_schema)
df_daily_summary_enhanced = run_etl(df_tile, df_inter, df_metadata, process_date)

# Expected output: new rows for TILE_A and TILE_B with correct tile_category
insert_columns = ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks", "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"]
insert_output = df_daily_summary_enhanced.orderBy("tile_id").select(insert_columns)

# Scenario 2: Update
# Simulate update by adding new click for TILE_A
update_tile_df = spark.createDataFrame(home_tile_events_data + home_tile_events_update, tile_schema)
update_inter_df = spark.createDataFrame(interstitial_events_data + interstitial_events_update, interstitial_schema)
update_metadata_df = spark.createDataFrame(tile_metadata_data, metadata_schema)
update_daily_summary_enhanced = run_etl(update_tile_df, update_inter_df, update_metadata_df, process_date)

update_output = update_daily_summary_enhanced.orderBy("tile_id").select(insert_columns)

# Validate Insert Scenario
expected_insert = pd.DataFrame([
    {"date": process_date, "tile_id": "TILE_A", "tile_category": "OFFERS", "unique_tile_views": 1, "unique_tile_clicks": 1, "unique_interstitial_views": 1, "unique_interstitial_primary_clicks": 0, "unique_interstitial_secondary_clicks": 0},
    {"date": process_date, "tile_id": "TILE_B", "tile_category": "HEALTH_CHECKS", "unique_tile_views": 1, "unique_tile_clicks": 0, "unique_interstitial_views": 1, "unique_interstitial_primary_clicks": 1, "unique_interstitial_secondary_clicks": 0}
])
actual_insert = insert_output.toPandas().fillna(0)
insert_pass = actual_insert.equals(expected_insert)

# Validate Update Scenario
expected_update = pd.DataFrame([
    {"date": process_date, "tile_id": "TILE_A", "tile_category": "OFFERS", "unique_tile_views": 1, "unique_tile_clicks": 2, "unique_interstitial_views": 2, "unique_interstitial_primary_clicks": 1, "unique_interstitial_secondary_clicks": 0},
    {"date": process_date, "tile_id": "TILE_B", "tile_category": "HEALTH_CHECKS", "unique_tile_views": 1, "unique_tile_clicks": 0, "unique_interstitial_views": 1, "unique_interstitial_primary_clicks": 1, "unique_interstitial_secondary_clicks": 0}
])
actual_update = update_output.toPandas().fillna(0)
update_pass = actual_update.equals(expected_update)

# Print markdown test report
print("## Test Report\n")
print("### Scenario 1: Insert")
print("Input:")
print(df_to_markdown(df_tile, ["event_id", "user_id", "tile_id", "event_type", "event_ts"]))
print("Output:")
print(df_to_markdown(insert_output, insert_columns))
print(f"Status: {'PASS' if insert_pass else 'FAIL'}\n")

print("### Scenario 2: Update")
print("Input:")
print(df_to_markdown(update_tile_df, ["event_id", "user_id", "tile_id", "event_type", "event_ts"]))
print("Output:")
print(df_to_markdown(update_output, insert_columns))
print(f"Status: {'PASS' if update_pass else 'FAIL'}\n")
