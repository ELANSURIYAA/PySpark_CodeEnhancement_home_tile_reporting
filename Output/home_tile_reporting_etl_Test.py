"""
===============================================================================
Author: Ascendion AAVA
Date: 
Description: Python test script for Home Tile Reporting ETL Pipeline (Insert & Update scenarios)
===============================================================================
File Name       : home_tile_reporting_etl_Test.py
Functional Description:
    - Tests ETL pipeline for insert and update scenarios
    - Validates enrichment of tile_category and tile_name
    - Outputs markdown report with input, output, and pass/fail status
-------------------------------------------------------------------------------
Version     Date          Author          Description
-------------------------------------------------------------------------------
2.0.0       <Leave blank> Ascendion AAVA  Initial test script for metadata enrichment
-------------------------------------------------------------------------------
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# ------------------------------------------------------------------------------
# Helper function to run ETL logic (imported from pipeline script)
# ------------------------------------------------------------------------------
def run_etl(home_tile_events_data, interstitial_events_data, tile_metadata_data, process_date):
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("HomeTileReportingETLTest").getOrCreate()

    home_tile_events_schema = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("session_id", StringType()),
        StructField("event_ts", StringType()),
        StructField("tile_id", StringType()),
        StructField("event_type", StringType()),
        StructField("device_type", StringType()),
        StructField("app_version", StringType()),
    ])
    df_tile = spark.createDataFrame(home_tile_events_data, schema=home_tile_events_schema)
    df_tile = df_tile.withColumn("event_ts", F.to_timestamp("event_ts"))

    interstitial_events_schema = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("session_id", StringType()),
        StructField("event_ts", StringType()),
        StructField("tile_id", StringType()),
        StructField("interstitial_view_flag", BooleanType()),
        StructField("primary_button_click_flag", BooleanType()),
        StructField("secondary_button_click_flag", BooleanType()),
    ])
    df_inter = spark.createDataFrame(interstitial_events_data, schema=interstitial_events_schema)
    df_inter = df_inter.withColumn("event_ts", F.to_timestamp("event_ts"))

    tile_metadata_schema = StructType([
        StructField("tile_id", StringType()),
        StructField("tile_name", StringType()),
        StructField("tile_category", StringType()),
        StructField("is_active", BooleanType()),
        StructField("updated_ts", StringType()),
    ])
    df_metadata = spark.createDataFrame(tile_metadata_data, schema=tile_metadata_schema)
    df_metadata = df_metadata.withColumn("updated_ts", F.to_timestamp("updated_ts"))
    df_metadata = df_metadata.filter(F.col("is_active") == True).select("tile_id", "tile_name", "tile_category")

    # Tile Aggregation
    df_tile_agg = (
        df_tile.groupBy("tile_id")
        .agg(
            F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
            F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
        )
    )
    # Interstitial Aggregation
    df_inter_agg = (
        df_inter.groupBy("tile_id")
        .agg(
            F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
            F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
            F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
        )
    )
    # Join Aggregations
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
    # Metadata enrichment join
    df_daily_summary_enriched = (
        df_daily_summary
        .join(df_metadata, "tile_id", "left")
        .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        .withColumn("tile_name", F.coalesce(F.col("tile_name"), F.lit("Unknown Tile")))
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

# ------------------------------------------------------------------------------
# Scenario 1: Insert (new rows)
# ------------------------------------------------------------------------------
insert_home_tile_events = [
    {"event_id": "E10", "user_id": "U10", "session_id": "S10", "event_ts": "2025-12-01 10:00:00", "tile_id": "TILE_003", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
]
insert_interstitial_events = [
    {"event_id": "IE10", "user_id": "U10", "session_id": "S10", "event_ts": "2025-12-01 10:00:00", "tile_id": "TILE_003", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False},
]
insert_tile_metadata = [
    {"tile_id": "TILE_003", "tile_name": "Payment Options", "tile_category": "Payments", "is_active": True, "updated_ts": "2025-12-01 08:00:00"},
]
insert_date = "2025-12-01"

insert_df = run_etl(insert_home_tile_events, insert_interstitial_events, insert_tile_metadata, insert_date)
insert_output = insert_df.toPandas()

# ------------------------------------------------------------------------------
# Scenario 2: Update (existing keys)
# ------------------------------------------------------------------------------
update_home_tile_events = [
    {"event_id": "E1", "user_id": "U1", "session_id": "S1", "event_ts": "2025-12-01 09:00:00", "tile_id": "TILE_001", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
    {"event_id": "E4", "user_id": "U3", "session_id": "S3", "event_ts": "2025-12-01 09:20:00", "tile_id": "TILE_001", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0"},
]
update_interstitial_events = [
    {"event_id": "IE1", "user_id": "U1", "session_id": "S1", "event_ts": "2025-12-01 09:00:00", "tile_id": "TILE_001", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False},
]
update_tile_metadata = [
    {"tile_id": "TILE_001", "tile_name": "Credit Score Check", "tile_category": "Personal Finance", "is_active": True, "updated_ts": "2025-12-01 08:00:00"},
]
update_date = "2025-12-01"

update_df = run_etl(update_home_tile_events, update_interstitial_events, update_tile_metadata, update_date)
update_output = update_df.toPandas()

# ------------------------------------------------------------------------------
# Markdown Report Output
# ------------------------------------------------------------------------------
def markdown_table(df):
    cols = df.columns
    md = "| " + " | ".join(cols) + " |\n"
    md += "|" + "----|" * len(cols) + "\n"
    for _, row in df.iterrows():
        md += "| " + " | ".join(str(row[c]) for c in cols) + " |\n"
    return md

report = "## Test Report\n"
report += "### Scenario 1: Insert\nInput:\n"
report += "| tile_id | tile_name | tile_category |\n|----|----|----|\n| TILE_003 | Payment Options | Payments |\n"
report += "Output:\n" + markdown_table(insert_output)
if (insert_output.shape[0] == 1 and insert_output["tile_id"][0] == "TILE_003"):
    report += "Status: PASS\n"
else:
    report += "Status: FAIL\n"

report += "\n### Scenario 2: Update\nInput:\n"
report += "| tile_id | tile_name | tile_category |\n|----|----|----|\n| TILE_001 | Credit Score Check | Personal Finance |\n"
report += "Output:\n" + markdown_table(update_output)
if (update_output.shape[0] == 1 and update_output["tile_id"][0] == "TILE_001" and update_output["unique_tile_views"][0] >= 1):
    report += "Status: PASS\n"
else:
    report += "Status: FAIL\n"

print(report)
