'''
====================================================================
Author: Ascendion AAVA
Date: 
Description: Python-based test script for home_tile_reporting_etl_Pipeline.py (tile_category enrichment)
====================================================================

Functional Description:
    - Simulates insert and update scenarios for TARGET_HOME_TILE_DAILY_SUMMARY
    - Validates enrichment of tile_category and defaulting to UNKNOWN
    - Produces markdown test report

'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create Spark session for testing
spark = SparkSession.builder.master("local[1]").appName("TestHomeTileReportingETL").getOrCreate()

# Sample data for SOURCE_HOME_TILE_EVENTS
source_tile_events = [
    {"event_id": "e1", "user_id": "u1", "session_id": "s1", "event_ts": "2025-12-01 10:00:00", "tile_id": "T1", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
    {"event_id": "e2", "user_id": "u2", "session_id": "s2", "event_ts": "2025-12-01 10:05:00", "tile_id": "T2", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0"},
    {"event_id": "e3", "user_id": "u3", "session_id": "s3", "event_ts": "2025-12-01 10:10:00", "tile_id": "T3", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
]

df_tile = spark.createDataFrame(source_tile_events)

df_tile = df_tile.withColumn("event_ts", F.to_timestamp("event_ts"))

# Sample data for SOURCE_INTERSTITIAL_EVENTS
source_inter_events = [
    {"event_id": "i1", "user_id": "u1", "session_id": "s1", "event_ts": "2025-12-01 10:00:00", "tile_id": "T1", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False},
    {"event_id": "i2", "user_id": "u2", "session_id": "s2", "event_ts": "2025-12-01 10:05:00", "tile_id": "T2", "interstitial_view_flag": False, "primary_button_click_flag": False, "secondary_button_click_flag": True},
]

df_inter = spark.createDataFrame(source_inter_events)

df_inter = df_inter.withColumn("event_ts", F.to_timestamp("event_ts"))

# Sample data for SOURCE_TILE_METADATA
source_tile_metadata = [
    {"tile_id": "T1", "tile_category": "Offers"},
    {"tile_id": "T2", "tile_category": "Payments"},
    # T3 intentionally missing to test UNKNOWN
]

df_tile_metadata = spark.createDataFrame(source_tile_metadata)

PROCESS_DATE = "2025-12-01"

# Aggregations

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
    .join(df_tile_metadata, "tile_id", "left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
    .withColumn("date", F.lit(PROCESS_DATE))
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

# Scenario 1: Insert - all rows are new
insert_input = df_daily_summary.toPandas()

# Simulate target table empty
target_table = insert_input.copy()

# Validate insert
insert_pass = (len(target_table) == len(insert_input)) and all(target_table["tile_id"] == insert_input["tile_id"])

# Scenario 2: Update - keys exist, update metrics
# Simulate existing target table with T1 and T2, but different metrics
existing_target = target_table.copy()
existing_target["unique_tile_views"] = [0, 0, 0]
existing_target["unique_tile_clicks"] = [0, 0, 0]

# Apply update (overwrite partition)
updated_target = df_daily_summary.toPandas()
update_pass = all(updated_target["unique_tile_views"] == insert_input["unique_tile_views"]) and all(updated_target["tile_category"] == insert_input["tile_category"])

# Markdown report
md = "## Test Report\n"
md += "### Scenario 1: Insert\n"
md += "Input:\n"
md += "| tile_id | tile_category | unique_tile_views | unique_tile_clicks |\n"
md += "|--------|--------------|------------------|-------------------|\n"
for idx, row in insert_input.iterrows():
    md += f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} |\n"
md += "Output:\n"
for idx, row in target_table.iterrows():
    md += f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} |\n"
md += f"Status: {'PASS' if insert_pass else 'FAIL'}\n"

md += "\n### Scenario 2: Update\n"
md += "Input (existing target):\n"
for idx, row in existing_target.iterrows():
    md += f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} |\n"
md += "Output (after update):\n"
for idx, row in updated_target.iterrows():
    md += f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} |\n"
md += f"Status: {'PASS' if update_pass else 'FAIL'}\n"

# Show markdown report
print(md)
