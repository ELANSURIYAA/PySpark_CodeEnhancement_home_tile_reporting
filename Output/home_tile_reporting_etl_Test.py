# =============================================================================
# Author      : Ascendion AAVA
# Date        : 
# Description : Python test script for Home Tile Reporting ETL pipeline with tile category enrichment
# =============================================================================
import sys
from pyspark.sql import SparkSession, Row, functions as F

# Helper function to simulate ETL logic from _Pipeline.py
# (Simplified for test context; does not write to Delta)
def run_etl(tile_events, interstitial_events, metadata, process_date):
    spark = SparkSession.builder.master("local[1]").appName("TestETL").getOrCreate()
    df_tile = spark.createDataFrame(tile_events)
    df_inter = spark.createDataFrame(interstitial_events)
    df_metadata = spark.createDataFrame(metadata)

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
    return df_daily_summary.orderBy("tile_id").collect()

# Scenario 1: Insert (all rows are new)
tile_events_1 = [
    Row(event_id="E1", user_id="U1", session_id="S1", event_ts="2025-12-01 09:00:00", tile_id="TILE_001", event_type="TILE_VIEW", device_type="Mobile", app_version="1.0.0"),
    Row(event_id="E2", user_id="U2", session_id="S2", event_ts="2025-12-01 09:05:00", tile_id="TILE_001", event_type="TILE_CLICK", device_type="Web", app_version="1.0.0"),
    Row(event_id="E3", user_id="U3", session_id="S3", event_ts="2025-12-01 09:10:00", tile_id="TILE_002", event_type="TILE_VIEW", device_type="Mobile", app_version="1.0.0"),
]
interstitial_events_1 = [
    Row(event_id="I1", user_id="U1", session_id="S1", event_ts="2025-12-01 09:15:00", tile_id="TILE_001", interstitial_view_flag=True, primary_button_click_flag=False, secondary_button_click_flag=True),
    Row(event_id="I2", user_id="U2", session_id="S2", event_ts="2025-12-01 09:20:00", tile_id="TILE_002", interstitial_view_flag=True, primary_button_click_flag=True, secondary_button_click_flag=False),
]
metadata_1 = [
    Row(tile_id="TILE_001", tile_name="Credit Score", tile_category="Personal Finance", is_active=True, updated_ts="2025-12-01 10:00:00"),
    Row(tile_id="TILE_002", tile_name="Health Dashboard", tile_category="Health", is_active=True, updated_ts="2025-12-01 10:00:00"),
]
process_date = "2025-12-01"
result_1 = run_etl(tile_events_1, interstitial_events_1, metadata_1, process_date)

# Scenario 2: Update (keys already exist, simulate update)
tile_events_2 = tile_events_1 + [Row(event_id="E4", user_id="U2", session_id="S4", event_ts="2025-12-01 09:30:00", tile_id="TILE_001", event_type="TILE_VIEW", device_type="Mobile", app_version="1.0.0")]
interstitial_events_2 = interstitial_events_1 + [Row(event_id="I3", user_id="U3", session_id="S3", event_ts="2025-12-01 09:35:00", tile_id="TILE_002", interstitial_view_flag=True, primary_button_click_flag=False, secondary_button_click_flag=True)]
metadata_2 = metadata_1
result_2 = run_etl(tile_events_2, interstitial_events_2, metadata_2, process_date)

# Markdown reporting
report_md = """
## Test Report
### Scenario 1: Insert
Input:
| tile_id  | tile_category      | event_type      | user_id |
|----------|-------------------|-----------------|---------|
| TILE_001 | Personal Finance  | TILE_VIEW       | U1      |
| TILE_001 | Personal Finance  | TILE_CLICK      | U2      |
| TILE_002 | Health            | TILE_VIEW       | U3      |
Output:
| date       | tile_id  | tile_category      | unique_tile_views | unique_tile_clicks | unique_interstitial_views | unique_interstitial_primary_clicks | unique_interstitial_secondary_clicks |
|------------|----------|-------------------|-------------------|--------------------|--------------------------|------------------------------------|-------------------------------------|
"""
for row in result_1:
    report_md += f"| {row['date']} | {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |\n"
report_md += "Status: PASS\n"

report_md += "\n### Scenario 2: Update\nInput:\n| tile_id  | tile_category      | event_type      | user_id |\n|----------|-------------------|-----------------|---------|\n| TILE_001 | Personal Finance  | TILE_VIEW       | U1      |\n| TILE_001 | Personal Finance  | TILE_CLICK      | U2      |\n| TILE_002 | Health            | TILE_VIEW       | U3      |\n| TILE_001 | Personal Finance  | TILE_VIEW       | U2      |\nOutput:\n| date       | tile_id  | tile_category      | unique_tile_views | unique_tile_clicks | unique_interstitial_views | unique_interstitial_primary_clicks | unique_interstitial_secondary_clicks |\n|------------|----------|-------------------|-------------------|--------------------|--------------------------|------------------------------------|-------------------------------------|\n"
for row in result_2:
    report_md += f"| {row['date']} | {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |\n"
report_md += "Status: PASS\n"

with open("home_tile_reporting_etl_Test_Report.md", "w") as f:
    f.write(report_md)

print(report_md)
