# Author: Ascendion AAVA
# Date: 
# Description: Python-based test script for enhanced ETL pipeline (tile metadata join)

import sys
import pandas as pd
from pyspark.sql import SparkSession, functions as F

# --------- Setup Spark Session ---------
spark = SparkSession.builder.master("local[1]").appName("HomeTileReportingETLTest").getOrCreate()

# --------- Utility: Run ETL Logic ---------
def run_etl(df_tile, df_inter, df_metadata, process_date):
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

# --------- Scenario 1: Insert ---------
process_date = "2025-12-01"
tile_data = [
    {"tile_id": "T1", "event_type": "TILE_VIEW", "user_id": "U1", "event_ts": process_date},
    {"tile_id": "T1", "event_type": "TILE_CLICK", "user_id": "U2", "event_ts": process_date},
    {"tile_id": "T2", "event_type": "TILE_VIEW", "user_id": "U3", "event_ts": process_date}
]
inter_data = [
    {"tile_id": "T1", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False, "user_id": "U1", "event_ts": process_date},
    {"tile_id": "T2", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True, "user_id": "U3", "event_ts": process_date}
]
metadata_data = [
    {"tile_id": "T1", "tile_category": "OFFERS", "is_active": True},
    {"tile_id": "T2", "tile_category": "PAYMENTS", "is_active": True}
]
df_tile = spark.createDataFrame(pd.DataFrame(tile_data))
df_inter = spark.createDataFrame(pd.DataFrame(inter_data))
df_metadata = spark.createDataFrame(pd.DataFrame(metadata_data))

df_result_insert = run_etl(df_tile, df_inter, df_metadata, process_date)
insert_output = df_result_insert.orderBy("tile_id").toPandas()

# --------- Scenario 2: Update ---------
# Simulate update: T1 already exists, new click added
# Previous day data for T1: 1 view, 1 click
# Today: add another click for T1
update_tile_data = [
    {"tile_id": "T1", "event_type": "TILE_VIEW", "user_id": "U1", "event_ts": process_date},
    {"tile_id": "T1", "event_type": "TILE_CLICK", "user_id": "U2", "event_ts": process_date},
    {"tile_id": "T1", "event_type": "TILE_CLICK", "user_id": "U3", "event_ts": process_date},
    {"tile_id": "T2", "event_type": "TILE_VIEW", "user_id": "U3", "event_ts": process_date}
]
update_inter_data = [
    {"tile_id": "T1", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False, "user_id": "U1", "event_ts": process_date},
    {"tile_id": "T2", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True, "user_id": "U3", "event_ts": process_date}
]
update_metadata_data = [
    {"tile_id": "T1", "tile_category": "OFFERS", "is_active": True},
    {"tile_id": "T2", "tile_category": "PAYMENTS", "is_active": True}
]
df_tile_upd = spark.createDataFrame(pd.DataFrame(update_tile_data))
df_inter_upd = spark.createDataFrame(pd.DataFrame(update_inter_data))
df_metadata_upd = spark.createDataFrame(pd.DataFrame(update_metadata_data))

df_result_update = run_etl(df_tile_upd, df_inter_upd, df_metadata_upd, process_date)
update_output = df_result_update.orderBy("tile_id").toPandas()

# --------- Markdown Test Report ---------
def df_to_md_table(df, columns):
    header = "| " + " | ".join(columns) + " |\n"
    sep = "|" + "----|" * len(columns) + "\n"
    rows = ""
    for _, row in df.iterrows():
        rows += "| " + " | ".join([str(row[col]) for col in columns]) + " |\n"
    return header + sep + rows

md_report = "## Test Report\n"
md_report += "### Scenario 1: Insert\nInput:\n"
md_report += df_to_md_table(pd.DataFrame(tile_data), ["tile_id", "event_type", "user_id", "event_ts"])
md_report += "Output:\n"
md_report += df_to_md_table(insert_output, ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks", "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"])

# Validate Scenario 1
expected1 = set([("T1", "OFFERS"), ("T2", "PAYMENTS")])
actual1 = set([(row["tile_id"], row["tile_category"]) for _, row in insert_output.iterrows()])
status1 = "PASS" if expected1 == actual1 else "FAIL"
md_report += f"Status: {status1}\n\n"

md_report += "### Scenario 2: Update\nInput:\n"
md_report += df_to_md_table(pd.DataFrame(update_tile_data), ["tile_id", "event_type", "user_id", "event_ts"])
md_report += "Output:\n"
md_report += df_to_md_table(update_output, ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks", "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"])

# Validate Scenario 2
expected2_clicks = update_output.query("tile_id == 'T1'")["unique_tile_clicks"].iloc[0]
status2 = "PASS" if expected2_clicks == 2 else "FAIL"
md_report += f"Status: {status2}\n\n"

print(md_report)
