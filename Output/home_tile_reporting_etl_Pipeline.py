"""
===============================================================================
Author: Ascendion AAVA
Date: 
Description: Self-contained PySpark ETL pipeline for Home Tile Reporting with metadata enrichment
===============================================================================
File Name       : home_tile_reporting_etl_Pipeline.py
Functional Description:
    - Reads sample home tile events, interstitial events, and tile metadata from DataFrames
    - Computes aggregated metrics and joins with metadata for enrichment
    - Writes enriched results to Delta tables (simulated)
    - Includes error handling and validation
    - All legacy logic retained and commented where superseded
-------------------------------------------------------------------------------
Version     Date          Author          Description
-------------------------------------------------------------------------------
2.0.0       <Leave blank> Ascendion AAVA  Added tile metadata enrichment per SCRUM-7567
-------------------------------------------------------------------------------
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import logging

# ------------------------------------------------------------------------------
# LOGGING CONFIGURATION
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("home_tile_reporting_etl")

# ------------------------------------------------------------------------------
# SPARK SESSION (Databricks Spark Connect compatible)
# ------------------------------------------------------------------------------
spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.appName("HomeTileReportingETL").getOrCreate()

# ------------------------------------------------------------------------------
# SAMPLE DATA FOR TESTING (Simulates Delta tables)
# ------------------------------------------------------------------------------
# [ADDED] Sample Home Tile Events
home_tile_events_data = [
    {"event_id": "E1", "user_id": "U1", "session_id": "S1", "event_ts": "2025-12-01 09:00:00", "tile_id": "TILE_001", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
    {"event_id": "E2", "user_id": "U2", "session_id": "S2", "event_ts": "2025-12-01 09:05:00", "tile_id": "TILE_002", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0"},
    {"event_id": "E3", "user_id": "U1", "session_id": "S1", "event_ts": "2025-12-01 09:10:00", "tile_id": "TILE_001", "event_type": "TILE_CLICK", "device_type": "Mobile", "app_version": "1.0"},
]
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

# [ADDED] Sample Interstitial Events
interstitial_events_data = [
    {"event_id": "IE1", "user_id": "U1", "session_id": "S1", "event_ts": "2025-12-01 09:00:00", "tile_id": "TILE_001", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False},
    {"event_id": "IE2", "user_id": "U2", "session_id": "S2", "event_ts": "2025-12-01 09:10:00", "tile_id": "TILE_002", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True},
]
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

# [ADDED] Sample Tile Metadata
tile_metadata_data = [
    {"tile_id": "TILE_001", "tile_name": "Credit Score Check", "tile_category": "Personal Finance", "is_active": True, "updated_ts": "2025-12-01 08:00:00"},
    {"tile_id": "TILE_002", "tile_name": "Health Assessment", "tile_category": "Health & Wellness", "is_active": True, "updated_ts": "2025-12-01 08:00:00"},
]
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

PROCESS_DATE = "2025-12-01"

# ------------------------------------------------------------------------------
# AGGREGATION LOGIC
# ------------------------------------------------------------------------------
# [MODIFIED] - Legacy aggregation preserved below
# df_tile_agg = (
#     df_tile.groupBy("tile_id")
#     .agg(
#         F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
#         F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
#     )
# )

# [ADDED] - Enhanced aggregation with metadata enrichment
# --- Tile Aggregation ---
df_tile_agg = (
    df_tile.groupBy("tile_id")
    .agg(
        F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
        F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
    )
)

# --- Interstitial Aggregation ---
df_inter_agg = (
    df_inter.groupBy("tile_id")
    .agg(
        F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
        F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
        F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
    )
)

# --- Join Aggregations ---
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

# [ADDED] Metadata enrichment join
# - LEFT JOIN with metadata table
# - Default tile_category to "UNKNOWN" and tile_name to "Unknown Tile" if missing

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

# ------------------------------------------------------------------------------
# GLOBAL KPIs (unchanged)
# ------------------------------------------------------------------------------
df_global = (
    df_daily_summary.groupBy("date")
    .agg(
        F.sum("unique_tile_views").alias("total_tile_views"),
        F.sum("unique_tile_clicks").alias("total_tile_clicks"),
        F.sum("unique_interstitial_views").alias("total_interstitial_views"),
        F.sum("unique_interstitial_primary_clicks").alias("total_primary_clicks"),
        F.sum("unique_interstitial_secondary_clicks").alias("total_secondary_clicks")
    )
    .withColumn(
        "overall_ctr",
        F.when(F.col("total_tile_views") > 0,
               F.col("total_tile_clicks") / F.col("total_tile_views")).otherwise(0.0)
    )
    .withColumn(
        "overall_primary_ctr",
        F.when(F.col("total_interstitial_views") > 0,
               F.col("total_primary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
    )
    .withColumn(
        "overall_secondary_ctr",
        F.when(F.col("total_interstitial_views") > 0,
               F.col("total_secondary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
    )
)

# ------------------------------------------------------------------------------
# WRITE TARGET TABLES – SIMULATED DELTA WRITE
# ------------------------------------------------------------------------------
def overwrite_partition(df, table, partition_col="date"):
    logger.info(f"Writing to {table} for {partition_col} = {PROCESS_DATE}")
    df.show(truncate=False)
    # [SIMULATED] Delta write
    # df.write.format("delta").mode("overwrite").option("replaceWhere", f"{partition_col} = '{PROCESS_DATE}'").saveAsTable(table)

# [MODIFIED] Use enriched summary for daily summary target
overwrite_partition(df_daily_summary_enriched, "reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY")
# [UNCHANGED] Global KPIs logic
overwrite_partition(df_global, "reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS")

logger.info(f"ETL completed successfully for {PROCESS_DATE}")

# ------------------------------------------------------------------------------
# VALIDATION & ERROR HANDLING
# ------------------------------------------------------------------------------
# [ADDED] Metadata table validation
try:
    metadata_count = df_metadata.count()
    logger.info(f"Metadata table contains {metadata_count} records")
except Exception as e:
    logger.warning(f"Metadata table not available: {e}")

# ------------------------------------------------------------------------------
# END OF SCRIPT
# ------------------------------------------------------------------------------
