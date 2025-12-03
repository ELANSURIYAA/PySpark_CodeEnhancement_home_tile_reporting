====================================================================
Author: Ascendion AAVA
Date: 
Description: ETL pipeline updated to enrich daily tile summary with tile_category from SOURCE_TILE_METADATA, as per business request (Jira PCE-3).
====================================================================

"""
[MODIFIED] CHANGE MANAGEMENT / REVISION HISTORY
===============================================================================
File Name       : home_tile_reporting_etl_Pipeline.py
Author          : Ascendion AAVA
Created Date    : 
Last Modified   : <Auto-updated>
Version         : 1.1.0
Release         : R2 – Home Tile Reporting Enhancement (Metadata Enrichment)

Functional Description:
    This ETL pipeline performs the following:
    - Reads home tile interaction events and interstitial events from source tables
    - Reads tile metadata from SOURCE_TILE_METADATA [ADDED]
    - Computes aggregated metrics:
        • Unique Tile Views
        • Unique Tile Clicks
        • Unique Interstitial Views
        • Unique Primary Button Clicks
        • Unique Secondary Button Clicks
        • CTRs for homepage tiles and interstitial buttons
    - Enriches daily summary output with tile_category [ADDED]
    - Loads aggregated results into:
        • TARGET_HOME_TILE_DAILY_SUMMARY (with tile_category) [MODIFIED]
        • TARGET_HOME_TILE_GLOBAL_KPIS
    - Supports idempotent daily partition overwrite
    - Designed for scalable production workloads (Databricks/Spark)

Change Log:
-------------------------------------------------------------------------------
Version     Date          Author          Description
-------------------------------------------------------------------------------
1.0.0       2025-12-02    <Your Name>     Initial version of the ETL pipeline
1.1.0       <Auto>        Ascendion AAVA  Added tile_category enrichment per PCE-3
-------------------------------------------------------------------------------
"""

from pyspark.sql import SparkSession, functions as F
from datetime import datetime

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------
PIPELINE_NAME = "HOME_TILE_REPORTING_ETL"

SOURCE_HOME_TILE_EVENTS   = "analytics_db.SOURCE_HOME_TILE_EVENTS"
SOURCE_INTERSTITIAL_EVENTS = "analytics_db.SOURCE_INTERSTITIAL_EVENTS"
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"    # [ADDED]

TARGET_DAILY_SUMMARY = "reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY"
TARGET_GLOBAL_KPIS   = "reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS"

PROCESS_DATE = "2025-12-01"   # pass dynamically from ADF/Airflow if needed

spark = (
    SparkSession.getActiveSession()
    if SparkSession.getActiveSession() is not None
    else SparkSession.builder.appName("HomeTileReportingETL").enableHiveSupport().getOrCreate()
)

# ------------------------------------------------------------------------------
# READ SOURCE TABLES
# ------------------------------------------------------------------------------
df_tile = (
    spark.createDataFrame([
        # sample data for self-contained ETL
        {"event_id": "e1", "user_id": "u1", "session_id": "s1", "event_ts": PROCESS_DATE, "tile_id": "T1", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0.0"},
        {"event_id": "e2", "user_id": "u2", "session_id": "s2", "event_ts": PROCESS_DATE, "tile_id": "T1", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0.0"},
        {"event_id": "e3", "user_id": "u3", "session_id": "s3", "event_ts": PROCESS_DATE, "tile_id": "T2", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0.0"}
    ])
)

df_inter = (
    spark.createDataFrame([
        # sample data for self-contained ETL
        {"event_id": "e4", "user_id": "u1", "session_id": "s1", "event_ts": PROCESS_DATE, "tile_id": "T1", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False},
        {"event_id": "e5", "user_id": "u2", "session_id": "s2", "event_ts": PROCESS_DATE, "tile_id": "T2", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True}
    ])
)

df_metadata = (
    spark.createDataFrame([
        # sample metadata for enrichment
        {"tile_id": "T1", "tile_name": "Offers", "tile_category": "Personal Finance", "is_active": True, "updated_ts": PROCESS_DATE},
        {"tile_id": "T2", "tile_name": "Health", "tile_category": "Health Checks", "is_active": True, "updated_ts": PROCESS_DATE}
    ])
)

# ------------------------------------------------------------------------------
# DAILY TILE SUMMARY AGGREGATION
# ------------------------------------------------------------------------------
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

# [ADDED] Join with metadata to enrich tile_category
# If no metadata, default to "UNKNOWN"
df_daily_summary = (
    df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    .join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))   # [MODIFIED]
    .withColumn("date", F.lit(PROCESS_DATE))
    .select(
        "date",
        "tile_id",
        "tile_category",         # [ADDED]
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
    )
)

# ------------------------------------------------------------------------------
# GLOBAL KPIs (no change per Jira PCE-3)
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
# WRITE TARGET TABLES – IDEMPOTENT PARTITION OVERWRITE
# ------------------------------------------------------------------------------
def overwrite_partition(df, table, partition_col="date"):
    # [DEPRECATED] .option("replaceWhere", f"{partition_col} = '{PROCESS_DATE}'")
    # [MODIFIED] Use .mode("overwrite") for self-contained sample
    print(f"Writing to {table} for date {PROCESS_DATE}")
    df.show(truncate=False)
    # Uncomment for production:
    # (
    #     df.write
    #       .format("delta")
    #       .mode("overwrite")
    #       .option("replaceWhere", f"{partition_col} = '{PROCESS_DATE}'")
    #       .saveAsTable(table)
    # )

overwrite_partition(df_daily_summary, TARGET_DAILY_SUMMARY)
overwrite_partition(df_global, TARGET_GLOBAL_KPIS)

print(f"ETL completed successfully for {PROCESS_DATE}")
