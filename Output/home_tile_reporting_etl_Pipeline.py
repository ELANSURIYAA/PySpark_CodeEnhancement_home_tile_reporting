'''
====================================================================
Author: Ascendion AAVA
Date: 
Description: PySpark ETL pipeline for home tile reporting, now enriched with tile_category via join to SOURCE_TILE_METADATA. Backward compatibility and clear delta annotations included.
====================================================================

CHANGE MANAGEMENT / REVISION HISTORY
-------------------------------------------------------------------------------
File Name       : home_tile_reporting_etl_Pipeline.py
Author          : Ascendion AAVA
Created Date    : 
Last Modified   : <Auto-updated>
Version         : 2.0.0
Release         : R2 – Tile Category Enrichment

Functional Description:
    - Reads home tile interaction events, interstitial events, and tile metadata
    - Aggregates metrics at tile level and enriches output with tile_category
    - Writes results to target summary table with new tile_category column
    - Maintains backward compatibility (defaults category to 'UNKNOWN')
    - All delta changes are annotated
-------------------------------------------------------------------------------
Version     Date          Author          Description
-------------------------------------------------------------------------------
1.0.0       2025-12-02    <Your Name>    Initial version of the ETL pipeline
2.0.0       <Today>       Ascendion AAVA Add tile_category enrichment per PCE-3
-------------------------------------------------------------------------------
'''

from pyspark.sql import SparkSession, functions as F
from datetime import datetime

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------
PIPELINE_NAME = "HOME_TILE_REPORTING_ETL"

SOURCE_HOME_TILE_EVENTS   = "analytics_db.SOURCE_HOME_TILE_EVENTS"
SOURCE_INTERSTITIAL_EVENTS = "analytics_db.SOURCE_INTERSTITIAL_EVENTS"
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"  # [ADDED] New metadata source

TARGET_DAILY_SUMMARY = "reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY"
TARGET_GLOBAL_KPIS   = "reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS"

PROCESS_DATE = "2025-12-01"   # pass dynamically from ADF/Airflow if needed

spark = (
    SparkSession.getActiveSession()
    if SparkSession.getActiveSession() else SparkSession.builder.appName("HomeTileReportingETL").enableHiveSupport().getOrCreate()
)

# ------------------------------------------------------------------------------
# READ SOURCE TABLES
# ------------------------------------------------------------------------------
df_tile = (
    spark.createDataFrame([
        {"event_id": "e1", "user_id": "u1", "session_id": "s1", "event_ts": "2025-12-01 08:00:00", "tile_id": "TILE1", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
        {"event_id": "e2", "user_id": "u2", "session_id": "s2", "event_ts": "2025-12-01 08:01:00", "tile_id": "TILE1", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0"},
        {"event_id": "e3", "user_id": "u3", "session_id": "s3", "event_ts": "2025-12-01 08:02:00", "tile_id": "TILE2", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"}
    ])
    .withColumn("event_ts", F.to_timestamp("event_ts"))
)

# [MODIFIED] Simulate interstitial events

df_inter = (
    spark.createDataFrame([
        {"event_id": "i1", "user_id": "u1", "session_id": "s1", "event_ts": "2025-12-01 08:05:00", "tile_id": "TILE1", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True},
        {"event_id": "i2", "user_id": "u2", "session_id": "s2", "event_ts": "2025-12-01 08:06:00", "tile_id": "TILE2", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False}
    ])
    .withColumn("event_ts", F.to_timestamp("event_ts"))
)

# [ADDED] Simulate tile metadata

df_tile_metadata = (
    spark.createDataFrame([
        {"tile_id": "TILE1", "tile_name": "Payments", "tile_category": "Personal Finance", "is_active": True, "updated_ts": "2025-11-30 12:00:00"},
        {"tile_id": "TILE2", "tile_name": "Health Check", "tile_category": "Health", "is_active": True, "updated_ts": "2025-11-30 12:01:00"}
    ])
    .withColumn("updated_ts", F.to_timestamp("updated_ts"))
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

# [ADDED] Enrich with tile_category from metadata
# Left join to ensure backward compatibility (default to 'UNKNOWN' if not found)
df_daily_summary = (
    df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    .join(df_tile_metadata.select("tile_id", "tile_category"), "tile_id", "left")
    .withColumn("date", F.lit(PROCESS_DATE))
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))  # [ADDED] Default if missing
    .select(
        "date",
        "tile_id",
        "tile_category",  # [ADDED] New column in output
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
    )
)

# [DEPRECATED] Old join without tile_category (commented out for traceability)
# df_daily_summary = (
#     df_tile_agg.join(df_inter_agg, "tile_id", "outer")
#     .withColumn("date", F.lit(PROCESS_DATE))
#     .select(
#         "date",
#         "tile_id",
#         F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
#         F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
#         F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
#         F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
#         F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
#     )
# )

# ------------------------------------------------------------------------------
# GLOBAL KPIs
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
    print(f"[LOG] Writing to {table} for {PROCESS_DATE}")
    # [NOTE] In Databricks, use .saveAsTable for Delta
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("replaceWhere", f"{partition_col} = '{PROCESS_DATE}'")
          .saveAsTable(table)
    )

overwrite_partition(df_daily_summary, TARGET_DAILY_SUMMARY)
overwrite_partition(df_global, TARGET_GLOBAL_KPIS)

print(f"ETL completed successfully for {PROCESS_DATE}")

# ------------------------------------------------------------------------------
# Summary of Updation in this Version
# ------------------------------------------------------------------------------
print("""
====================================================================
Summary of Updation in Version 2.0.0:
- Added left join to SOURCE_TILE_METADATA to enrich tile_category
- Updated target table output to include tile_category (default 'UNKNOWN' if missing)
- Deprecated old join logic (commented out)
- All delta changes annotated with [ADDED], [MODIFIED], [DEPRECATED]
- Code is self-contained, uses sample data for all sources
====================================================================
""")
