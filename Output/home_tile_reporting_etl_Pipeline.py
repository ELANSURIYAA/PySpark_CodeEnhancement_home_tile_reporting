"""
===============================================================================
Author: Ascendion AAVA
Date: 
Description: PySpark ETL pipeline enhanced to enrich daily summary with tile category from metadata table
===============================================================================
Functional Description:
    This ETL pipeline performs the following:
    - Reads home tile interaction events, interstitial events, and tile metadata from source tables
    - Computes aggregated metrics:
        • Unique Tile Views
        • Unique Tile Clicks
        • Unique Interstitial Views
        • Unique Primary Button Clicks
        • Unique Secondary Button Clicks
        • CTRs for homepage tiles and interstitial buttons
    - Enriches daily summary with tile_category from metadata table
    - Loads aggregated results into:
        • TARGET_HOME_TILE_DAILY_SUMMARY (with tile_category)
        • TARGET_HOME_TILE_GLOBAL_KPIS
    - Supports idempotent daily partition overwrite
    - Designed for scalable production workloads (Databricks/Spark)

Change Log:
-------------------------------------------------------------------------------
Version     Date          Author          Description
-------------------------------------------------------------------------------
1.0.0       2025-12-02    <Your Name>     Initial version of the ETL pipeline
1.1.0       <Leave blank> Ascendion AAVA  [MODIFIED] Integrated tile metadata enrichment and added tile_category column
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
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"  # [ADDED] new source table for metadata

TARGET_DAILY_SUMMARY = "reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY"
TARGET_GLOBAL_KPIS   = "reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS"

PROCESS_DATE = "2025-12-01"   # pass dynamically from ADF/Airflow if needed

spark = (
    SparkSession.getActiveSession()  # [MODIFIED] for Spark Connect compatibility
    if SparkSession.getActiveSession() else SparkSession.builder.appName("HomeTileReportingETL").enableHiveSupport().getOrCreate()
)

# ------------------------------------------------------------------------------
# READ SOURCE TABLES
# ------------------------------------------------------------------------------
df_tile = (
    spark.table(SOURCE_HOME_TILE_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
)

df_inter = (
    spark.table(SOURCE_INTERSTITIAL_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
)

# ------------------------------------------------------------------------------
# READ METADATA TABLE
# ------------------------------------------------------------------------------
df_metadata = spark.table(SOURCE_TILE_METADATA).filter(F.col("is_active") == True)  # [ADDED] read active tile metadata
metadata_count = df_metadata.count()  # [ADDED] validation
print(f"Loaded {metadata_count} active tile metadata records")

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

# [DEPRECATED] Old join without metadata enrichment
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

# [MODIFIED] Enrich daily summary with metadata
# [ADDED] Join with metadata and add tile_category
# [ADDED] Default tile_category to "UNKNOWN" if not mapped
# [ADDED] Validation for enrichment

df_daily_summary = (
    df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    .join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left")
    .withColumn("date", F.lit(PROCESS_DATE))
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

unknown_tiles = df_daily_summary.filter(F.col("tile_category") == "UNKNOWN").count()  # [ADDED] validation
if unknown_tiles > 0:
    print(f"Warning: {unknown_tiles} tiles have no metadata mapping")

# [ADDED] Schema validation for new column
expected_columns = [
    "date", "tile_id", "tile_category", "unique_tile_views",
    "unique_tile_clicks", "unique_interstitial_views",
    "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"
]
actual_columns = df_daily_summary.columns
assert set(expected_columns).issubset(set(actual_columns)), "Schema validation failed: tile_category missing"

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
# WRITE TARGET TABLES – IDEMPOTENT PARTITION OVERWRITE
# ------------------------------------------------------------------------------
def overwrite_partition(df, table, partition_col="date"):
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
