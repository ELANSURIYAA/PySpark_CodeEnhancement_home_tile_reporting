"""
===============================================================================
Author: Ascendion AAVA
Date: 
Description: Enhanced ETL pipeline to enrich daily summary with tile category from SOURCE_TILE_METADATA, preserving legacy logic for audit.
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
    - Enriches daily summary with tile category
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
1.1.0       <Leave blank> Ascendion AAVA  [MODIFIED] Added tile category enrichment per SCRUM-7567
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
TARGET_DAILY_SUMMARY = "reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY"
TARGET_GLOBAL_KPIS   = "reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS"
PROCESS_DATE = "2025-12-01"   # pass dynamically from ADF/Airflow if needed

# [ADDED] SCRUM-7567: Add tile metadata table
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"

# [MODIFIED] Use Spark Connect compatible session
spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.appName("HomeTileReportingETL").getOrCreate()

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

# [ADDED] SCRUM-7567: Read tile metadata table
# Only use active tiles for enrichment
# If no match, will default to UNKNOWN
# ------------------------------------------------------------------------------
df_metadata = (
    spark.table(SOURCE_TILE_METADATA)
    .filter(F.col("is_active") == True)
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

# [DEPRECATED] Legacy join without metadata enrichment
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

# [MODIFIED] SCRUM-7567: Enhanced daily summary with metadata join
# ------------------------------------------------------------------------------
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

# [ADDED] SCRUM-7567: Join with metadata to add tile_category
# Left join ensures all summary rows are preserved
# If no metadata, tile_category defaults to UNKNOWN

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

# [ADDED] SCRUM-7567: Data quality validation after join
# Ensures no record loss during metadata enrichment

def validate_metadata_join(df_summary, df_enhanced):
    """Ensure no record loss during metadata join"""
    original_count = df_summary.count()
    enhanced_count = df_enhanced.count()
    if original_count != enhanced_count:
        raise ValueError(f"Record count mismatch: Original={original_count}, Enhanced={enhanced_count}")
    return True

validate_metadata_join(df_daily_summary, df_daily_summary_enhanced)

# ------------------------------------------------------------------------------
# GLOBAL KPIs (Unchanged)
# ------------------------------------------------------------------------------
df_global = (
    df_daily_summary_enhanced.groupBy("date")
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

# [MODIFIED] SCRUM-7567: Write enhanced summary with tile_category
overwrite_partition(df_daily_summary_enhanced, TARGET_DAILY_SUMMARY)
overwrite_partition(df_global, TARGET_GLOBAL_KPIS)

print(f"ETL completed successfully for {PROCESS_DATE}")
