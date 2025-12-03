'''
====================================================================
Author: Ascendion AAVA
Date: 
Description: Home Tile Reporting ETL pipeline with tile_category enrichment from SOURCE_TILE_METADATA, per Jira PCE-3
====================================================================

Functional Description:
    This ETL pipeline performs the following:
    - Reads home tile interaction events and interstitial events from source tables
    - Reads tile metadata from SOURCE_TILE_METADATA
    - Computes aggregated metrics:
        • Unique Tile Views
        • Unique Tile Clicks
        • Unique Interstitial Views
        • Unique Primary Button Clicks
        • Unique Secondary Button Clicks
        • CTRs for homepage tiles and interstitial buttons
    - Enriches daily summary output with tile_category from metadata
    - Loads aggregated results into:
        • TARGET_HOME_TILE_DAILY_SUMMARY (now includes tile_category)
        • TARGET_HOME_TILE_GLOBAL_KPIS (unchanged)
    - Supports idempotent daily partition overwrite
    - Designed for scalable production workloads (Databricks/Spark)

Change Log:
--------------------------------------------------------------------
Version     Date          Author            Description
--------------------------------------------------------------------
1.0.0       2025-12-02    <Your Name>       Initial version of the ETL pipeline
1.1.0       <Auto>        Ascendion AAVA    [MODIFIED] Enrich daily summary with tile_category (PCE-3)
--------------------------------------------------------------------
'''

from pyspark.sql import SparkSession, functions as F
from datetime import datetime

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------
PIPELINE_NAME = "HOME_TILE_REPORTING_ETL"

SOURCE_HOME_TILE_EVENTS   = "analytics_db.SOURCE_HOME_TILE_EVENTS"
SOURCE_INTERSTITIAL_EVENTS = "analytics_db.SOURCE_INTERSTITIAL_EVENTS"
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"  # [ADDED] Tile metadata for enrichment

TARGET_DAILY_SUMMARY = "reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY"
TARGET_GLOBAL_KPIS   = "reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS"

PROCESS_DATE = "2025-12-01"   # pass dynamically from ADF/Airflow if needed

spark = (
    SparkSession.getActiveSession()
    .builder.appName("HomeTileReportingETL")
    .enableHiveSupport()
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

df_tile_metadata = (
    spark.table(SOURCE_TILE_METADATA)
    .select("tile_id", "tile_category")  # [ADDED] Only columns needed for join
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

# [MODIFIED] Outer join with tile metadata to enrich with tile_category
# [MODIFIED] Default tile_category to 'UNKNOWN' if not found

df_daily_summary = (
    df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    .join(df_tile_metadata, "tile_id", "left")  # [ADDED] Join with metadata
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))  # [ADDED] Default
    .withColumn("date", F.lit(PROCESS_DATE))
    .select(
        "date",
        "tile_id",
        "tile_category",  # [ADDED] Output enriched category
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
    )
)

# ------------------------------------------------------------------------------
# GLOBAL KPIs (NO CHANGE)
# ------------------------------------------------------------------------------
# [NO CHANGE] per Jira PCE-3, global KPI table is not changed

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

# [DEPRECATED] Previous version of df_daily_summary without tile_category enrichment
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
# [END DEPRECATED]
