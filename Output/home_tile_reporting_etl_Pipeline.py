"""
===============================================================================
Author: Ascendion AAVA
Date: 
Description: PySpark ETL pipeline for home tile reporting with tile category enrichment via metadata join
===============================================================================
Functional Description:
    - Reads sample Delta tables for home tile events, interstitial events, and tile metadata
    - Computes aggregated metrics (unique views/clicks, interstitial interactions)
    - Enriches daily summary with tile_category from metadata
    - Writes results to Delta tables with idempotent partition overwrite
    - Includes validation for metadata join integrity

Change Log:
-------------------------------------------------------------------------------
Version     Date          Author          Description
-------------------------------------------------------------------------------
1.0.0       2025-12-02    <Your Name>     Initial version of the ETL pipeline
1.1.0       <Leave blank> Ascendion AAVA  [MODIFIED] Added tile_category enrichment per SCRUM-7567
-------------------------------------------------------------------------------
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from datetime import datetime

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------
PIPELINE_NAME = "HOME_TILE_REPORTING_ETL"
PROCESS_DATE = "2025-12-01"

# Table names (simulated for self-contained script)
SOURCE_HOME_TILE_EVENTS   = "SOURCE_HOME_TILE_EVENTS"
SOURCE_INTERSTITIAL_EVENTS = "SOURCE_INTERSTITIAL_EVENTS"
SOURCE_TILE_METADATA      = "SOURCE_TILE_METADATA"
TARGET_DAILY_SUMMARY      = "TARGET_HOME_TILE_DAILY_SUMMARY"
TARGET_GLOBAL_KPIS        = "TARGET_HOME_TILE_GLOBAL_KPIS"

spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.appName("HomeTileReportingETL").getOrCreate()

# ------------------------------------------------------------------------------
# SAMPLE DATA CREATION (self-contained, no external tables)
# ------------------------------------------------------------------------------
# [ADDED] Sample data for SOURCE_HOME_TILE_EVENTS
sample_tile_events = [
    ("e1", "u1", "s1", "2025-12-01 10:00:00", "TILE01", "TILE_VIEW", "Mobile", "1.0.0"),
    ("e2", "u2", "s2", "2025-12-01 10:01:00", "TILE01", "TILE_CLICK", "Web", "1.0.0"),
    ("e3", "u3", "s3", "2025-12-01 10:02:00", "TILE02", "TILE_VIEW", "Mobile", "1.0.0"),
    ("e4", "u4", "s4", "2025-12-01 10:03:00", "TILE02", "TILE_CLICK", "Web", "1.0.0"),
    ("e5", "u5", "s5", "2025-12-01 10:04:00", "TILE03", "TILE_VIEW", "Mobile", "1.0.0")
]
schema_tile_events = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("tile_id", StringType()),
    StructField("event_type", StringType()),
    StructField("device_type", StringType()),
    StructField("app_version", StringType())
])
df_tile = spark.createDataFrame(sample_tile_events, schema=schema_tile_events)\
    .withColumn("event_ts", F.to_timestamp("event_ts"))\
    .filter(F.to_date("event_ts") == PROCESS_DATE)

# [ADDED] Sample data for SOURCE_INTERSTITIAL_EVENTS
sample_interstitial_events = [
    ("e10", "u1", "s1", "2025-12-01 11:00:00", "TILE01", True, True, False),
    ("e11", "u2", "s2", "2025-12-01 11:01:00", "TILE01", True, False, True),
    ("e12", "u3", "s3", "2025-12-01 11:02:00", "TILE02", True, True, False),
    ("e13", "u4", "s4", "2025-12-01 11:03:00", "TILE02", False, False, False),
    ("e14", "u5", "s5", "2025-12-01 11:04:00", "TILE03", True, False, True)
]
schema_interstitial_events = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("tile_id", StringType()),
    StructField("interstitial_view_flag", BooleanType()),
    StructField("primary_button_click_flag", BooleanType()),
    StructField("secondary_button_click_flag", BooleanType())
])
df_inter = spark.createDataFrame(sample_interstitial_events, schema=schema_interstitial_events)\
    .withColumn("event_ts", F.to_timestamp("event_ts"))\
    .filter(F.to_date("event_ts") == PROCESS_DATE)

# [ADDED] Sample data for SOURCE_TILE_METADATA
sample_tile_metadata = [
    ("TILE01", "Finance Tile", "Personal Finance", True, "2025-11-30 12:00:00"),
    ("TILE02", "Health Tile", "Health Check", True, "2025-11-30 12:01:00"),
    ("TILE03", "Promo Tile", None, True, "2025-11-30 12:02:00"),
    ("TILE04", "Inactive Tile", "Offers", False, "2025-11-30 12:03:00")
]
schema_tile_metadata = StructType([
    StructField("tile_id", StringType()),
    StructField("tile_name", StringType()),
    StructField("tile_category", StringType()),
    StructField("is_active", BooleanType()),
    StructField("updated_ts", StringType())
])
df_metadata = spark.createDataFrame(sample_tile_metadata, schema=schema_tile_metadata)\
    .withColumn("updated_ts", F.to_timestamp("updated_ts"))\
    .filter(F.col("is_active") == True)

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

# ------------------------------------------------------------------------------
# [MODIFIED] METADATA JOIN FOR TILE CATEGORY ENRICHMENT
# ------------------------------------------------------------------------------
df_daily_summary_enriched = (
    df_daily_summary.join(df_metadata, "tile_id", "left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))) # [ADDED] Default to UNKNOWN
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

# ------------------------------------------------------------------------------
# [ADDED] DATA QUALITY VALIDATION FOR METADATA JOIN
# ------------------------------------------------------------------------------
def validate_metadata_join(df_original, df_enriched):
    """
    Validate that metadata join doesn't cause data loss
    """
    original_count = df_original.count()
    enriched_count = df_enriched.count()
    if original_count != enriched_count:
        raise Exception(f"Data loss detected: Original={original_count}, Enriched={enriched_count}")
    return df_enriched

df_daily_summary_enriched = validate_metadata_join(df_daily_summary, df_daily_summary_enriched)

# ------------------------------------------------------------------------------
# GLOBAL KPIs (unchanged)
# ------------------------------------------------------------------------------
df_global = (
    df_daily_summary_enriched.groupBy("date")
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
    print(f"[INFO] Writing to {table} for {PROCESS_DATE}")
    # [SIMULATED] Instead of writing to Delta, show DataFrame
    df.show(truncate=False)
    # Uncomment below for actual Delta write in Databricks
    # df.write.format("delta").mode("overwrite").option("replaceWhere", f"{partition_col} = '{PROCESS_DATE}'").saveAsTable(table)

# [MODIFIED] Write enriched daily summary
overwrite_partition(df_daily_summary_enriched, TARGET_DAILY_SUMMARY)
# [DEPRECATED] overwrite_partition(df_daily_summary, TARGET_DAILY_SUMMARY) # [DEPRECATED] Replaced by enriched version

# [UNCHANGED] Write global KPIs
overwrite_partition(df_global, TARGET_GLOBAL_KPIS)

print(f"ETL completed successfully for {PROCESS_DATE}")
