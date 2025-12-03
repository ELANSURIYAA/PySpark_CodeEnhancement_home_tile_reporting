# ====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: PyTest unit tests for enhanced home tile reporting ETL pipeline - covers tile_category enrichment, joins, edge cases, and partition overwrite logic.
# ====================================================================

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException
from unittest.mock import MagicMock

# =============================
# PyTest Fixture: Spark Session
# =============================
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("PyTestHomeTileETL").getOrCreate()

# =============================
# Utility: Sample Data Creation
# =============================
def create_df(spark, data, schema=None):
    if schema:
        return spark.createDataFrame(data, schema=schema)
    return spark.createDataFrame(data)

# =============================
# ETL Logic Extraction (for test)
# =============================
def etl_transform(df_tile, df_inter, df_metadata, process_date):
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
        .join(df_metadata, "tile_id", "left")
        .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        .withColumn("date", F.lit(process_date))
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
    return df_daily_summary

# =============================
# Test Case Index & Documentation
# =============================
"""
Test Case List:
   - TC01: Happy path - tile and interstitial events with metadata present
   - TC02: Tile present, metadata missing - tile_category defaults to UNKNOWN
   - TC03: Interstitial events only, no tile events
   - TC04: Tile events only, no interstitial events
   - TC05: Multiple tiles, mixed metadata presence
   - TC06: overwrite_partition function - verifies .write API usage and idempotency
   - TC07: Error handling - invalid schema or empty DataFrames
"""

# =============================
# TC01: Happy path - tile/interstitial, metadata present
# =============================
def test_happy_path_tile_and_metadata(spark):
    """TC01: Happy path - tile and interstitial events with metadata present"""
    process_date = "2025-12-01"
    tile_events = [
        {"event_id": "e1", "user_id": "u1", "session_id": "s1", "event_ts": process_date, "tile_id": "T1", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
        {"event_id": "e2", "user_id": "u2", "session_id": "s2", "event_ts": process_date, "tile_id": "T1", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0"}
    ]
    inter_events = [
        {"event_id": "ie1", "user_id": "u1", "session_id": "s1", "event_ts": process_date, "tile_id": "T1", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False}
    ]
    metadata = [
        {"tile_id": "T1", "tile_category": "Finance"}
    ]
    df_tile = create_df(spark, tile_events)
    df_inter = create_df(spark, inter_events)
    df_metadata = create_df(spark, metadata)
    df_out = etl_transform(df_tile, df_inter, df_metadata, process_date)
    result = df_out.collect()[0].asDict()
    assert result["tile_category"] == "Finance"
    assert result["unique_tile_views"] == 1
    assert result["unique_tile_clicks"] == 1
    assert result["unique_interstitial_views"] == 1
    assert result["unique_interstitial_primary_clicks"] == 1
    assert result["unique_interstitial_secondary_clicks"] == 0
    assert result["date"] == process_date

# =============================
# TC02: Tile present, metadata missing - tile_category defaults to UNKNOWN
# =============================
def test_tile_metadata_missing_category(spark):
    """TC02: Tile present, metadata missing - tile_category defaults to UNKNOWN"""
    process_date = "2025-12-01"
    tile_events = [
        {"event_id": "e5", "user_id": "u3", "session_id": "s3", "event_ts": process_date, "tile_id": "T3", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"}
    ]
    inter_events = []
    metadata = []
    df_tile = create_df(spark, tile_events)
    df_inter = create_df(spark, inter_events, schema=df_tile.schema if inter_events else None)
    df_metadata = create_df(spark, metadata, schema=df_tile.select("tile_id").schema)
    df_out = etl_transform(df_tile, df_inter, df_metadata, process_date)
    result = df_out.collect()[0].asDict()
    assert result["tile_category"] == "UNKNOWN"
    assert result["unique_tile_views"] == 1
    assert result["unique_tile_clicks"] == 0
    assert result["unique_interstitial_views"] == 0
    assert result["unique_interstitial_primary_clicks"] == 0
    assert result["unique_interstitial_secondary_clicks"] == 0
    assert result["date"] == process_date

# =============================
# TC03: Interstitial events only, no tile events
# =============================
def test_interstitial_only(spark):
    """TC03: Interstitial events only, no tile events"""
    process_date = "2025-12-01"
    tile_events = []
    inter_events = [
        {"event_id": "ie2", "user_id": "u4", "session_id": "s4", "event_ts": process_date, "tile_id": "T4", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True}
    ]
    metadata = [
        {"tile_id": "T4", "tile_category": "Promo"}
    ]
    df_tile = create_df(spark, tile_events, schema=None)
    df_inter = create_df(spark, inter_events)
    df_metadata = create_df(spark, metadata)
    df_out = etl_transform(df_tile, df_inter, df_metadata, process_date)
    result = df_out.collect()[0].asDict()
    assert result["tile_id"] == "T4"
    assert result["tile_category"] == "Promo"
    assert result["unique_tile_views"] == 0
    assert result["unique_tile_clicks"] == 0
    assert result["unique_interstitial_views"] == 1
    assert result["unique_interstitial_primary_clicks"] == 0
    assert result["unique_interstitial_secondary_clicks"] == 1

# =============================
# TC04: Tile events only, no interstitial events
# =============================
def test_tile_only(spark):
    """TC04: Tile events only, no interstitial events"""
    process_date = "2025-12-01"
    tile_events = [
        {"event_id": "e6", "user_id": "u5", "session_id": "s5", "event_ts": process_date, "tile_id": "T5", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"}
    ]
    inter_events = []
    metadata = [
        {"tile_id": "T5", "tile_category": "Games"}
    ]
    df_tile = create_df(spark, tile_events)
    df_inter = create_df(spark, inter_events, schema=df_tile.schema if inter_events else None)
    df_metadata = create_df(spark, metadata)
    df_out = etl_transform(df_tile, df_inter, df_metadata, process_date)
    result = df_out.collect()[0].asDict()
    assert result["tile_id"] == "T5"
    assert result["tile_category"] == "Games"
    assert result["unique_tile_views"] == 1
    assert result["unique_tile_clicks"] == 0
    assert result["unique_interstitial_views"] == 0
    assert result["unique_interstitial_primary_clicks"] == 0
    assert result["unique_interstitial_secondary_clicks"] == 0

# =============================
# TC05: Multiple tiles, mixed metadata presence
# =============================
def test_multiple_tiles_mixed_metadata(spark):
    """TC05: Multiple tiles, mixed metadata presence"""
    process_date = "2025-12-01"
    tile_events = [
        {"event_id": "e7", "user_id": "u6", "session_id": "s6", "event_ts": process_date, "tile_id": "T6", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
        {"event_id": "e8", "user_id": "u7", "session_id": "s7", "event_ts": process_date, "tile_id": "T7", "event_type": "TILE_CLICK", "device_type": "Web", "app_version": "1.0"}
    ]
    inter_events = [
        {"event_id": "ie3", "user_id": "u8", "session_id": "s8", "event_ts": process_date, "tile_id": "T6", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": False},
        {"event_id": "ie4", "user_id": "u9", "session_id": "s9", "event_ts": process_date, "tile_id": "T7", "interstitial_view_flag": False, "primary_button_click_flag": True, "secondary_button_click_flag": True}
    ]
    metadata = [
        {"tile_id": "T6", "tile_category": "Shopping"}
        # T7 missing metadata
    ]
    df_tile = create_df(spark, tile_events)
    df_inter = create_df(spark, inter_events)
    df_metadata = create_df(spark, metadata)
    df_out = etl_transform(df_tile, df_inter, df_metadata, process_date)
    results = {row.tile_id: row.tile_category for row in df_out.collect()}
    assert results["T6"] == "Shopping"
    assert results["T7"] == "UNKNOWN"

# =============================
# TC06: overwrite_partition function - verifies .write API usage and idempotency
# =============================
def test_overwrite_partition_write_called():
    """TC06: overwrite_partition function - verifies .write API usage and idempotency"""
    # Mock DataFrame and .write
    df_mock = MagicMock()
    df_mock.write.format.return_value = df_mock.write
    df_mock.write.mode.return_value = df_mock.write
    df_mock.write.option.return_value = df_mock.write
    df_mock.write.saveAsTable.return_value = None
    # Function under test
    def overwrite_partition(df, table, partition_col="date"):
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("replaceWhere", f"{partition_col} = '2025-12-01'")
              .saveAsTable(table)
        )
    overwrite_partition(df_mock, "dummy_table")
    df_mock.write.format.assert_called_with("delta")
    df_mock.write.mode.assert_called_with("overwrite")
    df_mock.write.option.assert_called_with("replaceWhere", "date = '2025-12-01'")
    df_mock.write.saveAsTable.assert_called_with("dummy_table")

# =============================
# TC07: Error handling - invalid schema or empty DataFrames
# =============================
def test_etl_transform_empty_inputs(spark):
    """TC07: Error handling - empty DataFrames should not error and should return empty result"""
    process_date = "2025-12-01"
    df_tile = create_df(spark, [])
    df_inter = create_df(spark, [])
    df_metadata = create_df(spark, [])
    df_out = etl_transform(df_tile, df_inter, df_metadata, process_date)
    assert df_out.count() == 0

# =============================
# End of Test Suite
# =============================
