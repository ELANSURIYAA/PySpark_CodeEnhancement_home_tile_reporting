
===============================================================================
Author: Ascendion AAVA
Date: 
Description: PyTest unit test suite for enhanced ETL pipeline: validates tile metadata enrichment, aggregation logic, error handling, and edge cases.
===============================================================================

import pytest
from pyspark.sql import SparkSession, functions as F
import pandas as pd

# ------------------------------------------------------------------------------
# PyTest Fixtures
# ------------------------------------------------------------------------------
@pytest.fixture(scope='module')
def spark():
    return SparkSession.builder.master('local[1]').appName('HomeTileReportingETLTest').getOrCreate()

@pytest.fixture
def process_date():
    return '2025-12-01'

# ------------------------------------------------------------------------------
# ETL Logic (as testable function)
# ------------------------------------------------------------------------------
def run_etl(df_tile, df_inter, df_metadata, process_date):
    df_tile_agg = (
        df_tile.groupBy('tile_id')
        .agg(
            F.countDistinct(F.when(F.col('event_type') == 'TILE_VIEW', F.col('user_id'))).alias('unique_tile_views'),
            F.countDistinct(F.when(F.col('event_type') == 'TILE_CLICK', F.col('user_id'))).alias('unique_tile_clicks')
        )
    )
    df_inter_agg = (
        df_inter.groupBy('tile_id')
        .agg(
            F.countDistinct(F.when(F.col('interstitial_view_flag') == True, F.col('user_id'))).alias('unique_interstitial_views'),
            F.countDistinct(F.when(F.col('primary_button_click_flag') == True, F.col('user_id'))).alias('unique_interstitial_primary_clicks'),
            F.countDistinct(F.when(F.col('secondary_button_click_flag') == True, F.col('user_id'))).alias('unique_interstitial_secondary_clicks')
        )
    )
    df_daily_summary = (
        df_tile_agg.join(df_inter_agg, 'tile_id', 'outer')
        .withColumn('date', F.lit(process_date))
        .select(
            'date',
            'tile_id',
            F.coalesce('unique_tile_views', F.lit(0)).alias('unique_tile_views'),
            F.coalesce('unique_tile_clicks', F.lit(0)).alias('unique_tile_clicks'),
            F.coalesce('unique_interstitial_views', F.lit(0)).alias('unique_interstitial_views'),
            F.coalesce('unique_interstitial_primary_clicks', F.lit(0)).alias('unique_interstitial_primary_clicks'),
            F.coalesce('unique_interstitial_secondary_clicks', F.lit(0)).alias('unique_interstitial_secondary_clicks')
        )
    )
    df_daily_summary_enhanced = (
        df_daily_summary
        .join(df_metadata.select('tile_id', 'tile_category'), 'tile_id', 'left')
        .withColumn('tile_category', F.coalesce(F.col('tile_category'), F.lit('UNKNOWN')))
        .select(
            'date',
            'tile_id',
            'tile_category',
            'unique_tile_views',
            'unique_tile_clicks',
            'unique_interstitial_views',
            'unique_interstitial_primary_clicks',
            'unique_interstitial_secondary_clicks'
        )
    )
    return df_daily_summary_enhanced

def validate_metadata_join(df_summary, df_enhanced):
    """Ensure no record loss during metadata join"""
    original_count = df_summary.count()
    enhanced_count = df_enhanced.count()
    if original_count != enhanced_count:
        raise ValueError(f'Record count mismatch: Original={original_count}, Enhanced={enhanced_count}')
    return True

# ------------------------------------------------------------------------------
# TEST CASE INDEX & DESCRIPTION
# ------------------------------------------------------------------------------
# Test Case List:
# TC01: Happy Path - Correct aggregation and enrichment
# TC02: Edge Case - Tile missing in metadata (should default to UNKNOWN)
# TC03: Edge Case - No interstitial events for a tile
# TC04: Error Handling - Metadata join count mismatch
# TC05: Data Quality - All aggregation columns present and correct types
# TC06: Multiple Events - Correct distinct count logic
# TC07: Empty Inputs - Graceful handling of empty DataFrames

# ------------------------------------------------------------------------------
# TEST CASES
# ------------------------------------------------------------------------------

# TC01: Happy Path - Correct aggregation and enrichment
# ----------------------------------------------------
def test_happy_path_aggregation_and_enrichment(spark, process_date):
    # Setup
    tile_data = [
        {'tile_id': 'T1', 'event_type': 'TILE_VIEW', 'user_id': 'U1', 'event_ts': process_date},
        {'tile_id': 'T1', 'event_type': 'TILE_CLICK', 'user_id': 'U2', 'event_ts': process_date},
        {'tile_id': 'T2', 'event_type': 'TILE_VIEW', 'user_id': 'U3', 'event_ts': process_date}
    ]
    inter_data = [
        {'tile_id': 'T1', 'interstitial_view_flag': True, 'primary_button_click_flag': True, 'secondary_button_click_flag': False, 'user_id': 'U1', 'event_ts': process_date},
        {'tile_id': 'T2', 'interstitial_view_flag': True, 'primary_button_click_flag': False, 'secondary_button_click_flag': True, 'user_id': 'U3', 'event_ts': process_date}
    ]
    metadata_data = [
        {'tile_id': 'T1', 'tile_category': 'OFFERS', 'is_active': True},
        {'tile_id': 'T2', 'tile_category': 'PAYMENTS', 'is_active': True}
    ]
    df_tile = spark.createDataFrame(pd.DataFrame(tile_data))
    df_inter = spark.createDataFrame(pd.DataFrame(inter_data))
    df_metadata = spark.createDataFrame(pd.DataFrame(metadata_data))
    # Execute
    df_result = run_etl(df_tile, df_inter, df_metadata, process_date)
    pdf = df_result.orderBy('tile_id').toPandas()
    # Assert
    assert set(pdf['tile_category']) == {'OFFERS', 'PAYMENTS'}
    assert pdf.loc[pdf['tile_id'] == 'T1', 'unique_tile_views'].iloc[0] == 1
    assert pdf.loc[pdf['tile_id'] == 'T1', 'unique_tile_clicks'].iloc[0] == 1
    assert pdf.loc[pdf['tile_id'] == 'T1', 'unique_interstitial_primary_clicks'].iloc[0] == 1

# TC02: Edge Case - Tile missing in metadata (should default to UNKNOWN)
# ----------------------------------------------------------------------
def test_tile_missing_in_metadata_defaults_unknown(spark, process_date):
    tile_data = [
        {'tile_id': 'T3', 'event_type': 'TILE_VIEW', 'user_id': 'U4', 'event_ts': process_date}
    ]
    inter_data = [
        {'tile_id': 'T3', 'interstitial_view_flag': True, 'primary_button_click_flag': False, 'secondary_button_click_flag': False, 'user_id': 'U4', 'event_ts': process_date}
    ]
    metadata_data = [
        # T3 missing
        {'tile_id': 'T1', 'tile_category': 'OFFERS', 'is_active': True}
    ]
    df_tile = spark.createDataFrame(pd.DataFrame(tile_data))
    df_inter = spark.createDataFrame(pd.DataFrame(inter_data))
    df_metadata = spark.createDataFrame(pd.DataFrame(metadata_data))
    df_result = run_etl(df_tile, df_inter, df_metadata, process_date)
    pdf = df_result.toPandas()
    assert pdf.loc[pdf['tile_id'] == 'T3', 'tile_category'].iloc[0] == 'UNKNOWN'

# TC03: Edge Case - No interstitial events for a tile
# ---------------------------------------------------
def test_no_interstitial_events_for_tile(spark, process_date):
    tile_data = [
        {'tile_id': 'T4', 'event_type': 'TILE_VIEW', 'user_id': 'U5', 'event_ts': process_date}
    ]
    inter_data = []  # No interstitials
    metadata_data = [
        {'tile_id': 'T4', 'tile_category': 'BANNERS', 'is_active': True}
    ]
    df_tile = spark.createDataFrame(pd.DataFrame(tile_data))
    df_inter = spark.createDataFrame(pd.DataFrame(inter_data))
    df_metadata = spark.createDataFrame(pd.DataFrame(metadata_data))
    df_result = run_etl(df_tile, df_inter, df_metadata, process_date)
    pdf = df_result.toPandas()
    assert pdf.loc[pdf['tile_id'] == 'T4', 'unique_interstitial_views'].iloc[0] == 0
    assert pdf.loc[pdf['tile_id'] == 'T4', 'tile_category'].iloc[0] == 'BANNERS'

# TC04: Error Handling - Metadata join count mismatch
# ---------------------------------------------------
def test_metadata_join_count_mismatch_raises(spark, process_date):
    tile_data = [
        {'tile_id': 'T1', 'event_type': 'TILE_VIEW', 'user_id': 'U1', 'event_ts': process_date},
    ]
    inter_data = [
        {'tile_id': 'T1', 'interstitial_view_flag': True, 'primary_button_click_flag': True, 'secondary_button_click_flag': False, 'user_id': 'U1', 'event_ts': process_date}
    ]
    metadata_data = [
        {'tile_id': 'T1', 'tile_category': 'OFFERS', 'is_active': True}
    ]
    df_tile = spark.createDataFrame(pd.DataFrame(tile_data))
    df_inter = spark.createDataFrame(pd.DataFrame(inter_data))
    df_metadata = spark.createDataFrame(pd.DataFrame(metadata_data))
    # Simulate count mismatch by dropping a row after join
    df_result = run_etl(df_tile, df_inter, df_metadata, process_date)
    # Drop a row to create mismatch
    df_result = df_result.filter(F.col('tile_id') != 'T1')
    with pytest.raises(ValueError):
        validate_metadata_join(df_tile.join(df_inter, 'tile_id', 'outer'), df_result)

# TC05: Data Quality - All aggregation columns present and correct types
# ---------------------------------------------------------------------
def test_aggregation_columns_and_types(spark, process_date):
    tile_data = [
        {'tile_id': 'T5', 'event_type': 'TILE_VIEW', 'user_id': 'U6', 'event_ts': process_date}
    ]
    inter_data = [
        {'tile_id': 'T5', 'interstitial_view_flag': True, 'primary_button_click_flag': True, 'secondary_button_click_flag': True, 'user_id': 'U6', 'event_ts': process_date}
    ]
    metadata_data = [
        {'tile_id': 'T5', 'tile_category': 'PROMO', 'is_active': True}
    ]
    df_tile = spark.createDataFrame(pd.DataFrame(tile_data))
    df_inter = spark.createDataFrame(pd.DataFrame(inter_data))
    df_metadata = spark.createDataFrame(pd.DataFrame(metadata_data))
    df_result = run_etl(df_tile, df_inter, df_metadata, process_date)
    schema = df_result.schema
    expected_cols = [
        'date', 'tile_id', 'tile_category',
        'unique_tile_views', 'unique_tile_clicks',
        'unique_interstitial_views', 'unique_interstitial_primary_clicks', 'unique_interstitial_secondary_clicks'
    ]
    assert set(expected_cols) == set([f.name for f in schema.fields])
    # All metric columns should be integral type
    for col in ['unique_tile_views', 'unique_tile_clicks', 'unique_interstitial_views', 'unique_interstitial_primary_clicks', 'unique_interstitial_secondary_clicks']:
        assert str(schema[col].dataType) in ['IntegerType()', 'LongType()']

# TC06: Multiple Events - Correct distinct count logic
# ---------------------------------------------------
def test_multiple_events_distinct_count(spark, process_date):
    tile_data = [
        {'tile_id': 'T6', 'event_type': 'TILE_VIEW', 'user_id': 'U7', 'event_ts': process_date},
        {'tile_id': 'T6', 'event_type': 'TILE_VIEW', 'user_id': 'U7', 'event_ts': process_date},  # duplicate
        {'tile_id': 'T6', 'event_type': 'TILE_CLICK', 'user_id': 'U8', 'event_ts': process_date},
        {'tile_id': 'T6', 'event_type': 'TILE_CLICK', 'user_id': 'U8', 'event_ts': process_date}   # duplicate
    ]
    inter_data = [
        {'tile_id': 'T6', 'interstitial_view_flag': True, 'primary_button_click_flag': True, 'secondary_button_click_flag': False, 'user_id': 'U7', 'event_ts': process_date},
        {'tile_id': 'T6', 'interstitial_view_flag': True, 'primary_button_click_flag': True, 'secondary_button_click_flag': False, 'user_id': 'U7', 'event_ts': process_date}  # duplicate
    ]
    metadata_data = [
        {'tile_id': 'T6', 'tile_category': 'RECOMMENDATION', 'is_active': True}
    ]
    df_tile = spark.createDataFrame(pd.DataFrame(tile_data))
    df_inter = spark.createDataFrame(pd.DataFrame(inter_data))
    df_metadata = spark.createDataFrame(pd.DataFrame(metadata_data))
    df_result = run_etl(df_tile, df_inter, df_metadata, process_date)
    pdf = df_result.toPandas()
    # Only 1 distinct view/click per user
    assert pdf.loc[pdf['tile_id'] == 'T6', 'unique_tile_views'].iloc[0] == 1
    assert pdf.loc[pdf['tile_id'] == 'T6', 'unique_tile_clicks'].iloc[0] == 1
    assert pdf.loc[pdf['tile_id'] == 'T6', 'unique_interstitial_primary_clicks'].iloc[0] == 1

# TC07: Empty Inputs - Graceful handling of empty DataFrames
# ----------------------------------------------------------
def test_empty_inputs_handling(spark, process_date):
    df_tile = spark.createDataFrame([], schema='tile_id string, event_type string, user_id string, event_ts string')
    df_inter = spark.createDataFrame([], schema='tile_id string, interstitial_view_flag boolean, primary_button_click_flag boolean, secondary_button_click_flag boolean, user_id string, event_ts string')
    df_metadata = spark.createDataFrame([], schema='tile_id string, tile_category string, is_active boolean')
    df_result = run_etl(df_tile, df_inter, df_metadata, process_date)
    assert df_result.count() == 0

# ------------------------------------------------------------------------------
# End of Test Suite
# ------------------------------------------------------------------------------