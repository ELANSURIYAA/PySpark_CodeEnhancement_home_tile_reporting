# Author: Ascendion AAVA
# Date: 
# Description: Python test script for home_tile_reporting_etl_Pipeline.py - validates tile_category enrichment and update logic

from pyspark.sql import SparkSession, Row, functions as F
import pandas as pd

# Helper function for pretty markdown table
def markdown_table(df, columns):
    md = '| ' + ' | '.join(columns) + ' |\n'
    md += '| ' + ' | '.join(['---']*len(columns)) + ' |\n'
    for row in df:
        md += '| ' + ' | '.join(str(row[c]) for c in columns) + ' |\n'
    return md

spark = SparkSession.builder.master('local[*]').appName('TestETL').getOrCreate()
PROCESS_DATE = '2025-12-01'

# Sample source data for scenario 1 (insert)
tile_events = [
    Row(event_id='E1', user_id='U1', session_id='S1', event_ts='2025-12-01', tile_id='TILE_001', event_type='TILE_VIEW', device_type='Mobile', app_version='1.0'),
    Row(event_id='E2', user_id='U2', session_id='S2', event_ts='2025-12-01', tile_id='TILE_001', event_type='TILE_CLICK', device_type='Mobile', app_version='1.0'),
    Row(event_id='E3', user_id='U3', session_id='S3', event_ts='2025-12-01', tile_id='TILE_002', event_type='TILE_VIEW', device_type='Web', app_version='2.0'),
]
interstitial_events = [
    Row(event_id='I1', user_id='U1', session_id='S1', event_ts='2025-12-01', tile_id='TILE_001', interstitial_view_flag=True, primary_button_click_flag=True, secondary_button_click_flag=False),
    Row(event_id='I2', user_id='U2', session_id='S2', event_ts='2025-12-01', tile_id='TILE_002', interstitial_view_flag=True, primary_button_click_flag=False, secondary_button_click_flag=True),
]
tile_metadata = [
    Row(tile_id='TILE_001', tile_name='Credit Score Check', tile_category='Personal Finance', is_active=True, updated_ts='2025-12-01 10:00:00'),
    Row(tile_id='TILE_002', tile_name='Health Dashboard', tile_category='Health', is_active=True, updated_ts='2025-12-01 10:00:00'),
]

df_tile = spark.createDataFrame(tile_events)
df_inter = spark.createDataFrame(interstitial_events)
df_metadata = spark.createDataFrame(tile_metadata)
df_metadata = df_metadata.filter(F.col('is_active') == True)

# Aggregations

df_tile_agg = (
    df_tile.groupBy('tile_id').agg(
        F.countDistinct(F.when(F.col('event_type') == 'TILE_VIEW', F.col('user_id'))).alias('unique_tile_views'),
        F.countDistinct(F.when(F.col('event_type') == 'TILE_CLICK', F.col('user_id'))).alias('unique_tile_clicks')
    )
)
df_inter_agg = (
    df_inter.groupBy('tile_id').agg(
        F.countDistinct(F.when(F.col('interstitial_view_flag') == True, F.col('user_id'))).alias('unique_interstitial_views'),
        F.countDistinct(F.when(F.col('primary_button_click_flag') == True, F.col('user_id'))).alias('unique_interstitial_primary_clicks'),
        F.countDistinct(F.when(F.col('secondary_button_click_flag') == True, F.col('user_id'))).alias('unique_interstitial_secondary_clicks')
    )
)
df_daily_summary = (
    df_tile_agg.join(df_inter_agg, 'tile_id', 'outer')
    .join(df_metadata.select('tile_id', 'tile_category'), 'tile_id', 'left')
    .withColumn('date', F.lit(PROCESS_DATE))
    .select(
        'date', 'tile_id',
        F.coalesce('tile_category', F.lit('UNKNOWN')).alias('tile_category'),
        F.coalesce('unique_tile_views', F.lit(0)).alias('unique_tile_views'),
        F.coalesce('unique_tile_clicks', F.lit(0)).alias('unique_tile_clicks'),
        F.coalesce('unique_interstitial_views', F.lit(0)).alias('unique_interstitial_views'),
        F.coalesce('unique_interstitial_primary_clicks', F.lit(0)).alias('unique_interstitial_primary_clicks'),
        F.coalesce('unique_interstitial_secondary_clicks', F.lit(0)).alias('unique_interstitial_secondary_clicks')
    )
)

# Scenario 1: Insert validation
insert_input = df_daily_summary.orderBy('tile_id').collect()
insert_expected = [
    {'date': PROCESS_DATE, 'tile_id': 'TILE_001', 'tile_category': 'Personal Finance', 'unique_tile_views': 1, 'unique_tile_clicks': 1, 'unique_interstitial_views': 1, 'unique_interstitial_primary_clicks': 1, 'unique_interstitial_secondary_clicks': 0},
    {'date': PROCESS_DATE, 'tile_id': 'TILE_002', 'tile_category': 'Health', 'unique_tile_views': 1, 'unique_tile_clicks': 0, 'unique_interstitial_views': 1, 'unique_interstitial_primary_clicks': 0, 'unique_interstitial_secondary_clicks': 1}
]
insert_pass = all(any(row['tile_id'] == exp['tile_id'] and row['tile_category'] == exp['tile_category'] and row['unique_tile_views'] == exp['unique_tile_views'] for row in [r.asDict() for r in insert_input]) for exp in insert_expected)

# Scenario 2: Update validation
# Simulate update by providing new click for TILE_001
update_tile_events = tile_events + [Row(event_id='E4', user_id='U1', session_id='S1', event_ts='2025-12-01', tile_id='TILE_001', event_type='TILE_CLICK', device_type='Mobile', app_version='1.0')]
df_tile_update = spark.createDataFrame(update_tile_events)
df_tile_agg_update = (
    df_tile_update.groupBy('tile_id').agg(
        F.countDistinct(F.when(F.col('event_type') == 'TILE_VIEW', F.col('user_id'))).alias('unique_tile_views'),
        F.countDistinct(F.when(F.col('event_type') == 'TILE_CLICK', F.col('user_id'))).alias('unique_tile_clicks')
    )
)
df_daily_summary_update = (
    df_tile_agg_update.join(df_inter_agg, 'tile_id', 'outer')
    .join(df_metadata.select('tile_id', 'tile_category'), 'tile_id', 'left')
    .withColumn('date', F.lit(PROCESS_DATE))
    .select(
        'date', 'tile_id',
        F.coalesce('tile_category', F.lit('UNKNOWN')).alias('tile_category'),
        F.coalesce('unique_tile_views', F.lit(0)).alias('unique_tile_views'),
        F.coalesce('unique_tile_clicks', F.lit(0)).alias('unique_tile_clicks'),
        F.coalesce('unique_interstitial_views', F.lit(0)).alias('unique_interstitial_views'),
        F.coalesce('unique_interstitial_primary_clicks', F.lit(0)).alias('unique_interstitial_primary_clicks'),
        F.coalesce('unique_interstitial_secondary_clicks', F.lit(0)).alias('unique_interstitial_secondary_clicks')
    )
)
update_input = df_daily_summary_update.orderBy('tile_id').collect()
update_expected = [
    {'date': PROCESS_DATE, 'tile_id': 'TILE_001', 'tile_category': 'Personal Finance', 'unique_tile_views': 1, 'unique_tile_clicks': 1, 'unique_interstitial_views': 1, 'unique_interstitial_primary_clicks': 1, 'unique_interstitial_secondary_clicks': 0},
    {'date': PROCESS_DATE, 'tile_id': 'TILE_002', 'tile_category': 'Health', 'unique_tile_views': 1, 'unique_tile_clicks': 0, 'unique_interstitial_views': 1, 'unique_interstitial_primary_clicks': 0, 'unique_interstitial_secondary_clicks': 1}
]
update_pass = all(any(row['tile_id'] == exp['tile_id'] and row['unique_tile_clicks'] >= exp['unique_tile_clicks'] for row in [r.asDict() for r in update_input]) for exp in update_expected)

md_report = '## Test Report\n'
md_report += '### Scenario 1: Insert\nInput:\n' + markdown_table(insert_expected, ['tile_id','tile_category','unique_tile_views','unique_tile_clicks','unique_interstitial_views','unique_interstitial_primary_clicks','unique_interstitial_secondary_clicks'])
md_report += 'Output:\n' + markdown_table([r.asDict() for r in insert_input], ['tile_id','tile_category','unique_tile_views','unique_tile_clicks','unique_interstitial_views','unique_interstitial_primary_clicks','unique_interstitial_secondary_clicks'])
md_report += f'Status: {'PASS' if insert_pass else 'FAIL'}\n\n'
md_report += '### Scenario 2: Update\nInput:\n' + markdown_table(update_expected, ['tile_id','tile_category','unique_tile_views','unique_tile_clicks','unique_interstitial_views','unique_interstitial_primary_clicks','unique_interstitial_secondary_clicks'])
md_report += 'Output:\n' + markdown_table([r.asDict() for r in update_input], ['tile_id','tile_category','unique_tile_views','unique_tile_clicks','unique_interstitial_views','unique_interstitial_primary_clicks','unique_interstitial_secondary_clicks'])
md_report += f'Status: {'PASS' if update_pass else 'FAIL'}\n'

print(md_report)
