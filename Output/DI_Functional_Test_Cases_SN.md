=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for Home Tile Reporting ETL enhancement with metadata integration
=============================================

### Test Case ID: TC_SCRUM-7567_01
**Title:** Validate metadata enrichment in daily summary
**Description:** Ensure that the TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED table is populated with tile_name and tile_category from SOURCE_TILE_METADATA for active tiles.
**Preconditions:**
- SOURCE_TILE_METADATA contains at least one active tile (is_active = true).
- SOURCE_HOME_TILE_EVENTS contains events for the same tile_id.
**Steps to Execute:**
1. Run the ETL pipeline for a date with relevant events and active tiles in metadata.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED for the date and tile_id.
**Expected Result:**
- tile_name and tile_category columns are populated as per SOURCE_TILE_METADATA for matching tile_id.
- Only active tiles are present; inactive tiles are excluded.
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_02
**Title:** Validate fallback for missing metadata
**Description:** Ensure that if tile_name or tile_category is null in SOURCE_TILE_METADATA, the ETL populates default values in TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED.
**Preconditions:**
- SOURCE_TILE_METADATA contains a tile with null tile_name and/or tile_category.
- SOURCE_HOME_TILE_EVENTS contains events for that tile_id.
**Steps to Execute:**
1. Run the ETL pipeline for a date with relevant events and the tile in metadata with null fields.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED for the tile_id.
**Expected Result:**
- tile_name is 'Unknown Tile' if null.
- tile_category is 'Uncategorized' if null.
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_03
**Title:** Validate tile-level metrics aggregation
**Description:** Ensure that unique_tile_views, unique_tile_clicks, and interstitial metrics are correctly aggregated per tile_id per day.
**Preconditions:**
- SOURCE_HOME_TILE_EVENTS and SOURCE_INTERSTITIAL_EVENTS contain multiple events for the same tile_id and date.
**Steps to Execute:**
1. Run the ETL pipeline for the relevant date.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED for the tile_id and date.
**Expected Result:**
- unique_tile_views = countDistinct(user_id) where event_type = 'TILE_VIEW'.
- unique_tile_clicks = countDistinct(user_id) where event_type = 'TILE_CLICK'.
- unique_interstitial_views = countDistinct(user_id) where interstitial_view_flag = true.
- unique_interstitial_primary_clicks = countDistinct(user_id) where primary_button_click_flag = true.
- unique_interstitial_secondary_clicks = countDistinct(user_id) where secondary_button_click_flag = true.
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_04
**Title:** Validate CTR calculations and division by zero handling
**Description:** Ensure that tile_ctr, primary_ctr, and secondary_ctr are correctly calculated and set to 0.0 when denominators are zero.
**Preconditions:**
- TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED has tiles with zero views or clicks.
**Steps to Execute:**
1. Run the ETL pipeline for a date with at least one tile having zero views or clicks.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED for the tile_id and date.
**Expected Result:**
- tile_ctr = unique_tile_clicks / unique_tile_views if views > 0, else 0.0.
- primary_ctr = unique_interstitial_primary_clicks / unique_interstitial_views if views > 0, else 0.0.
- secondary_ctr = unique_interstitial_secondary_clicks / unique_interstitial_views if views > 0, else 0.0.
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_05
**Title:** Validate category KPIs aggregation
**Description:** Ensure that TARGET_HOME_TILE_CATEGORY_KPIS aggregates metrics correctly by tile_category and date.
**Preconditions:**
- TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED contains multiple tiles with the same tile_category.
**Steps to Execute:**
1. Run the ETL pipeline for the relevant date.
2. Query TARGET_HOME_TILE_CATEGORY_KPIS for the date and tile_category.
**Expected Result:**
- category_tile_views = sum(unique_tile_views) grouped by date, tile_category.
- category_tile_clicks = sum(unique_tile_clicks) grouped by date, tile_category.
- category_interstitial_views = sum(unique_interstitial_views) grouped by date, tile_category.
- category_primary_clicks = sum(unique_interstitial_primary_clicks) grouped by date, tile_category.
- category_secondary_clicks = sum(unique_interstitial_secondary_clicks) grouped by date, tile_category.
- category_ctr = category_tile_clicks / category_tile_views if views > 0, else 0.0.
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_06
**Title:** Validate inactive tiles exclusion
**Description:** Ensure that tiles marked as is_active = false in SOURCE_TILE_METADATA are excluded from all enriched reporting tables.
**Preconditions:**
- SOURCE_TILE_METADATA contains at least one tile with is_active = false.
- SOURCE_HOME_TILE_EVENTS contains events for that tile_id.
**Steps to Execute:**
1. Run the ETL pipeline for the relevant date.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED for the tile_id.
**Expected Result:**
- No records for tile_id with is_active = false in TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED.
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_07
**Title:** Validate referential integrity of tile_id
**Description:** Ensure that tile_id values in TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED and TARGET_HOME_TILE_CATEGORY_KPIS exist in SOURCE_TILE_METADATA and events tables.
**Preconditions:**
- All tables are populated with test data.
**Steps to Execute:**
1. Run the ETL pipeline for the relevant date.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED and TARGET_HOME_TILE_CATEGORY_KPIS for tile_id and tile_category.
**Expected Result:**
- All tile_id values exist in both SOURCE_TILE_METADATA (is_active = true) and event tables.
- No orphan tile_id records.
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_08
**Title:** Validate partitioning by date in target tables
**Description:** Ensure that TARGET_HOME_TILE_DAILY_SUMMARY_ENHANCED and TARGET_HOME_TILE_CATEGORY_KPIS are partitioned by date and support partition pruning.
**Preconditions:**
- Multiple days of data are available in source tables.
**Steps to Execute:**
1. Run the ETL pipeline for multiple dates.
2. Query the target tables for specific date partitions.
**Expected Result:**
- Data is correctly partitioned by date.
- Querying by date only scans relevant partitions.
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_09
**Title:** Validate broadcast join optimization for metadata
**Description:** Ensure that the ETL uses broadcast join for SOURCE_TILE_METADATA and does not exceed cluster memory limits.
**Preconditions:**
- SOURCE_TILE_METADATA is small enough for broadcast join.
- Cluster memory limits are known.
**Steps to Execute:**
1. Run the ETL pipeline with monitoring enabled.
2. Check Spark UI or logs for broadcast join usage and memory consumption.
**Expected Result:**
- Broadcast join is used for metadata table.
- No memory errors or spills.
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_10
**Title:** Validate backward compatibility of existing reports
**Description:** Ensure that existing reports and dashboards using old target tables continue to function after enhancement.
**Preconditions:**
- Existing reports reference TARGET_HOME_TILE_DAILY_SUMMARY and TARGET_HOME_TILE_GLOBAL_KPIS.
**Steps to Execute:**
1. Deploy the enhanced ETL pipeline.
2. Run existing reports and dashboards.
**Expected Result:**
- No errors or missing data in existing reports.
- Old tables remain unchanged and available.
**Linked Jira Ticket:** SCRUM-7567
