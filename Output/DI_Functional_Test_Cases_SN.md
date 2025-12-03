=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for SOURCE_TILE_METADATA integration and tile category enrichment in TARGET_HOME_TILE_DAILY_SUMMARY
=============================================

### Test Case ID: TC_PCE3_01
**Title:** Validate creation of SOURCE_TILE_METADATA table
**Description:** Ensure the SOURCE_TILE_METADATA table is created with all required columns and correct data types.
**Preconditions:**
- Database analytics_db is accessible
- Appropriate permissions to create tables
**Steps to Execute:**
1. Execute the DDL script for SOURCE_TILE_METADATA.
2. Verify the table exists in analytics_db.
3. Check that columns tile_id, tile_name, tile_category, is_active, updated_ts are present and of correct types.
**Expected Result:**
- Table is created successfully with specified columns and types.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_02
**Title:** Validate insertion and retrieval of metadata records
**Description:** Ensure records can be inserted and retrieved from SOURCE_TILE_METADATA table.
**Preconditions:**
- SOURCE_TILE_METADATA table exists
**Steps to Execute:**
1. Insert a sample record into SOURCE_TILE_METADATA (e.g., tile_id='TILE_001', tile_name='Credit Score Check', tile_category='Personal Finance', is_active=true).
2. Query the table for tile_id='TILE_001'.
**Expected Result:**
- Inserted record is retrieved with correct values.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_03
**Title:** Validate ETL pipeline enrichment with tile metadata
**Description:** Ensure ETL pipeline joins SOURCE_TILE_METADATA and enriches daily summary with tile_name and tile_category.
**Preconditions:**
- SOURCE_TILE_METADATA table populated with active records
- Daily event data available
**Steps to Execute:**
1. Run the ETL pipeline for a day with events for a tile present in metadata.
2. Verify TARGET_HOME_TILE_DAILY_SUMMARY contains tile_name and tile_category for each tile_id.
**Expected Result:**
- Summary table is enriched with correct tile_name and tile_category values.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_04
**Title:** Validate handling of missing metadata (default values)
**Description:** Ensure that when no metadata exists for a tile_id, ETL populates tile_name='Unknown Tile' and tile_category='UNKNOWN'.
**Preconditions:**
- SOURCE_TILE_METADATA does not contain metadata for a specific tile_id
- Daily event data available for that tile_id
**Steps to Execute:**
1. Run ETL pipeline with an event for a tile_id not present in metadata.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for that tile_id.
**Expected Result:**
- tile_name is 'Unknown Tile' and tile_category is 'UNKNOWN' in the summary table.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_05
**Title:** Validate filtering of inactive tiles in enrichment
**Description:** Ensure only active tiles (is_active=true) from SOURCE_TILE_METADATA are joined and enriched; inactive tiles are ignored.
**Preconditions:**
- SOURCE_TILE_METADATA contains both active and inactive tile records
- Daily event data available for both
**Steps to Execute:**
1. Run ETL pipeline with events for tile_ids with is_active=true and is_active=false.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY.
**Expected Result:**
- Only active tile metadata is joined; inactive tiles get default values ('Unknown Tile', 'UNKNOWN').
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_06
**Title:** Validate schema evolution of TARGET_HOME_TILE_DAILY_SUMMARY
**Description:** Ensure new columns tile_name and tile_category are added to the summary table without impacting existing columns/data.
**Preconditions:**
- Existing TARGET_HOME_TILE_DAILY_SUMMARY table populated with historical data
**Steps to Execute:**
1. Run ALTER TABLE to add tile_name and tile_category.
2. Verify existing data remains unchanged; new columns are NULL or default for previous records.
**Expected Result:**
- Existing data is intact; new columns added successfully.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_07
**Title:** Validate unit test for tile_category enrichment
**Description:** Ensure unit test validates correct population of tile_category from metadata and default value handling.
**Preconditions:**
- Unit test framework configured
- Test data available
**Steps to Execute:**
1. Run unit tests for ETL transformation logic.
2. Check assertions for tile_category enrichment and default value fallback.
**Expected Result:**
- All assertions pass; tile_category populated as expected.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_08
**Title:** Validate reporting output includes tile_category
**Description:** Ensure reporting outputs (dashboards, extracts) include tile_category and values match enriched summary table.
**Preconditions:**
- ETL pipeline has run successfully
- Reporting tools configured
**Steps to Execute:**
1. Generate reporting extract or dashboard view from TARGET_HOME_TILE_DAILY_SUMMARY.
2. Verify tile_category values match those in the summary table.
**Expected Result:**
- Reporting output includes correct tile_category values for all tiles.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_09
**Title:** Validate backward compatibility and record counts
**Description:** Ensure the enhancement does not impact existing record counts and metrics in the summary table.
**Preconditions:**
- Historical data available in TARGET_HOME_TILE_DAILY_SUMMARY
**Steps to Execute:**
1. Compare record counts and metric values before and after schema change.
2. Validate no change except for addition of new columns.
**Expected Result:**
- Record counts and existing metrics remain unchanged; only new columns added.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_10
**Title:** Validate error handling when metadata table is unavailable
**Description:** Ensure ETL pipeline logs a warning and continues gracefully if SOURCE_TILE_METADATA is unavailable.
**Preconditions:**
- ETL pipeline configured
- Metadata table temporarily removed or inaccessible
**Steps to Execute:**
1. Trigger ETL pipeline with SOURCE_TILE_METADATA inaccessible.
2. Check logs for warning message.
3. Verify ETL completes and summary table uses default values for tile_name and tile_category.
**Expected Result:**
- Warning is logged; ETL completes; defaults used for metadata columns.
**Linked Jira Ticket:** PCE-3
