=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for validating the integration of SOURCE_TILE_METADATA and extension of reporting metrics with tile category, per SCRUM-7819 requirements.
=============================================

### Test Case ID: TC_SCRUM7819_01
**Title:** Create SOURCE_TILE_METADATA table in analytics_db
**Description:** Ensure the SOURCE_TILE_METADATA table is created successfully with the correct schema as specified.
**Preconditions:**
- Access to analytics_db with necessary privileges
**Steps to Execute:**
1. Execute the DDL statement to create SOURCE_TILE_METADATA table
2. Verify the table exists in the schema registry/catalog
3. Validate that all columns (tile_id, tile_name, tile_category, is_active, updated_ts) exist with correct data types
**Expected Result:**
- Table is created and visible in analytics_db
- Schema matches specification
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_02
**Title:** ETL pipeline reads SOURCE_TILE_METADATA table successfully
**Description:** Validate that the ETL pipeline can read and process data from SOURCE_TILE_METADATA without errors.
**Preconditions:**
- SOURCE_TILE_METADATA table exists and contains sample data
- ETL job is configured to access analytics_db
**Steps to Execute:**
1. Run the ETL pipeline
2. Check ETL logs for successful read of metadata table
3. Validate that tile metadata is loaded into ETL memory/dataframe
**Expected Result:**
- ETL pipeline reads the metadata table without errors
- tile metadata is available for downstream joins
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_03
**Title:** Enrich target reporting output with tile_category
**Description:** Ensure that the ETL joins SOURCE_TILE_METADATA on tile_id and enriches the target summary output with tile_category.
**Preconditions:**
- SOURCE_TILE_METADATA and source event tables populated
- ETL pipeline operational
**Steps to Execute:**
1. Run the ETL pipeline for a sample date
2. Validate that the output table TARGET_HOME_TILE_DAILY_SUMMARY contains the tile_category column
3. For each tile_id, check that tile_category matches metadata
**Expected Result:**
- tile_category column exists in output
- tile_category values match metadata for each tile_id
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_04
**Title:** Default tile_category to "UNKNOWN" if no metadata exists
**Description:** Ensure that if a tile_id in source data does not have a mapping in SOURCE_TILE_METADATA, the output assigns tile_category as "UNKNOWN".
**Preconditions:**
- SOURCE_TILE_METADATA table missing entry for at least one tile_id present in source events
**Steps to Execute:**
1. Run the ETL pipeline
2. Inspect TARGET_HOME_TILE_DAILY_SUMMARY for tile_id(s) not present in metadata
3. Verify tile_category is set to "UNKNOWN" for those rows
**Expected Result:**
- For any tile_id missing in metadata, tile_category is "UNKNOWN"
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_05
**Title:** Maintain backward compatibility (record counts unchanged)
**Description:** Validate that the introduction of tile_category does not affect record counts or other existing metrics in the target summary table.
**Preconditions:**
- Baseline output from previous ETL run (without tile_category)
**Steps to Execute:**
1. Run ETL with tile_category enrichment
2. Compare record counts and existing metric values to baseline
**Expected Result:**
- Record counts and existing metrics are unchanged
- Only tile_category is added
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_06
**Title:** Target table schema validation
**Description:** Ensure the TARGET_HOME_TILE_DAILY_SUMMARY table includes the new tile_category column with correct data type and comment.
**Preconditions:**
- ETL pipeline completed
- Reporting table exists
**Steps to Execute:**
1. Inspect schema of TARGET_HOME_TILE_DAILY_SUMMARY
2. Verify tile_category column exists, is STRING, and has correct comment
**Expected Result:**
- tile_category column present, STRING type, correct comment
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_07
**Title:** ETL pipeline runs successfully with no schema drift
**Description:** Validate that the pipeline executes end-to-end without errors related to schema drift after adding tile_category.
**Preconditions:**
- ETL pipeline updated with new logic
**Steps to Execute:**
1. Trigger a full ETL run
2. Monitor logs for schema drift or compatibility errors
**Expected Result:**
- ETL completes successfully, no schema drift errors
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_08
**Title:** Accurate reporting output for tile_category
**Description:** Validate that tile_category values in reporting outputs (dashboard extracts, exports) match the source metadata and are accurate for all tiles.
**Preconditions:**
- Reporting outputs generated from ETL
**Steps to Execute:**
1. Extract reporting output for a sample date
2. Cross-reference tile_category values with SOURCE_TILE_METADATA
3. Validate accuracy for all tiles
**Expected Result:**
- All tile_category values in reporting output are accurate and match metadata
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_09
**Title:** Unit test: enrich tile_category correctly
**Description:** Ensure unit tests cover enrichment logic for tile_category and pass with sample data.
**Preconditions:**
- Unit test framework configured
- Sample data available
**Steps to Execute:**
1. Run unit tests for ETL enrichment logic
2. Validate all tests pass for tile_category enrichment
**Expected Result:**
- Unit tests pass, enrichment logic validated
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_10
**Title:** Unit test: default to "UNKNOWN" when no metadata exists
**Description:** Ensure unit tests verify that missing metadata results in tile_category="UNKNOWN".
**Preconditions:**
- Unit test framework configured
- Sample data with missing metadata
**Steps to Execute:**
1. Run unit tests for defaulting logic
2. Validate tests pass when tile_category is "UNKNOWN" for missing metadata
**Expected Result:**
- Unit tests pass, defaulting logic works
**Linked Jira Ticket:** SCRUM-7819
