=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for validating home tile reporting enhancements: metadata enrichment, schema extension, and ETL logic for tile category integration.
=============================================

### Test Case ID: TC_SCRUM7819_01
**Title:** Validate creation of SOURCE_TILE_METADATA table
**Description:** Ensure the SOURCE_TILE_METADATA table is created with the correct schema and all columns as defined in the specification.
**Preconditions:**
- Database analytics_db exists
- User has privileges to create tables
**Steps to Execute:**
1. Run the DDL from Input/SOURCE_TILE_METADATA.sql
2. Describe the table structure in analytics_db
3. Verify columns: tile_id, tile_name, tile_category, is_active, updated_ts
**Expected Result:**
- Table is created successfully
- All columns exist with correct data types and comments
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_02
**Title:** Validate addition of tile_category column to TARGET_HOME_TILE_DAILY_SUMMARY
**Description:** Ensure that the target table has the new tile_category column added without affecting existing columns/data.
**Preconditions:**
- reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY exists
- User has privileges to alter tables
**Steps to Execute:**
1. Run the ALTER TABLE statement from the technical spec
2. Describe the table structure
3. Verify tile_category column exists with correct type and comment
4. Check existing records for schema drift or errors
**Expected Result:**
- tile_category column is present
- No errors or data loss in existing columns
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_03
**Title:** Validate ETL pipeline reads SOURCE_TILE_METADATA and filters active tiles
**Description:** Ensure the ETL pipeline reads the SOURCE_TILE_METADATA table and only includes rows where is_active = True.
**Preconditions:**
- SOURCE_TILE_METADATA table is populated with both active and inactive tiles
- ETL job configured to read metadata
**Steps to Execute:**
1. Populate SOURCE_TILE_METADATA with sample tiles (active/inactive)
2. Run ETL pipeline
3. Inspect the dataframe used for enrichment
**Expected Result:**
- Only active tiles are present in ETL enrichment logic
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_04
**Title:** Validate enrichment of daily summary with tile_category
**Description:** Ensure ETL joins the metadata table on tile_id and enriches each record in the daily summary with the correct tile_category.
**Preconditions:**
- SOURCE_TILE_METADATA and SOURCE_HOME_TILE_EVENTS tables are populated
- ETL pipeline is updated with join logic
**Steps to Execute:**
1. Populate both tables with matching and non-matching tile_ids
2. Run the ETL pipeline
3. Inspect the output in TARGET_HOME_TILE_DAILY_SUMMARY
**Expected Result:**
- Each tile_id is enriched with the correct tile_category
- Non-matching tile_ids have tile_category set to "UNKNOWN"
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_05
**Title:** Validate defaulting of tile_category to "UNKNOWN" for missing metadata
**Description:** Ensure that when a tile_id in the summary does not have corresponding metadata, the tile_category field defaults to "UNKNOWN".
**Preconditions:**
- Some tile_ids in SOURCE_HOME_TILE_EVENTS do not exist in SOURCE_TILE_METADATA
**Steps to Execute:**
1. Populate SOURCE_HOME_TILE_EVENTS with a tile_id not present in metadata
2. Run ETL pipeline
3. Inspect TARGET_HOME_TILE_DAILY_SUMMARY for that tile_id
**Expected Result:**
- tile_category is "UNKNOWN" for missing metadata
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_06
**Title:** Validate backward compatibility of ETL pipeline
**Description:** Ensure that the addition of tile_category does not affect existing metrics or record counts in the target table.
**Preconditions:**
- Existing ETL logic and data present
- tile_category column added
**Steps to Execute:**
1. Run ETL pipeline on historical data
2. Compare record counts and metrics with previous outputs
**Expected Result:**
- Record counts and metrics remain unchanged except for enrichment with tile_category
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_07
**Title:** Validate schema validation and error handling in ETL pipeline
**Description:** Ensure that the ETL pipeline handles schema validation and does not fail due to schema drift or missing columns.
**Preconditions:**
- tile_category column added
- ETL pipeline updated
**Steps to Execute:**
1. Run unit tests for schema validation
2. Remove tile_category column and rerun ETL (simulate drift)
3. Observe ETL error handling
**Expected Result:**
- ETL pipeline logs schema drift errors but does not halt unexpectedly
- Proper error messages are generated
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_08
**Title:** Validate data quality: uniqueness and completeness of tile_id + date
**Description:** Ensure that the combination of tile_id and date is unique and no nulls exist in key fields after enrichment.
**Preconditions:**
- ETL pipeline produces enriched summary
**Steps to Execute:**
1. Run ETL pipeline
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for duplicate tile_id + date
3. Check for nulls in tile_id, date, tile_category
**Expected Result:**
- No duplicates for tile_id + date
- No nulls in key dimension fields
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_09
**Title:** Validate metric range: All counts are >= 0
**Description:** Ensure that all metric fields (views, clicks, etc.) are non-negative after ETL processing.
**Preconditions:**
- ETL pipeline produces summary metrics
**Steps to Execute:**
1. Run ETL pipeline
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for negative values in metric columns
**Expected Result:**
- All metric columns have values >= 0
**Linked Jira Ticket:** SCRUM-7819

### Test Case ID: TC_SCRUM7819_10
**Title:** Validate metadata completeness function logs missing tiles
**Description:** Ensure that the validate_metadata_completeness function correctly identifies and logs tile_ids missing from metadata.
**Preconditions:**
- Some tile_ids in summary not present in metadata
- validate_metadata_completeness function implemented
**Steps to Execute:**
1. Run validate_metadata_completeness on sample summary and metadata
2. Observe log output for missing tile_ids
**Expected Result:**
- Missing tile_ids are logged with warning message
**Linked Jira Ticket:** SCRUM-7819
