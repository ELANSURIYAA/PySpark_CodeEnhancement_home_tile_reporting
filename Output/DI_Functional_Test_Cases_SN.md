=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for validating SOURCE_TILE_METADATA integration and tile category enrichment in home tile reporting
=============================================

# Functional Test Cases - Home Tile Reporting Enhancement (SCRUM-7819)

## Traceability: All test cases linked to Jira SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_01
**Title:** Validate creation and structure of SOURCE_TILE_METADATA table
**Description:** Ensure the SOURCE_TILE_METADATA table is created with all required columns and data types as per specification.
**Preconditions:**
- Access to analytics_db database
- ETL pipeline is not running
**Steps to Execute:**
1. Run the DDL script for SOURCE_TILE_METADATA creation.
2. Verify the table exists in analytics_db.
3. Check that columns: tile_id, tile_name, tile_category, is_active, updated_ts exist with correct data types.
4. Validate column comments.
**Expected Result:**
- Table exists with all specified columns and correct types/comments.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_02
**Title:** Validate sample data insertion into SOURCE_TILE_METADATA
**Description:** Ensure sample metadata rows can be inserted and retrieved from the new table.
**Preconditions:**
- SOURCE_TILE_METADATA table exists
**Steps to Execute:**
1. Insert sample rows into SOURCE_TILE_METADATA.
2. Query the table for inserted rows.
**Expected Result:**
- Inserted rows are present and match input values.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_03
**Title:** ETL pipeline enriches target summary with tile_category from metadata
**Description:** Validate that the ETL job joins SOURCE_TILE_METADATA and populates tile_category in TARGET_HOME_TILE_DAILY_SUMMARY.
**Preconditions:**
- SOURCE_TILE_METADATA contains tile_id and tile_category values
- ETL pipeline is configured to join metadata
**Steps to Execute:**
1. Run ETL pipeline for a reporting date with event data and matching metadata.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for that date.
3. Check that tile_category column is populated from metadata for each tile_id.
**Expected Result:**
- tile_category in target table matches the value in metadata for each tile_id.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_04
**Title:** ETL pipeline defaults tile_category to UNKNOWN for missing metadata
**Description:** Validate that tiles without metadata are assigned tile_category='UNKNOWN' in the target summary.
**Preconditions:**
- SOURCE_TILE_METADATA missing entry for at least one tile_id present in event tables
**Steps to Execute:**
1. Run ETL pipeline for a reporting date where some tile_ids have no metadata.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for those tile_ids.
**Expected Result:**
- tile_category column is 'UNKNOWN' for tile_ids not present in metadata.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_05
**Title:** Target table schema includes new columns tile_category and tile_name
**Description:** Ensure TARGET_HOME_TILE_DAILY_SUMMARY table schema is updated to include tile_category and tile_name columns.
**Preconditions:**
- Schema migration script executed
**Steps to Execute:**
1. Run schema migration/ALTER TABLE script.
2. Describe TARGET_HOME_TILE_DAILY_SUMMARY table.
3. Verify tile_category and tile_name columns exist with correct data types and comments.
**Expected Result:**
- Both columns exist and match specification.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_06
**Title:** ETL pipeline populates tile_name from metadata
**Description:** Validate that tile_name is correctly populated in the target summary from SOURCE_TILE_METADATA.
**Preconditions:**
- Metadata table contains tile_name for relevant tile_ids
**Steps to Execute:**
1. Run ETL pipeline with event and metadata data.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for tile_name values.
**Expected Result:**
- tile_name matches metadata for each tile_id; default logic applies if missing.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_07
**Title:** ETL pipeline maintains backward compatibility for existing metrics
**Description:** Ensure that addition of metadata enrichment does not affect existing metric counts in the target summary.
**Preconditions:**
- Historical data available for comparison
**Steps to Execute:**
1. Run ETL pipeline before and after metadata integration.
2. Compare counts of unique_tile_views, unique_tile_clicks, interstitial metrics for same reporting date.
**Expected Result:**
- Metric counts are unchanged; only new columns are added.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_08
**Title:** ETL pipeline handles inactive tiles in metadata
**Description:** Validate that tiles marked is_active=false in metadata are handled correctly in reporting (e.g., excluded or flagged as inactive).
**Preconditions:**
- SOURCE_TILE_METADATA contains inactive tile entries
**Steps to Execute:**
1. Insert metadata with is_active=false for some tile_ids.
2. Run ETL pipeline.
3. Query target summary for handling of inactive tiles.
**Expected Result:**
- Inactive tiles are excluded from reporting or flagged appropriately as per business rules.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_09
**Title:** ETL pipeline passes schema validation and unit tests
**Description:** Ensure the ETL job passes all schema validation checks and unit tests after metadata integration.
**Preconditions:**
- Updated ETL code with schema validation logic
- Unit test suite available
**Steps to Execute:**
1. Run ETL job with schema validation enabled.
2. Execute unit tests for tile_category enrichment, default logic, and record counts.
**Expected Result:**
- All schema validations and unit tests pass with no errors.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_10
**Title:** ETL pipeline runs successfully with no schema drift
**Description:** Validate that the ETL pipeline executes end-to-end without schema drift or errors after metadata integration.
**Preconditions:**
- All DDL and ETL changes deployed
**Steps to Execute:**
1. Run full ETL pipeline for a reporting date.
2. Monitor job logs for schema drift or runtime errors.
**Expected Result:**
- ETL job completes successfully; no schema drift or errors.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_11
**Title:** Data quality check for metadata coverage in reporting
**Description:** Ensure that all tile_ids present in event tables are either enriched with metadata or have tile_category set to 'UNKNOWN'.
**Preconditions:**
- Event tables and metadata table populated
**Steps to Execute:**
1. Run ETL pipeline for a reporting date.
2. Query target summary for all tile_ids.
3. Verify that each tile_id has either a valid tile_category or 'UNKNOWN'.
**Expected Result:**
- No tile_id is missing tile_category in the target summary.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_12
**Title:** Validate handling of null and edge-case values in metadata
**Description:** Ensure that nulls, blanks, and unexpected values in SOURCE_TILE_METADATA do not cause ETL failures or incorrect reporting.
**Preconditions:**
- Insert metadata rows with null/blank tile_name and tile_category
**Steps to Execute:**
1. Insert edge-case metadata rows.
2. Run ETL pipeline.
3. Query target summary for affected tile_ids.
**Expected Result:**
- ETL pipeline handles nulls/blanks gracefully; applies default logic as specified.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_13
**Title:** Validate documentation and lineage updates
**Description:** Ensure that metadata table and new columns are documented and lineage diagrams updated as per requirements.
**Preconditions:**
- Documentation tools/process available
**Steps to Execute:**
1. Review updated data dictionary and lineage diagrams.
2. Verify inclusion of SOURCE_TILE_METADATA and new target columns.
**Expected Result:**
- Documentation and diagrams are complete and accurate.
**Linked Jira Ticket:** SCRUM-7819
