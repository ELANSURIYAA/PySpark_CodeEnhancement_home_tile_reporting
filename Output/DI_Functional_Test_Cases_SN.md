=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for SOURCE_TILE_METADATA integration and tile_category enrichment in Home Tile Reporting ETL pipeline, as specified in SCRUM-7819.
=============================================

# Functional Test Cases for Home Tile Reporting Enhancement (SCRUM-7819)

---

### Test Case ID: TC_SCRUM-7819_01
**Title:** Validate creation of SOURCE_TILE_METADATA table
**Description:** Ensure the new metadata table is created with the correct schema and all required columns as per DDL.
**Preconditions:**
- Access to analytics_db with CREATE TABLE privileges.
- No existing table named SOURCE_TILE_METADATA.
**Steps to Execute:**
1. Execute the DDL to create analytics_db.SOURCE_TILE_METADATA.
2. Query the table schema.
3. Compare the columns and types to the specification.
**Expected Result:**
- Table is created with columns: tile_id, tile_name, tile_category, is_active, updated_ts.
- Data types and comments match specification.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM-7819_02
**Title:** Validate ETL pipeline reads metadata table successfully
**Description:** Ensure ETL job can read from SOURCE_TILE_METADATA without errors and loads active tile metadata.
**Preconditions:**
- SOURCE_TILE_METADATA table exists and is populated with sample data.
- ETL job is deployed.
**Steps to Execute:**
1. Run the ETL pipeline.
2. Check ETL logs for successful read of SOURCE_TILE_METADATA.
3. Validate that only records with is_active = true are loaded.
**Expected Result:**
- ETL reads metadata table without errors.
- Only active tiles are loaded.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM-7819_03
**Title:** Validate enrichment of tile_category in target table
**Description:** Ensure tile_category is correctly populated in TARGET_HOME_TILE_DAILY_SUMMARY for all records with valid metadata.
**Preconditions:**
- ETL pipeline configured to perform LEFT JOIN with SOURCE_TILE_METADATA.
- Metadata table contains at least one matching tile_id for source events.
**Steps to Execute:**
1. Run the ETL pipeline.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for recent records.
3. Check that tile_category matches the metadata for each tile_id.
**Expected Result:**
- tile_category is populated according to metadata for all matching tile_id values.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM-7819_04
**Title:** Validate default category assignment for unmapped tiles
**Description:** Ensure that if a tile_id in the source data does not exist in SOURCE_TILE_METADATA, tile_category is set to "UNKNOWN" in the target table.
**Preconditions:**
- At least one tile_id in source events does not exist in metadata table.
**Steps to Execute:**
1. Insert a new tile_id in source events not present in SOURCE_TILE_METADATA.
2. Run the ETL pipeline.
3. Query TARGET_HOME_TILE_DAILY_SUMMARY for the new tile_id.
**Expected Result:**
- tile_category is set to "UNKNOWN" for unmapped tile_id.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM-7819_05
**Title:** Validate backward compatibility (record counts unchanged)
**Description:** Ensure the addition of tile_category and metadata enrichment does not alter the number of records output by the ETL pipeline.
**Preconditions:**
- Baseline record counts from previous ETL runs are available.
**Steps to Execute:**
1. Run the ETL pipeline with and without metadata enrichment.
2. Compare record counts in TARGET_HOME_TILE_DAILY_SUMMARY for the same date range.
**Expected Result:**
- Record counts remain unchanged (no dropped or duplicated records).
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM-7819_06
**Title:** Validate schema evolution of TARGET_HOME_TILE_DAILY_SUMMARY
**Description:** Ensure the target table is updated to include the new tile_category column with correct data type and comment.
**Preconditions:**
- Table exists prior to schema change.
**Steps to Execute:**
1. Execute ALTER TABLE statement to add tile_category.
2. Query the table schema.
**Expected Result:**
- tile_category column is present, type STRING, with correct comment.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM-7819_07
**Title:** Validate null and default value handling for tile_category
**Description:** Ensure there are no NULL values in tile_category after ETL run; unmapped tiles should show "UNKNOWN".
**Preconditions:**
- ETL pipeline is configured with COALESCE logic for tile_category.
**Steps to Execute:**
1. Run the ETL pipeline.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for records where tile_category IS NULL.
**Expected Result:**
- No records with tile_category IS NULL; all unmapped tiles have "UNKNOWN".
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM-7819_08
**Title:** Validate referential integrity between target and metadata tables
**Description:** Ensure all tile_id values in TARGET_HOME_TILE_DAILY_SUMMARY exist in SOURCE_TILE_METADATA, or have tile_category = "UNKNOWN".
**Preconditions:**
- Both tables contain data.
**Steps to Execute:**
1. Run the ETL pipeline.
2. For each tile_id in TARGET_HOME_TILE_DAILY_SUMMARY, check existence in SOURCE_TILE_METADATA.
3. For any missing, confirm tile_category is "UNKNOWN".
**Expected Result:**
- All mapped tile_id have valid category; missing tile_id have "UNKNOWN".
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM-7819_09
**Title:** Validate error handling when metadata table is unavailable
**Description:** Ensure ETL pipeline does not fail fatally if SOURCE_TILE_METADATA is temporarily unavailable; target table should still be populated with "UNKNOWN" categories.
**Preconditions:**
- Simulate metadata table unavailability (e.g., rename or drop table).
**Steps to Execute:**
1. Make SOURCE_TILE_METADATA unavailable.
2. Run the ETL pipeline.
3. Query TARGET_HOME_TILE_DAILY_SUMMARY for recent records.
**Expected Result:**
- ETL pipeline completes, all tile_category values are "UNKNOWN".
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM-7819_10
**Title:** Validate unit test for tile_category enrichment logic
**Description:** Ensure unit tests validate enrichment logic for both mapped and unmapped tile_id values.
**Preconditions:**
- Unit test framework is in place.
- Sample data covers mapped and unmapped tiles.
**Steps to Execute:**
1. Run ETL unit tests for enrichment logic.
2. Review test results for tile_category mapping and defaulting.
**Expected Result:**
- All unit tests pass; enrichment logic works as expected.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM-7819_11
**Title:** Validate regression test for overall ETL pipeline
**Description:** Ensure the ETL pipeline continues to produce correct outputs for all fields, including new tile_category, after code changes.
**Preconditions:**
- Regression test suite covers all ETL outputs.
**Steps to Execute:**
1. Run regression test suite.
2. Compare outputs before and after enhancement.
**Expected Result:**
- All regression tests pass; no unintended changes to existing outputs.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM-7819_12
**Title:** Validate dashboard reporting of tile_category field
**Description:** Ensure tile_category appears in reporting outputs and is available for BI dashboard consumption after ETL run.
**Preconditions:**
- BI dashboards are configured to query target table.
**Steps to Execute:**
1. Run ETL pipeline.
2. Refresh BI dashboard data source.
3. Confirm tile_category is available and populated in dashboard fields.
**Expected Result:**
- tile_category is visible and accurate in BI dashboards.
**Linked Jira Ticket:** SCRUM-7819
