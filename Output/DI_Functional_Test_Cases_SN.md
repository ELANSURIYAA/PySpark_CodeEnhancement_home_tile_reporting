=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for Home Tile Reporting Enhancement: metadata integration, ETL enrichment, schema validation, and backward compatibility.
=============================================

# Functional Test Cases for Home Tile Reporting Enhancement

---

### Test Case ID: TC_SCRUM7567_01
**Title:** Verify creation of SOURCE_TILE_METADATA table
**Description:** Ensure the SOURCE_TILE_METADATA table is created with the correct schema and all required columns.
**Preconditions:**
- Database analytics_db is accessible.
- Sufficient privileges to create tables.
**Steps to Execute:**
1. Execute the DDL script for SOURCE_TILE_METADATA.
2. Query the table schema using DESCRIBE analytics_db.SOURCE_TILE_METADATA.
3. Validate presence and data types of columns: tile_id, tile_name, tile_category, is_active, updated_ts.
**Expected Result:**
- Table is created successfully.
- All columns exist with correct data types and comments.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_02
**Title:** Validate insertion and retrieval of sample metadata records
**Description:** Ensure sample records can be inserted and retrieved from SOURCE_TILE_METADATA.
**Preconditions:**
- SOURCE_TILE_METADATA table exists.
**Steps to Execute:**
1. Insert sample records for various tile categories (e.g., Personal Finance, Health Checks).
2. Query the table for all records.
3. Validate that inserted records are present and fields are populated correctly.
**Expected Result:**
- All sample records are inserted and retrievable.
- tile_category field reflects correct business categories.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_03
**Title:** ETL pipeline reads metadata and enriches target summary
**Description:** Ensure the ETL pipeline reads SOURCE_TILE_METADATA and enriches TARGET_HOME_TILE_DAILY_SUMMARY with tile_category.
**Preconditions:**
- SOURCE_TILE_METADATA and event source tables are populated.
- ETL pipeline code deployed.
**Steps to Execute:**
1. Run the ETL pipeline for a sample date range.
2. Query reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY for output records.
3. Validate that tile_category is present and correctly populated for mapped tile_ids.
**Expected Result:**
- tile_category column exists in output.
- For mapped tile_ids, tile_category matches metadata.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_04
**Title:** Validate default category assignment for unmapped tiles
**Description:** Ensure that tile_ids not present in SOURCE_TILE_METADATA are assigned tile_category = "UNKNOWN".
**Preconditions:**
- TARGET_HOME_TILE_DAILY_SUMMARY table exists.
- Some tile_ids in event data do not exist in metadata table.
**Steps to Execute:**
1. Populate event tables with tile_ids absent from metadata.
2. Run the ETL pipeline.
3. Query TARGET_HOME_TILE_DAILY_SUMMARY for those tile_ids.
**Expected Result:**
- tile_category is "UNKNOWN" for unmapped tile_ids.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_05
**Title:** Verify backward compatibility in ETL output
**Description:** Ensure the ETL output maintains all existing metrics and record counts after enhancement.
**Preconditions:**
- ETL pipeline previously run before enhancement.
- Baseline output available for comparison.
**Steps to Execute:**
1. Run ETL pipeline after metadata integration.
2. Compare record counts and metrics (unique_tile_views, unique_tile_clicks, etc.) to baseline.
3. Validate that all counts are unchanged except for addition of tile_category.
**Expected Result:**
- Record counts and metrics remain unchanged.
- No data loss or schema drift.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_06
**Title:** Validate schema of TARGET_HOME_TILE_DAILY_SUMMARY after migration
**Description:** Ensure the target table schema includes tile_category and all existing columns after migration.
**Preconditions:**
- Migration scripts executed.
**Steps to Execute:**
1. Execute DESCRIBE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY.
2. Validate presence of tile_category column and all pre-existing columns.
**Expected Result:**
- tile_category column exists with correct data type.
- All previous columns remain unchanged.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_07
**Title:** ETL pipeline handles empty metadata table gracefully
**Description:** Ensure ETL pipeline does not fail and assigns tile_category = "UNKNOWN" when metadata table is empty.
**Preconditions:**
- SOURCE_TILE_METADATA table exists but is empty.
- Event tables are populated.
**Steps to Execute:**
1. Truncate SOURCE_TILE_METADATA table.
2. Run ETL pipeline.
3. Query TARGET_HOME_TILE_DAILY_SUMMARY.
**Expected Result:**
- All output records have tile_category = "UNKNOWN".
- ETL pipeline completes successfully.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_08
**Title:** ETL pipeline maintains data quality and error handling
**Description:** Ensure the ETL pipeline validates data quality and handles errors (e.g., missing tile_id, invalid categories).
**Preconditions:**
- Event data contains edge cases (missing tile_id, invalid category values).
**Steps to Execute:**
1. Populate event tables with edge case records.
2. Run ETL pipeline.
3. Validate that pipeline logs errors/warnings and skips or defaults appropriately.
**Expected Result:**
- Data quality validation is enforced.
- Errors are logged and handled gracefully.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_09
**Title:** Validate dashboard/reporting outputs reflect new category field
**Description:** Ensure downstream dashboards and reporting tools can access and display tile_category in outputs.
**Preconditions:**
- TARGET_HOME_TILE_DAILY_SUMMARY populated with tile_category.
- Dashboard/reporting tools configured to read new column.
**Steps to Execute:**
1. Load reporting data into dashboard tool (e.g., Power BI, Tableau).
2. Create category-level visualizations.
3. Validate that tile_category is available for filtering/grouping.
**Expected Result:**
- Dashboards display and filter by tile_category.
**Linked Jira Ticket:** SCRUM-7567
