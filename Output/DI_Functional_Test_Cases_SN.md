=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for Home Tile Reporting Enhancement (SCRUM-7819): metadata enrichment, schema changes, ETL logic, and edge case validation
=============================================

# Functional Test Cases - Home Tile Reporting Enhancement (SCRUM-7819)

---

### Test Case ID: TC_SCRUM7819_01
**Title:** Create SOURCE_TILE_METADATA table with correct schema
**Description:** Validate that the SOURCE_TILE_METADATA table is created in analytics_db with all required columns and correct data types.
**Preconditions:**
- Access to analytics_db schema.
- Database user has CREATE TABLE privileges.
**Steps to Execute:**
1. Execute the DDL from Input/SOURCE_TILE_METADATA.sql.
2. Describe the table structure using DESCRIBE TABLE analytics_db.SOURCE_TILE_METADATA.
3. Verify the presence and data types of columns: tile_id (STRING), tile_name (STRING), tile_category (STRING), is_active (BOOLEAN), updated_ts (TIMESTAMP).
**Expected Result:**
- Table is created successfully with all specified columns and comments.
- Data types match the specification.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_02
**Title:** Insert and validate sample metadata in SOURCE_TILE_METADATA
**Description:** Ensure sample metadata records can be inserted and retrieved accurately from SOURCE_TILE_METADATA.
**Preconditions:**
- SOURCE_TILE_METADATA table exists.
**Steps to Execute:**
1. Insert sample records for at least two tile_ids, each with tile_name, tile_category, is_active, updated_ts.
2. Query the table for inserted tile_ids.
3. Validate that data matches inserted values.
**Expected Result:**
- Sample records are present and fields match inserted values.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_03
**Title:** Extend TARGET_HOME_TILE_DAILY_SUMMARY table with tile_category and tile_name columns
**Description:** Validate that the target table reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY contains new columns tile_category and tile_name after DDL execution.
**Preconditions:**
- Access to reporting_db schema.
- Database user has ALTER TABLE privileges.
**Steps to Execute:**
1. Execute the ALTER TABLE statement from Input/TargetDDL.sql to add tile_category and tile_name columns.
2. Describe the table structure using DESCRIBE TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY.
3. Verify the new columns are present with correct data types (STRING).
**Expected Result:**
- Table contains tile_category and tile_name columns with correct data types and comments.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_04
**Title:** ETL pipeline correctly enriches daily summary with tile metadata
**Description:** Validate that the ETL pipeline joins SOURCE_TILE_METADATA and populates tile_category and tile_name in TARGET_HOME_TILE_DAILY_SUMMARY.
**Preconditions:**
- SOURCE_TILE_METADATA table populated with active tiles.
- ETL pipeline code updated as per technical specification.
**Steps to Execute:**
1. Run the ETL job for a test date where tile events exist for tile_ids present in metadata.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for that date.
3. Verify that tile_category and tile_name fields are populated from metadata for matching tile_ids.
**Expected Result:**
- tile_category and tile_name fields are correctly enriched for all matching tile_ids.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_05
**Title:** ETL pipeline defaults tile_category and tile_name to "UNKNOWN" for missing metadata
**Description:** Ensure that when a tile_id does not exist in SOURCE_TILE_METADATA, the ETL pipeline sets tile_category and tile_name to "UNKNOWN" in the target table.
**Preconditions:**
- SOURCE_TILE_METADATA table does NOT contain all tile_ids present in event tables.
- ETL pipeline code updated for default logic.
**Steps to Execute:**
1. Insert tile events for a tile_id not present in SOURCE_TILE_METADATA.
2. Run the ETL job for a test date.
3. Query TARGET_HOME_TILE_DAILY_SUMMARY for that tile_id and date.
4. Check values of tile_category and tile_name.
**Expected Result:**
- tile_category and tile_name are set to "UNKNOWN" for tile_ids missing in metadata.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_06
**Title:** Validate record count consistency before and after enrichment
**Description:** Ensure the ETL pipeline maintains record counts in TARGET_HOME_TILE_DAILY_SUMMARY after metadata enrichment (within tolerance).
**Preconditions:**
- Baseline record count available from previous ETL runs.
- ETL pipeline updated for enrichment.
**Steps to Execute:**
1. Run ETL job before metadata enrichment and record output row count.
2. Run ETL job after metadata enrichment.
3. Compare row counts; difference should be within 1% tolerance.
**Expected Result:**
- Record count difference does not exceed 1% after enrichment.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_07
**Title:** Schema validation in ETL unit tests
**Description:** Validate that the ETL pipeline output schema matches the expected target schema (including new columns).
**Preconditions:**
- Unit test framework set up for ETL pipeline.
**Steps to Execute:**
1. Run ETL unit tests for output DataFrame schema.
2. Assert presence and types of tile_category and tile_name columns.
**Expected Result:**
- ETL output schema includes tile_category and tile_name as STRING columns.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_08
**Title:** Backward compatibility for existing reporting metrics
**Description:** Ensure that addition of new columns does not affect existing metrics or reporting outputs.
**Preconditions:**
- Existing reports and dashboards configured to read TARGET_HOME_TILE_DAILY_SUMMARY.
**Steps to Execute:**
1. Run ETL job and generate target table with new columns.
2. Query existing metrics (unique_tile_views, unique_tile_clicks, etc.).
3. Compare results with previous runs for same data.
**Expected Result:**
- Existing metrics are unchanged and accurate; only new columns are added.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_09
**Title:** ETL pipeline does not introduce schema drift
**Description:** Validate that the ETL pipeline writes the target table with the expected schema and no extra/missing columns.
**Preconditions:**
- ETL pipeline updated as per technical specification.
**Steps to Execute:**
1. Run ETL job and inspect output table schema.
2. Compare schema with expected definition in Input/TargetDDL.sql.
**Expected Result:**
- Output table schema matches expected definition; no drift detected.
**Linked Jira Ticket:** SCRUM-7819

---

### Test Case ID: TC_SCRUM7819_10
**Title:** Reporting outputs display tile_category accurately
**Description:** Validate that downstream reporting tools (Power BI/Tableau) display tile_category field as expected.
**Preconditions:**
- BI dashboard updated to read new column (dependency on BI team).
- ETL pipeline has populated tile_category.
**Steps to Execute:**
1. Refresh reporting dashboard after ETL pipeline run.
2. Verify tile_category appears in dashboard drilldowns and is accurate.
**Expected Result:**
- tile_category is available and correctly populated in reporting outputs.
**Linked Jira Ticket:** SCRUM-7819
