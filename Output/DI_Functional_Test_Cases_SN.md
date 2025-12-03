=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for validating tile metadata integration and category enrichment in home tile reporting ETL pipeline
=============================================

# Functional Test Cases: Tile Metadata Integration (SCRUM-7567)

---

### Test Case ID: TC_SCRUM-7567_01
**Title:** Validate creation of SOURCE_TILE_METADATA table
**Description:** Ensure the SOURCE_TILE_METADATA table is created with the expected schema and columns for tile_id, tile_name, tile_category, is_active, and updated_ts.
**Preconditions:**
- Access to analytics_db
- Sufficient privileges to create tables
**Steps to Execute:**
1. Execute the DDL statement for SOURCE_TILE_METADATA.
2. Query information_schema or DESCRIBE TABLE for SOURCE_TILE_METADATA.
3. Verify all expected columns and data types are present.
**Expected Result:**
- Table exists in analytics_db
- Columns: tile_id (STRING), tile_name (STRING), tile_category (STRING), is_active (BOOLEAN), updated_ts (TIMESTAMP) are present
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_02
**Title:** Validate ETL pipeline reads metadata table successfully
**Description:** Ensure the ETL pipeline reads from SOURCE_TILE_METADATA and loads only active tiles.
**Preconditions:**
- SOURCE_TILE_METADATA table exists and contains sample data
- ETL pipeline is configured to read from the metadata table
**Steps to Execute:**
1. Populate SOURCE_TILE_METADATA with sample records (active and inactive tiles).
2. Run the ETL pipeline.
3. Inspect the dataframe or intermediate output for metadata ingestion.
**Expected Result:**
- Only records with is_active = true are loaded into the pipeline
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_03
**Title:** Validate enrichment of target table with tile_category and tile_name
**Description:** Ensure the ETL pipeline enriches TARGET_HOME_TILE_DAILY_SUMMARY with tile_category and tile_name from metadata.
**Preconditions:**
- SOURCE_TILE_METADATA and source event tables populated
- ETL pipeline updated with LEFT JOIN logic
**Steps to Execute:**
1. Run the ETL pipeline for a reporting date.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for output records.
3. Verify that tile_name and tile_category columns are present and populated.
**Expected Result:**
- tile_name and tile_category columns exist in TARGET_HOME_TILE_DAILY_SUMMARY
- Values match those in SOURCE_TILE_METADATA for corresponding tile_id
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_04
**Title:** Validate handling of tiles with missing metadata
**Description:** Ensure tiles with no corresponding metadata entry are assigned default values ("UNKNOWN") for tile_name and tile_category.
**Preconditions:**
- SOURCE_TILE_METADATA does not contain metadata for some tile_ids present in source events
**Steps to Execute:**
1. Run the ETL pipeline with events for tile_ids not present in metadata table.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for those tile_ids.
**Expected Result:**
- tile_name and tile_category are set to "UNKNOWN" for tile_ids without metadata
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_05
**Title:** Validate backward compatibility of reporting metrics
**Description:** Ensure that addition of metadata enrichment does not affect existing metric counts or schema for other columns.
**Preconditions:**
- Historical data and ETL outputs available before enhancement
**Steps to Execute:**
1. Run ETL pipeline for a date before and after metadata integration.
2. Compare counts for unique_tile_views, unique_tile_clicks, etc.
**Expected Result:**
- No change in counts for existing columns
- Only new columns (tile_name, tile_category) are added
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_06
**Title:** Validate schema validation and drift checks
**Description:** Ensure the ETL pipeline and target table schema validation passes and no unexpected schema drift occurs after adding new columns.
**Preconditions:**
- ETL pipeline includes schema validation logic
**Steps to Execute:**
1. Run ETL pipeline with schema validation enabled.
2. Check logs and validation output.
**Expected Result:**
- Schema validation passes
- Only expected columns are present in target table
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_07
**Title:** Validate record count preservation after metadata enrichment
**Description:** Ensure that the LEFT JOIN with metadata does not reduce the number of records in the daily summary output.
**Preconditions:**
- Known event records and expected output counts
**Steps to Execute:**
1. Run ETL pipeline without metadata enrichment and record output count.
2. Run ETL pipeline with metadata enrichment and record output count.
3. Compare counts.
**Expected Result:**
- Output record count remains unchanged after enrichment
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_08
**Title:** Validate reporting outputs for category-level drilldowns
**Description:** Ensure that reporting dashboards or queries can aggregate and filter metrics by tile_category.
**Preconditions:**
- TARGET_HOME_TILE_DAILY_SUMMARY is populated with enriched data
**Steps to Execute:**
1. Run queries or generate reports grouping by tile_category.
2. Validate results against sample metadata and events.
**Expected Result:**
- Metrics can be grouped and filtered by tile_category
- Results match expected category breakdowns
**Linked Jira Ticket:** SCRUM-7567
