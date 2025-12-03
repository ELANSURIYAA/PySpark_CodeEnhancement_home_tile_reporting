=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for tile-level metadata enrichment in home tile reporting (JIRA SCRUM-7567)
=============================================

# Functional Test Cases: Home Tile Reporting Enhancement

## Overview
This document provides detailed functional test cases for the implementation of tile-level metadata enrichment in the home tile reporting ETL pipeline, as specified in JIRA SCRUM-7567. The test cases ensure comprehensive coverage of all business and technical requirements, including positive, negative, and edge scenarios. Each test case is traceable to the Jira story.

---

### Test Case ID: TC_SCRUM7567_01
**Title:** Verify creation of SOURCE_TILE_METADATA table
**Description:** Ensure the new metadata table is created in analytics_db with the correct schema and columns.
**Preconditions:**
- Access to analytics_db
- Sufficient privileges to create tables
**Steps to Execute:**
1. Run the DDL script for SOURCE_TILE_METADATA.
2. Query the schema registry/catalog for analytics_db.SOURCE_TILE_METADATA.
3. Verify columns: tile_id, tile_name, tile_category, is_active, updated_ts.
4. Confirm table properties and comments are present.
**Expected Result:**
- Table exists with correct structure and metadata.
- No errors during creation.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_02
**Title:** Validate sample metadata insertion
**Description:** Confirm that sample metadata records can be inserted and retrieved from SOURCE_TILE_METADATA.
**Preconditions:**
- SOURCE_TILE_METADATA table exists
**Steps to Execute:**
1. Insert sample records into SOURCE_TILE_METADATA (e.g., TILE_001, TILE_002).
2. Query the table for all records.
3. Verify inserted values for tile_id, tile_name, tile_category, is_active, updated_ts.
**Expected Result:**
- Sample records are present and match expected values.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_03
**Title:** ETL pipeline reads metadata table successfully
**Description:** Ensure the ETL pipeline can read from SOURCE_TILE_METADATA and load active tile metadata.
**Preconditions:**
- ETL pipeline deployed
- SOURCE_TILE_METADATA table populated
**Steps to Execute:**
1. Trigger ETL pipeline run.
2. Inspect ETL logs for successful read of metadata table.
3. Verify only records with is_active = True are loaded.
**Expected Result:**
- ETL pipeline reads metadata table without errors.
- Only active tiles are loaded into memory.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_04
**Title:** Enrich daily summary with tile_category
**Description:** Validate that the ETL joins metadata and adds tile_category to the target table for each tile.
**Preconditions:**
- ETL pipeline updated to join metadata
- SOURCE_TILE_METADATA and source event tables populated
**Steps to Execute:**
1. Run the ETL pipeline for a given date.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for that date.
3. Verify that each record has a tile_category populated from metadata.
**Expected Result:**
- tile_category column exists and is populated for each tile.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_05
**Title:** Default tile_category to 'UNKNOWN' for unmapped tiles
**Description:** Ensure that if a tile_id has no metadata, the ETL assigns 'UNKNOWN' as tile_category in the target table.
**Preconditions:**
- ETL pipeline updated for backward compatibility
- TARGET_HOME_TILE_DAILY_SUMMARY table exists
**Steps to Execute:**
1. Insert an event in source tables with a tile_id not present in SOURCE_TILE_METADATA.
2. Run the ETL pipeline.
3. Query TARGET_HOME_TILE_DAILY_SUMMARY for the new tile_id.
4. Check the tile_category value.
**Expected Result:**
- tile_category is 'UNKNOWN' for unmapped tile_ids.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_06
**Title:** Verify addition of tile_category column to target table
**Description:** Confirm that TARGET_HOME_TILE_DAILY_SUMMARY has the new tile_category column with correct data type and comment.
**Preconditions:**
- Target table schema migration executed
**Steps to Execute:**
1. Query table schema for TARGET_HOME_TILE_DAILY_SUMMARY.
2. Verify presence of tile_category column.
3. Check data type is STRING and comment is correct.
**Expected Result:**
- tile_category column exists with correct properties.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_07
**Title:** Validate reporting output accuracy for tile_category
**Description:** Ensure reporting queries and dashboards reflect correct tile_category values in outputs.
**Preconditions:**
- ETL pipeline run completed
- Reporting queries/dashboards configured for new column
**Steps to Execute:**
1. Execute reporting query on TARGET_HOME_TILE_DAILY_SUMMARY.
2. Group results by tile_category.
3. Validate counts and metrics per category.
**Expected Result:**
- Reporting output shows accurate tile_category groupings and metrics.
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_08
**Title:** ETL unit test: schema validation and no drift
**Description:** Validate that the ETL pipeline passes schema validation tests and does not introduce schema drift.
**Preconditions:**
- ETL pipeline with schema validation logic
**Steps to Execute:**
1. Run ETL unit tests (schema validation)
2. Check for errors or warnings related to schema drift
3. Verify all columns match expected schema
**Expected Result:**
- ETL passes schema validation
- No schema drift detected
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_09
**Title:** ETL regression test: record count consistency
**Description:** Ensure that the record counts in the target table remain consistent after metadata enrichment (no loss or duplication).
**Preconditions:**
- Historical data available for comparison
- ETL pipeline updated
**Steps to Execute:**
1. Run ETL pipeline before and after metadata enrichment
2. Compare record counts for a given date
3. Verify no loss or duplication of records
**Expected Result:**
- Record counts remain consistent
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_10
**Title:** Negative test: invalid metadata insertion
**Description:** Attempt to insert a record into SOURCE_TILE_METADATA with missing required fields and verify error handling.
**Preconditions:**
- SOURCE_TILE_METADATA table exists
**Steps to Execute:**
1. Attempt to insert a record with NULL tile_id
2. Attempt to insert a record with invalid data types (e.g., non-boolean for is_active)
3. Observe error messages
**Expected Result:**
- Table enforces NOT NULL constraint for tile_id
- Data type errors are raised appropriately
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_11
**Title:** Edge case: inactive tiles in metadata
**Description:** Ensure that tiles marked as inactive in metadata are excluded from reporting enrichment.
**Preconditions:**
- SOURCE_TILE_METADATA contains inactive tiles
**Steps to Execute:**
1. Mark a tile as is_active = False in SOURCE_TILE_METADATA
2. Run ETL pipeline
3. Query TARGET_HOME_TILE_DAILY_SUMMARY for that tile_id
**Expected Result:**
- Inactive tile is not enriched with tile_category
- tile_category is 'UNKNOWN' or tile is excluded per business logic
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM7567_12
**Title:** Edge case: future-proofing for new categories
**Description:** Validate that the ETL and reporting pipelines can handle new tile categories added to metadata without errors.
**Preconditions:**
- SOURCE_TILE_METADATA updated with new tile_category values
**Steps to Execute:**
1. Add a new category to SOURCE_TILE_METADATA (e.g., "Rewards")
2. Run ETL pipeline
3. Query TARGET_HOME_TILE_DAILY_SUMMARY for tiles in new category
**Expected Result:**
- New categories appear in reporting outputs without errors
**Linked Jira Ticket:** SCRUM-7567

---

## Cost Estimation and Justification

**Token Usage Analysis:**
- Input Tokens: ~13,700 tokens (prompt, DDLs, Jira story, instructions)
- Output Tokens: ~2,200 tokens (functional test cases, metadata)
- Model Used: GPT-4 (detected from context)

**Cost Breakdown:**
- Input Cost = 13,700 × $0.03/1K tokens = $0.411
- Output Cost = 2,200 × $0.06/1K tokens = $0.132
- **Total Cost = $0.543**

**Cost Formula:**
```
Total Cost = (Input_Tokens × Input_Rate_Per_1K) + (Output_Tokens × Output_Rate_Per_1K)
Total Cost = (13,700 × $0.03/1K) + (2,200 × $0.06/1K) = $0.543
```
