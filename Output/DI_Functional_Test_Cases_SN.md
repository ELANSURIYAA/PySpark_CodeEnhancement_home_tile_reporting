=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for validating the addition of SOURCE_TILE_METADATA and tile_category enrichment in Home Tile Reporting pipeline
=============================================

# Functional Test Cases: Home Tile Reporting Enhancement

## Overview
This document contains comprehensive functional test cases to validate the implementation of SCRUM-7567, covering the creation of the SOURCE_TILE_METADATA table, enrichment of the ETL pipeline, addition of tile_category to the target summary table, and ensuring backward compatibility and schema integrity.

---

### Test Case ID: TC_SCRUM-7567_01
**Title:** Validate creation of SOURCE_TILE_METADATA table
**Description:** Ensure the SOURCE_TILE_METADATA table is created with correct schema and columns as per specification.
**Preconditions:**
- Access to analytics_db schema
- Database migration scripts are available
**Steps to Execute:**
1. Run the DDL script to create SOURCE_TILE_METADATA.
2. Query system catalog for table existence.
3. Verify all columns: tile_id, tile_name, tile_category, is_active, updated_ts.
**Expected Result:**
- Table exists in analytics_db
- All columns are present with correct data types and comments
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_02
**Title:** Validate insertion and retrieval of tile metadata
**Description:** Ensure metadata entries can be inserted and retrieved correctly from SOURCE_TILE_METADATA.
**Preconditions:**
- SOURCE_TILE_METADATA table exists
**Steps to Execute:**
1. Insert sample rows into SOURCE_TILE_METADATA.
2. Query table for inserted rows.
3. Verify values of tile_id, tile_name, tile_category, is_active, updated_ts.
**Expected Result:**
- Inserted rows are retrievable and match input values
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_03
**Title:** Validate ETL pipeline joins metadata and enriches tile_category
**Description:** Ensure ETL pipeline performs LEFT JOIN with SOURCE_TILE_METADATA and enriches each output record with tile_category.
**Preconditions:**
- SOURCE_TILE_METADATA table populated
- ETL pipeline is runnable
**Steps to Execute:**
1. Run daily summary ETL job
2. Inspect output in TARGET_HOME_TILE_DAILY_SUMMARY
3. Verify tile_category column is populated per metadata mapping
**Expected Result:**
- Each tile_id in target table has correct tile_category from metadata
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_04
**Title:** Validate default tile_category for unmapped tiles
**Description:** Ensure tiles with no metadata mapping receive tile_category='UNKNOWN' in the output for backward compatibility.
**Preconditions:**
- ETL pipeline is runnable
- Some tile_ids present in events but not in metadata
**Steps to Execute:**
1. Run ETL job with events referencing unmapped tile_ids
2. Inspect TARGET_HOME_TILE_DAILY_SUMMARY output
3. Verify tile_category is 'UNKNOWN' for unmapped tiles
**Expected Result:**
- tile_category is 'UNKNOWN' where no metadata exists
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_05
**Title:** Validate addition of tile_category column to TARGET_HOME_TILE_DAILY_SUMMARY
**Description:** Ensure target table schema is updated to include tile_category with correct data type and comment.
**Preconditions:**
- Schema migration scripts available
**Steps to Execute:**
1. Run ALTER TABLE script to add tile_category column
2. Query system catalog for column existence
3. Verify column data type is STRING and comment matches specification
**Expected Result:**
- tile_category column exists with correct type and comment
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_06
**Title:** Validate reporting outputs include tile_category
**Description:** Ensure tile_category appears accurately in reporting outputs and dashboards (data layer only).
**Preconditions:**
- ETL pipeline run completed
- Reporting queries available
**Steps to Execute:**
1. Query TARGET_HOME_TILE_DAILY_SUMMARY for recent data
2. Verify tile_category column is present and populated
3. Validate sample records for correct category assignment
**Expected Result:**
- tile_category is present and accurate in reporting outputs
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_07
**Title:** Validate ETL pipeline runs without schema drift errors
**Description:** Ensure the ETL pipeline executes successfully and does not encounter schema drift due to new column.
**Preconditions:**
- ETL pipeline deployed
- Updated target table schema
**Steps to Execute:**
1. Trigger ETL pipeline for daily run
2. Monitor job logs for errors
3. Verify no schema drift or write errors occur
**Expected Result:**
- ETL pipeline completes successfully with no schema drift errors
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_08
**Title:** Validate record counts are unchanged after enrichment
**Description:** Ensure the addition of tile_category does not change the count of output records (backward compatibility).
**Preconditions:**
- Baseline record count established prior to change
**Steps to Execute:**
1. Run ETL pipeline before and after schema change
2. Compare record counts for a given date range
3. Confirm no change in record counts
**Expected Result:**
- Record counts remain unchanged after enrichment
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_09
**Title:** Validate handling of NULL and edge values in metadata
**Description:** Ensure ETL handles NULLs, empty strings, and unusual values in metadata gracefully.
**Preconditions:**
- Metadata table contains edge cases (NULLs, empty, special chars)
**Steps to Execute:**
1. Insert edge case rows into SOURCE_TILE_METADATA
2. Run ETL pipeline
3. Inspect target output for handling of edge cases
**Expected Result:**
- NULL or empty tile_category mapped to 'UNKNOWN'
- No ETL failures for special characters
**Linked Jira Ticket:** SCRUM-7567

---

### Test Case ID: TC_SCRUM-7567_10
**Title:** Validate schema documentation and data dictionary updates
**Description:** Ensure documentation for new table and column is updated and matches implementation.
**Preconditions:**
- Documentation is accessible
**Steps to Execute:**
1. Review data dictionary and schema docs
2. Compare with actual implementation
3. Verify all new fields are documented
**Expected Result:**
- Documentation matches implemented schema and describes tile_category
**Linked Jira Ticket:** SCRUM-7567
