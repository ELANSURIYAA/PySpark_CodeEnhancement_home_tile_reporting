=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for validating the integration of SOURCE_TILE_METADATA and tile_category enrichment in home tile reporting ETL pipeline.
=============================================

### Test Case ID: TC_PCE3_01
**Title:** Validate creation of SOURCE_TILE_METADATA table
**Description:** Ensure the SOURCE_TILE_METADATA table is created with the correct schema and all required columns.
**Preconditions:**
- Access to analytics_db schema
- Sufficient permissions to create tables
**Steps to Execute:**
1. Check if SOURCE_TILE_METADATA exists in analytics_db.
2. Describe the table schema.
3. Verify columns: tile_id, tile_name, tile_category, is_active, updated_ts
**Expected Result:**
- Table exists with all columns and correct data types as per specification.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_02
**Title:** Validate ETL reads active metadata records only
**Description:** Ensure that the ETL pipeline reads only records where is_active = true from SOURCE_TILE_METADATA.
**Preconditions:**
- SOURCE_TILE_METADATA contains both active and inactive records
**Steps to Execute:**
1. Insert test records with is_active true and false.
2. Run the ETL pipeline.
3. Check intermediate dataframe for presence of inactive records.
**Expected Result:**
- Only records with is_active = true are processed in the ETL.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_03
**Title:** Validate tile_category enrichment in target summary
**Description:** Ensure tile_category is correctly joined and populated in TARGET_HOME_TILE_DAILY_SUMMARY.
**Preconditions:**
- SOURCE_TILE_METADATA and source event tables are populated
**Steps to Execute:**
1. Run the ETL pipeline.
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for recent data.
3. Verify that tile_category matches the value in SOURCE_TILE_METADATA for each tile_id.
**Expected Result:**
- tile_category is populated and correct for each tile_id.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_04
**Title:** Validate defaulting tile_category to 'UNKNOWN' when no metadata exists
**Description:** Ensure that if a tile_id in the summary has no matching metadata, tile_category is set to 'UNKNOWN'.
**Preconditions:**
- TARGET_HOME_TILE_DAILY_SUMMARY contains tile_ids not present in SOURCE_TILE_METADATA
**Steps to Execute:**
1. Ensure some tile_ids in the daily summary have no metadata entry.
2. Run the ETL pipeline.
3. Query the output for those tile_ids.
**Expected Result:**
- tile_category is 'UNKNOWN' for all unmatched tile_ids.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_05
**Title:** Validate no record loss during metadata join
**Description:** Ensure that the number of records in the daily summary does not decrease after enriching with metadata (left join preserves all rows).
**Preconditions:**
- Both source event and metadata tables are populated
**Steps to Execute:**
1. Count records in the original summary before join.
2. Count records after metadata enrichment.
**Expected Result:**
- Record count is unchanged after join.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_06
**Title:** Validate only valid/active tile categories are used
**Description:** Ensure only valid, active tile categories are assigned (e.g., OFFERS, HEALTH_CHECKS, PAYMENTS, PERSONAL_FINANCE, UNKNOWN).
**Preconditions:**
- SOURCE_TILE_METADATA contains a mix of valid and invalid categories
**Steps to Execute:**
1. Insert records with valid and invalid tile_category values.
2. Run the ETL pipeline.
3. Query TARGET_HOME_TILE_DAILY_SUMMARY and check unique tile_category values.
**Expected Result:**
- Only valid categories and 'UNKNOWN' appear in the output.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_07
**Title:** Validate schema evolution and backward compatibility
**Description:** Ensure that the addition of tile_category to the target table does not break downstream processes or existing queries.
**Preconditions:**
- Existing dashboards or queries reference TARGET_HOME_TILE_DAILY_SUMMARY
**Steps to Execute:**
1. Add tile_category column via DDL.
2. Run existing queries/dashboards on the target table.
**Expected Result:**
- All queries and dashboards run successfully; no errors due to schema changes.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_08
**Title:** Validate ETL error handling for schema drift
**Description:** Ensure the ETL pipeline fails gracefully with a clear error message if the metadata schema changes unexpectedly.
**Preconditions:**
- ETL pipeline is configured for schema validation
**Steps to Execute:**
1. Alter SOURCE_TILE_METADATA to add/remove a column.
2. Run the ETL pipeline.
**Expected Result:**
- ETL fails with a descriptive error about schema mismatch.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_09
**Title:** Validate partition overwrite and data retention
**Description:** Ensure that the ETL overwrites only the intended partitions in the target table and respects retention policies.
**Preconditions:**
- TARGET_HOME_TILE_DAILY_SUMMARY is partitioned by date
**Steps to Execute:**
1. Run the ETL for a specific date range.
2. Verify only those partitions are overwritten.
3. Check that older data/partitions remain intact.
**Expected Result:**
- Only selected partitions are overwritten; no data loss outside range.
**Linked Jira Ticket:** PCE-3

### Test Case ID: TC_PCE3_10
**Title:** Validate ETL idempotency with metadata enrichment
**Description:** Ensure that rerunning the ETL with the same input does not produce duplicate or inconsistent results in the target table.
**Preconditions:**
- All source tables are populated
**Steps to Execute:**
1. Run the ETL pipeline for a given date.
2. Run the ETL pipeline again for the same date.
3. Compare results in TARGET_HOME_TILE_DAILY_SUMMARY.
**Expected Result:**
- Results are identical; no duplicates or inconsistencies.
**Linked Jira Ticket:** PCE-3
