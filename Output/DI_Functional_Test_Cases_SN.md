=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for validating tile metadata integration and reporting enhancements per SCRUM-7567
=============================================

### Test Case ID: TC_SCRUM-7567_01
**Title:** Validate creation of SOURCE_TILE_METADATA table
**Description:** Ensure the new metadata table is created with the correct schema and columns as per technical specification.
**Preconditions:**
- Access to analytics_db schema
- Appropriate privileges to create tables
**Steps to Execute:**
1. Execute the DDL for SOURCE_TILE_METADATA as provided in Input/SOURCE_TILE_METADATA.sql
2. Query the table structure using DESCRIBE TABLE analytics_db.SOURCE_TILE_METADATA
**Expected Result:**
- Table is created with columns: tile_id, tile_name, tile_category, is_active, updated_ts
- Column data types and comments match the specification
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_02
**Title:** Validate ETL pipeline reads metadata table successfully
**Description:** Ensure the ETL job loads data from SOURCE_TILE_METADATA without errors and can join with tile events.
**Preconditions:**
- SOURCE_TILE_METADATA table populated with sample data
- ETL pipeline configured to read from metadata table
**Steps to Execute:**
1. Run the ETL pipeline for daily summary aggregation
2. Check ETL logs for successful read operation from SOURCE_TILE_METADATA
**Expected Result:**
- ETL job completes without errors
- Metadata table is accessed and read successfully
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_03
**Title:** Validate enrichment of tile_category in target summary table
**Description:** Ensure tile_category is correctly joined and appears in reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY
**Preconditions:**
- SOURCE_TILE_METADATA and SOURCE_HOME_TILE_EVENTS have matching tile_id records
- ETL pipeline is updated for join logic
**Steps to Execute:**
1. Run the ETL pipeline
2. Query TARGET_HOME_TILE_DAILY_SUMMARY for recent date
3. Check that tile_category column is present and populated for each tile_id
**Expected Result:**
- tile_category values match those in SOURCE_TILE_METADATA
- No missing or incorrect categories for matching tile_ids
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_04
**Title:** Validate default category assignment when metadata is missing
**Description:** Ensure ETL assigns 'UNKNOWN' to tile_category when no metadata exists for a tile_id
**Preconditions:**
- SOURCE_HOME_TILE_EVENTS contains tile_id not present in SOURCE_TILE_METADATA
- ETL pipeline uses COALESCE logic for join
**Steps to Execute:**
1. Insert a tile_id in SOURCE_HOME_TILE_EVENTS that does not exist in SOURCE_TILE_METADATA
2. Run ETL pipeline
3. Query TARGET_HOME_TILE_DAILY_SUMMARY for that tile_id
**Expected Result:**
- tile_category column for that tile_id is set to 'UNKNOWN'
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_05
**Title:** Validate backward compatibility of record counts
**Description:** Ensure addition of tile_category does not change the count of records in TARGET_HOME_TILE_DAILY_SUMMARY
**Preconditions:**
- Baseline record counts established before metadata integration
- No changes to source event data
**Steps to Execute:**
1. Record the count of records in TARGET_HOME_TILE_DAILY_SUMMARY before ETL change
2. Run updated ETL pipeline
3. Record the count of records after ETL change
**Expected Result:**
- Record counts remain unchanged
- Only new column (tile_category) is added
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_06
**Title:** Validate schema validation and no drift in target table
**Description:** Ensure ETL output schema matches expected and no unexpected columns are added or removed
**Preconditions:**
- Updated ETL pipeline deployed
- Schema validation tests available
**Steps to Execute:**
1. Run ETL pipeline
2. Validate schema of TARGET_HOME_TILE_DAILY_SUMMARY using DESCRIBE TABLE
**Expected Result:**
- Schema matches specification (including new tile_category column)
- No schema drift or errors
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_07
**Title:** Validate dashboard reporting accuracy for tile_category
**Description:** Ensure tile_category appears correctly in reporting outputs and supports category-level drilldowns
**Preconditions:**
- BI dashboard configured to read from TARGET_HOME_TILE_DAILY_SUMMARY
- ETL pipeline run with enriched data
**Steps to Execute:**
1. Refresh BI dashboard after ETL run
2. Drill down by tile_category in dashboard
**Expected Result:**
- Dashboard displays tile_category for each tile
- Category-level metrics (e.g., CTR by category) are accurate
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_08
**Title:** Validate ETL error handling for invalid metadata entries
**Description:** Ensure ETL pipeline handles cases where metadata table has invalid or malformed entries
**Preconditions:**
- SOURCE_TILE_METADATA contains rows with null or invalid tile_id/tile_category
**Steps to Execute:**
1. Insert invalid rows into SOURCE_TILE_METADATA (e.g., null tile_id, blank tile_category)
2. Run ETL pipeline
3. Check ETL logs and target table output
**Expected Result:**
- ETL logs appropriate errors/warnings
- Invalid metadata does not corrupt reporting output
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_09
**Title:** Validate unit and regression tests for metadata enrichment
**Description:** Ensure all unit and regression tests pass for the new tile_category logic
**Preconditions:**
- Unit test suite updated for new logic
- Regression test suite available
**Steps to Execute:**
1. Run unit tests for ETL code
2. Run regression tests for daily summary reporting
**Expected Result:**
- All tests pass
- No failures related to tile_category enrichment
**Linked Jira Ticket:** SCRUM-7567

### Test Case ID: TC_SCRUM-7567_10
**Title:** Validate ETL pipeline performance with metadata join
**Description:** Ensure metadata join does not significantly degrade ETL performance
**Preconditions:**
- Baseline ETL runtime established
- Metadata table populated
**Steps to Execute:**
1. Run ETL pipeline before and after metadata integration
2. Compare runtime metrics
**Expected Result:**
- ETL runtime remains within acceptable limits
- No performance bottlenecks introduced by metadata join
**Linked Jira Ticket:** SCRUM-7567


---

## Cost Estimation and Justification

**Token Usage Analysis:**
- **Input Tokens:** Approximately 5,200 tokens (prompt, input SQL, Jira story)
- **Output Tokens:** Approximately 2,700 tokens (test cases, explanations, metadata)
- **Model Used:** GPT-4

**Cost Breakdown:**
- Input Cost = 5,200 tokens × $0.03/1K tokens = $0.156
- Output Cost = 2,700 tokens × $0.06/1K tokens = $0.162
- **Total Estimated Cost = $0.318**

Formula:
- Input Cost = input_tokens × input_cost_per_token
- Output Cost = output_tokens × output_cost_per_token
- Total Cost = Input Cost + Output Cost
