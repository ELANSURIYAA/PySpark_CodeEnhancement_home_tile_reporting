==================================================
Author: Ascendion AAVA
Date: 
Description: PySpark ETL pipeline enhancement review – tile metadata enrichment, code structure, and quality analysis
==================================================

## Summary of changes

**File:** Input/home_tile_reporting_etl.py → home_tile_reporting_etl_Pipeline.py

### 1. Structural Changes
- **[ADDED] Source Table:** `SOURCE_TILE_METADATA` introduced for tile metadata enrichment (Line ~28)
- **[ADDED] Read Metadata:** `df_metadata = spark.table(SOURCE_TILE_METADATA).filter(F.col("is_active") == True)` (Line ~49)
- **[ADDED] Join:** Daily summary aggregation now joins metadata (`tile_id`, `tile_category`) (Line ~83)
- **[ADDED] Column:** `tile_category` column added to daily summary output (Line ~88)
- **[MODIFIED] SparkSession:** Now supports `SparkSession.getActiveSession()` for Spark Connect compatibility (Line ~37)
- **[DEPRECATED]** Old join logic (without metadata enrichment) commented out (Line ~79)

### 2. Semantic Changes
- **[ADDED] Default/Null Handling:** `tile_category` defaults to "UNKNOWN" if no metadata mapping (Line ~88)
- **[ADDED] Validation:** Prints count of active metadata records loaded and warns if any tiles have `tile_category = "UNKNOWN"` (Lines ~51, ~92)
- **[ADDED] Schema Validation:** Checks that output DataFrame contains new `tile_category` column (Line ~96)
- **[MODIFIED] Aggregation Contract:** Output schema now includes `tile_category` for each tile, changing downstream contract for consumers of daily summary
- **[UNCHANGED] Global KPIs:** No changes to global KPI calculations (Lines ~101-117)

### 3. Quality & Robustness
- **[ADDED] Defensive Programming:** Assertion for schema validation (Line ~96)
- **[ADDED] Logging:** Print statements for metadata load and enrichment warnings (Lines ~51, ~92)
- **[MODIFIED] Partition Overwrite:** No change, but functionally validated for idempotency
- **[ADDED] Test Coverage:** Python and PyTest scripts provided for enrichment, aggregation, error handling, and edge cases

---

## List of Deviations (with file, line, type)

| File                                 | Line(s) | Type        | Description                                                      | Severity |
|--------------------------------------|---------|-------------|------------------------------------------------------------------|----------|
| home_tile_reporting_etl_Pipeline.py  | ~28     | Structural  | Added SOURCE_TILE_METADATA source table                          | High     |
| home_tile_reporting_etl_Pipeline.py  | ~49     | Structural  | Added df_metadata read and active filter                        | High     |
| home_tile_reporting_etl_Pipeline.py  | ~83     | Structural  | Join daily summary with metadata for tile_category              | High     |
| home_tile_reporting_etl_Pipeline.py  | ~88     | Semantic    | tile_category defaults to 'UNKNOWN' if not found                | Medium   |
| home_tile_reporting_etl_Pipeline.py  | ~51,92  | Quality     | Print/logging for metadata load and unmapped tiles              | Low      |
| home_tile_reporting_etl_Pipeline.py  | ~96     | Quality     | Assertion for schema validation                                 | Medium   |
| home_tile_reporting_etl_Pipeline.py  | ~79     | Structural  | Deprecated: old join logic without enrichment                   | Low      |
| home_tile_reporting_etl_Pipeline.py  | ~37     | Structural  | SparkSession.getActiveSession() for compatibility               | Low      |

---

## Categorization of Changes
- **Structural:** Addition of metadata source, enrichment join, new output column, SparkSession compatibility
- **Semantic:** Logic for enrichment, default value handling, downstream contract change (output schema)
- **Quality:** Logging, validation, assertion for schema, improved error handling

Severity: Most changes are **High** as they alter the ETL contract and core logic. Quality improvements are Medium/Low but increase robustness.

---

## Additional Optimization Suggestions
- **Parameterize process date:** Instead of hardcoding `PROCESS_DATE`, pass as an argument or environment variable for production scheduling.
- **Metadata Caching:** If metadata is small, consider caching `df_metadata` for performance if used in multiple joins.
- **Config-driven sources/targets:** Use config files or environment variables for table names to improve portability.
- **Error Handling:** Replace print statements with logging framework for better observability in production.
- **Unit Test Automation:** Integrate PyTest suite into CI pipeline for automated regression.
- **Partition Pruning:** Ensure that all joins and filters leverage partition columns for Spark performance.
- **Documentation:** Update downstream consumers about schema change (`tile_category` added).

---

## Cost Estimation and Justification
- **Development Cost:**
    - Metadata enrichment logic: ~2-3 dev days (analysis, coding, testing)
    - Schema validation, defensive programming: ~1 dev day
    - Test script creation: ~1 dev day
    - Update documentation, downstream contract: ~0.5 dev day
    - Total: ~4.5-5.5 dev days
- **Runtime Cost:**
    - Metadata join: Negligible if metadata table is small; may increase slightly for large tables (recommend broadcast join if applicable)
    - Additional column: Minimal impact on storage and compute
- **Justification:**
    - Provides critical enrichment for analytics
    - Improves robustness and error handling
    - Reduces manual review time with automated validation and tests

---

**Review completed. All code changes are clearly documented, categorized, and validated. Downstream contract change (tile_category column) must be communicated.**
