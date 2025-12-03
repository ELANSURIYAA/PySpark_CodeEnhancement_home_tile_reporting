==================================================
Author: Ascendion AAVA
Date: 
Description: PySpark ETL code review for home tile reporting enhancement per SCRUM-7567. Compares legacy and updated pipeline logic, annotates changes, and provides actionable feedback.
==================================================

## Summary of changes

### Files Compared
- Input/home_tile_reporting_etl.py (Legacy)
- home_tile_reporting_etl_Pipeline.py (Enhanced per SCRUM-7567)

### List of Deviations

| File                               | Line(s)         | Type        | Description                                                                                          |
|------------------------------------|-----------------|-------------|------------------------------------------------------------------------------------------------------|
| home_tile_reporting_etl_Pipeline.py| 38-40           | Structural  | [ADDED] Introduction of SOURCE_TILE_METADATA table for tile category enrichment                      |
| home_tile_reporting_etl_Pipeline.py| 51-55           | Structural  | [ADDED] Read and filter active tile metadata for enrichment                                          |
| home_tile_reporting_etl_Pipeline.py| 81-97           | Semantic    | [MODIFIED] Join with metadata to add tile_category to daily summary, left join, default UNKNOWN      |
| home_tile_reporting_etl_Pipeline.py| 99-108          | Quality     | [ADDED] Data quality validation for record count after metadata join                                 |
| home_tile_reporting_etl_Pipeline.py| 126-128         | Quality     | [MODIFIED] Enhanced daily summary write includes tile_category                                       |
| home_tile_reporting_etl_Pipeline.py| 129             | Semantic    | [UNCHANGED] Global KPIs logic remains identical                                                      |
| home_tile_reporting_etl_Pipeline.py| Throughout      | Quality     | [MODIFIED] Improved documentation, change log, and inline comments for auditability                  |

### Categorization_Changes

#### Structural Changes
- Addition of tile metadata source and enrichment logic (High severity: impacts reporting contract)
- Additional DataFrame (`df_metadata`) and join operation

#### Semantic Changes
- Daily summary now includes `tile_category` per tile, with fallback to 'UNKNOWN' (High severity: changes output schema)
- Left join ensures all tiles are preserved; semantic guarantee for enrichment

#### Quality Changes
- Data validation for record count post-join (Medium severity: prevents silent data loss)
- Enhanced documentation and code comments (Low severity: improves maintainability)

### Additional Optimization Suggestions
- Parameterize `PROCESS_DATE` for full pipeline automation (currently static)
- Consider caching `df_metadata` if reused in multiple joins for performance
- Add logging for validation and enrichment steps for operational monitoring
- Unit test coverage is strong; consider data-driven tests for additional edge cases
- Future: Make tile_category enrichment configurable (e.g., allow multiple category fields)

||||||

## Cost Estimation and Justification

- **Development Effort:**
    - Tile metadata enrichment logic: 2-3 hours (design, implementation, testing)
    - Data validation logic: 1 hour
    - Documentation and change log: 30 min
    - Test script and unit tests: 2-3 hours
- **Compute Impact:**
    - Minor increase due to additional join (tile_category enrichment), mitigated by join type and small metadata size
    - No change to aggregation complexity
- **Maintenance Impact:**
    - Improved auditability and error handling reduces future support cost
    - Schema change requires downstream consumers to update expectations
- **Justification:**
    - Enhancement aligns with SCRUM-7567 requirements, provides richer reporting, and preserves legacy logic for audit
    - All changes are backward compatible except output schema (tile_category addition)
    - Data validation and error handling mitigate operational risks

==================================================
