==================================================
Author: Ascendion AAVA
Date: 
Description: Review of PySpark ETL pipeline enhancement – tile metadata join and tile_category enrichment
==================================================

## Summary of changes

### List of deviations (with file, line, and type)

**File:** Input/home_tile_reporting_etl.py

| Line/Section           | Type         | Description                                                                                   |
|-----------------------|--------------|-----------------------------------------------------------------------------------------------|
| Config                | [ADDED]      | SOURCE_TILE_METADATA source table config added                                                 |
| Read Sources          | [ADDED]      | Read df_metadata from SOURCE_TILE_METADATA (active tiles only)                                 |
| Aggregation           | [MODIFIED]   | No change                                                                                     |
| Enrichment            | [ADDED]      | Join df_daily_summary with df_metadata to add tile_category column                            |
| Enrichment            | [ADDED]      | Use coalesce to default tile_category to 'UNKNOWN' if missing                                 |
| Data Quality          | [ADDED]      | validate_metadata_join function to ensure record count matches after metadata join             |
| Write Targets         | [MODIFIED]   | Write df_daily_summary_enhanced (with tile_category) to TARGET_DAILY_SUMMARY                  |
| Write Targets         | [DEPRECATED] | Write of original df_daily_summary to TARGET_DAILY_SUMMARY commented out                      |
| Comments              | [ADDED]      | Inline tags: # [ADDED], # [MODIFIED], # [DEPRECATED] for all changes                          |

### Categorization_Changes

| Change Area     | Type       | Severity      | Description                                                                                   |
|-----------------|------------|--------------|-----------------------------------------------------------------------------------------------|
| Data Structure  | Structural | High         | Addition of tile_category column to summary output                                            |
| Data Join       | Semantic   | High         | Left join with metadata, defaulting missing categories to 'UNKNOWN'                           |
| Validation      | Quality    | Medium       | Explicit record count check after join                                                        |
| Write Logic     | Structural | Medium       | Output table updated to include enriched summary                                              |
| Documentation   | Quality    | Low          | Clear inline comments and change tags                                                         |

### Additional Optimization Suggestions

- **Parameterize process date**: Accept as function argument or via pipeline config for production flexibility.
- **Metadata caching**: If SOURCE_TILE_METADATA is static, cache to optimize repeated joins.
- **Error logging**: Replace raise with logging for production, or propagate errors to orchestrator.
- **Test coverage**: Extend unit tests for more edge cases (invalid tile_id, inactive metadata, etc).
- **Partition pruning**: Ensure Spark/Hive partitions are pruned for all source tables for performance.
- **Column selection**: Explicitly select only required columns from source tables for memory efficiency.

||||||Cost Estimation and Justification
(Calculation steps remain unchanged)
- **Development Cost**: Minor increase due to join and validation logic (~10 lines added)
- **Runtime Cost**: Slightly higher due to join with metadata (depends on metadata table size)
- **Storage Cost**: Additional column in summary output; negligible for typical tile metadata cardinality
- **Quality Gain**: High – enables tile-category analytics, improves reporting, and reduces manual QA
- **Justification**: Added cost is justified by improved reporting granularity and future extensibility

==================================================
