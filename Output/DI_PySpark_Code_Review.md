====================================================================
Author: Ascendion AAVA
Date: 
Description: PySpark ETL delta review: tile_category enrichment, join to metadata, backward compatibility, and annotation of all changes.
====================================================================

## Summary of changes

**Deviations Detected (file: Input/home_tile_reporting_etl.py):**

| Line | Type | Description |
|------|------|-------------|
| ~44  | Structural [ADDED] | Added SOURCE_TILE_METADATA as new input source |
| ~110 | Structural [ADDED] | Added left join to SOURCE_TILE_METADATA for tile_category enrichment in daily summary |
| ~111 | Semantic [ADDED] | tile_category column added to output; defaults to 'UNKNOWN' if metadata is missing |
| ~112 | Quality [ADDED] | Inline comments and [ADDED], [MODIFIED], [DEPRECATED] tags for all delta changes |
| ~113 | Structural [MODIFIED] | Target table schema updated to include tile_category |
| ~120 | Quality [DEPRECATED] | Old join logic commented out for traceability |
| ~125 | Quality [ADDED] | Backward compatibility: tile_category assigned 'UNKNOWN' if no metadata match |
| ~140 | Quality [ADDED] | Inline documentation for new/modified logic |


**Categorization of Changes:**

- **Structural:**
    - Addition of SOURCE_TILE_METADATA as a new source table
    - Left join logic for tile_category enrichment
    - Output schema extension to include tile_category
- **Semantic:**
    - Logic to default tile_category to 'UNKNOWN' for unmatched tiles
    - All transformation contract shifts clearly annotated
- **Quality:**
    - Deprecated/commented-out old code for audit/revertibility
    - Inline documentation and delta annotations ([ADDED], [MODIFIED], [DEPRECATED])
    - Backward compatibility logic

**Severity:**
- All changes are HIGH impact (affecting output schema and business logic), but LOW risk due to backward compatibility and clear annotation.


## Additional Optimization Suggestions

1. **Parameterize Process Date:**
   - Accept process date as a parameter (e.g., via notebook widgets or job arguments) for better automation.
2. **Schema Validation:**
   - Add schema validation/assertions post-join to ensure tile_category is always present.
3. **Unit Test Integration:**
   - Integrate test cases from Output/home_tile_reporting_etl_Test.py as part of CI pipeline.
4. **Performance:**
   - If SOURCE_TILE_METADATA is large, consider broadcast join for small dimension table.
5. **Null Handling:**
   - Explicitly handle nulls for all new columns in select/withColumn logic.


||||||Cost Estimation and Justification

- **Development Time:** ~2-3 hours for analysis, code modification, and testing.
- **Testing Time:** <1 hour for scenario development and validation.
- **Maintenance:** Minimal, as backward compatibility and clear annotations are ensured.
- **Justification:** All changes are traceable and revertible; code is ready for production and audit.
