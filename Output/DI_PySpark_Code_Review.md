==================================================
Author: Ascendion AAVA
Date: 
Description: Code review and change analysis for Home Tile Reporting ETL pipeline enrichment (Jira PCE-3)
==================================================

## Summary of changes

### List of Deviations (with file, line, and type)

**home_tile_reporting_etl.py → home_tile_reporting_etl_Pipeline.py**

| Change Type   | File/Section                                    | Line/Block    | Description                                                                            |
|--------------|-------------------------------------------------|--------------|----------------------------------------------------------------------------------------|
| [ADDED]      | CONFIGURATION                                   | ~22          | SOURCE_TILE_METADATA source table added for enrichment                                  |
| [ADDED]      | READ SOURCE TABLES                              | ~41          | Read df_metadata from SOURCE_TILE_METADATA                                              |
| [MODIFIED]   | DAILY TILE SUMMARY AGGREGATION                  | ~61-80       | Join df_tile_agg and df_inter_agg with df_metadata to enrich tile_category              |
| [MODIFIED]   | DAILY TILE SUMMARY AGGREGATION                  | ~80          | tile_category column added to output; defaults to "UNKNOWN" if missing                 |
| [MODIFIED]   | TARGET TABLE SCHEMA                             | ~85          | TARGET_HOME_TILE_DAILY_SUMMARY now includes tile_category                               |
| [DEPRECATED] | WRITE TARGET TABLES                             | ~101-110     | Legacy .option("replaceWhere", ...) commented; .mode("overwrite") used for demo        |
| [ADDED]      | CHANGE LOG                                      | Header        | Version 1.1.0: Added tile_category enrichment per Jira PCE-3                            |
| [ADDED]      | Inline comments                                 | Throughout    | Change tags [ADDED], [MODIFIED], [DEPRECATED] for traceability                         |
| [UNCHANGED]  | GLOBAL KPIs AGGREGATION                         | ~86-100      | No change per Jira scope                                                               |

**home_tile_reporting_etl_Test.py**

| Change Type   | File/Section                                    | Line/Block    | Description                                                                            |
|--------------|-------------------------------------------------|--------------|----------------------------------------------------------------------------------------|
| [ADDED]      | Test Insert Scenario                            | ~12-32        | Validates enrichment for new tile (insert)                                             |
| [ADDED]      | Test Update Scenario                            | ~34-54        | Validates enrichment for existing tile (update)                                        |
| [ADDED]      | Output Reporting                                | ~56-70        | Prints input/output and status for both scenarios                                      |

**Output/home_tile_reporting_etl_Pipeline_pytest.py** (from context)

| Change Type   | File/Section                                    | Line/Block    | Description                                                                            |
|--------------|-------------------------------------------------|--------------|----------------------------------------------------------------------------------------|
| [ADDED]      | PyTest Test Suite                               | All           | Validates enrichment, aggregation, error handling, edge cases, schema, KPIs             |

### Categorization_Changes: structural, semantic, quality with severity

**Structural**
- Addition of new source table (SOURCE_TILE_METADATA) and associated DataFrame (df_metadata) [High]
- Output schema change: tile_category column added to TARGET_HOME_TILE_DAILY_SUMMARY [High]
- Join logic updated to combine metadata with tile/interstitial aggregates [High]

**Semantic**
- Aggregation logic now enriches each tile with its category from metadata [High]
- tile_category defaults to "UNKNOWN" if not found [Medium]
- No semantic changes to global KPIs (business logic unchanged) [None]

**Quality**
- Inline documentation and change tags for maintainability [Medium]
- Test coverage for insert/update scenarios and edge cases [High]
- Idempotency and error handling improved in test suite [Medium]

### Additional Optimization Suggestions
- For production, ensure SOURCE_TILE_METADATA is refreshed and indexed for join performance.
- Consider parameterizing PROCESS_DATE for greater pipeline flexibility.
- Add logging for enrichment failures or unexpected "UNKNOWN" tile_category assignments.
- Validate schema consistency between metadata and event sources before join.
- For large-scale workloads, consider broadcast join or partitioning strategies for metadata.
- Expand test suite to include performance benchmarks and stress tests.

||||||
## Cost Estimation and Justification
(Calculation steps remain unchanged)

- Code changes: 2 business days (analysis, code, test, documentation).
- Testing: 1 day (unit, regression, backward compatibility).
- Total: 3 business days.
- Justification: Schema change, join logic, test coverage, documentation, and backward compatibility validation.

---
Both files are available in the Output folder of the GitHub repo for immediate use.
