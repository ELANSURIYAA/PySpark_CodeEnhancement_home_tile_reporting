==================================================
Author: Ascendion AAVA
Date: 
Description: PySpark ETL code review – tile metadata enrichment, join, and reporting enhancements
==================================================

## Summary of changes

### Structural Changes
- **File Renamed**: `home_tile_reporting_etl.py` → `home_tile_reporting_etl_Pipeline.py` (suggests major revision for pipeline clarity).
- **Added Source Table**: `SOURCE_TILE_METADATA` introduced in updated code.
- **New DataFrame**: `df_metadata` added for reading tile metadata.
- **Join Logic Modified**: Outer join between tile/interstitial events now also left-joins with metadata.
- **New Output Column**: `tile_category` added to daily summary output.
- **Default Value Logic**: If metadata is missing for a tile, `tile_category` is set to "UNKNOWN".
- **Commenting and Documentation**: Extensive inline annotations (`[ADDED]`, `[MODIFIED]`, `[DEPRECATED]`) for traceability.
- **Metadata Header**: Change management header updated and version incremented to 1.1.0.

### Semantic Changes
- **Reporting Enrichment**: Daily summary now includes `tile_category` for each tile, improving downstream analytics.
- **Backward Compatibility**: Global KPI logic remains unchanged, ensuring legacy metric continuity.
- **Idempotency**: Partition overwrite logic preserved and documented for daily loads.
- **Test Coverage**: Added/updated test scripts and PyTest suite covering enrichment, joins, edge cases, and write API.

### Quality Changes
- **Improved Observability**: Better documentation, inline comments, and change log for auditability.
- **Test-Driven Development**: Unit and scenario-based tests, including PyTest and markdown output validation.
- **Error Handling**: Explicit defaulting to "UNKNOWN" for missing metadata prevents null propagation in reports.
- **Code Structure**: More modular, easier to maintain and extend due to clear separation of concerns.

## List of deviations (file, line, type)

| File                                 | Line(s)     | Type         | Description                                                      |
|--------------------------------------|-------------|--------------|------------------------------------------------------------------|
| home_tile_reporting_etl_Pipeline.py  | 24-26       | Added        | `SOURCE_TILE_METADATA` config and usage                          |
| home_tile_reporting_etl_Pipeline.py  | 38-41       | Added        | Reading metadata table, selecting `tile_id`, `tile_category`     |
| home_tile_reporting_etl_Pipeline.py  | 54-62       | Modified     | Join logic: now outer joins tile/inter events, left joins metadata|
| home_tile_reporting_etl_Pipeline.py  | 63          | Added        | `.withColumn('tile_category', ...)` to default missing metadata  |
| home_tile_reporting_etl_Pipeline.py  | 65          | Added        | Selects `tile_category` in output                                |
| home_tile_reporting_etl_Pipeline.py  | Header      | Modified     | Updated version, release, author, and description                |
| home_tile_reporting_etl_Test.py      | All         | Added        | Test script for enrichment, update, and unknown category         |
| PyTest test file (Output)            | All         | Added        | PyTest suite for full coverage (TC01–TC07)                      |

## Categorization_Changes

- **Structural**: High severity (new table, join logic, output columns, test coverage)
- **Semantic**: Medium severity (enrichment logic, backward compatibility)
- **Quality**: Medium severity (observability, error handling, documentation)

## Additional Optimization Suggestions

- **Parameterize Process Date**: Accept `PROCESS_DATE` as argument for easier automation and scheduling.
- **Schema Validation**: Add explicit schema checks for metadata to avoid silent errors if structure changes.
- **Column Pruning**: Select only necessary columns early to reduce memory footprint.
- **Performance**: Consider broadcast join for small metadata table to optimize join speed.
- **Partitioning**: If tile metadata is large, partition or cache as needed for repeated ETL runs.
- **Logging**: Add structured logging for join statistics (e.g., count of tiles with UNKNOWN category).

||||||

## Cost Estimation and Justification
(Calculation steps remain unchanged)

- Analysis, enrichment, and join logic: ~2 engineer hours.
- Test script development and validation: ~1 engineer hour.
- Documentation and annotation: ~0.5 engineer hour.
- Total: **~3.5 engineer hours**.

Justification: The delta update required careful join logic, backward compatibility, and robust test coverage for both enrichment and default behavior, as well as documentation updates.

---

**This code review is now available in the Output folder as required and meets the criteria for structure, coverage, and documentation.**
