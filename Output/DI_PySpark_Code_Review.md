==================================================
Author: Ascendion AAVA
Date: 
Description: PySpark ETL pipeline and test script for home tile reporting, enhanced with tile category enrichment per business requirements (SCRUM-7567)
==================================================

## Summary of changes

### List of Deviations (file, line, type)
- **Input/home_tile_reporting_etl.py** (Original):
  - No tile metadata join/enrichment
  - No tile_category field in output
  - No validation for metadata join integrity
  - Legacy write logic for daily summary (no enrichment)
  - No inline documentation for new business logic
- **Output/home_tile_reporting_etl_Pipeline.py** (Enhanced):
  - [ADDED] Tile metadata source and DataFrame creation
  - [MODIFIED] Join between summary and metadata for tile_category enrichment
  - [ADDED] tile_category column in output, defaulted to 'UNKNOWN' if missing
  - [ADDED] Data quality validation for join integrity
  - [DEPRECATED] Legacy write logic commented out
  - [ADDED] Inline documentation and clear annotation tags for all changes
  - [ADDED] Ready-to-run sample data for all sources
  - [ADDED] No breaking changes; fallback and comments provided where logic changed

### Categorization_Changes
- **Structural (High Severity):**
  - Addition of new tile metadata source and join logic
  - Output schema changed (tile_category field added)
  - Data quality validation function added
- **Semantic (High Severity):**
  - Output is now enriched with business-relevant tile_category
  - Defaulting logic for missing metadata (ensures reporting completeness)
  - Validation prevents silent data loss
- **Quality (Medium Severity):**
  - Inline documentation and change management header improved
  - Deprecated code blocks retained for auditability
  - All changes annotated for reviewer clarity

### Additional Optimization Suggestions
- Consider parameterizing process date for full automation
- Add unit test coverage for edge cases (missing metadata, empty input)
- Use config-driven source/target table names for portability
- If scale increases, evaluate broadcast join for metadata if small
- Add logging for join integrity validation exceptions

||||||
## Cost Estimation and Justification
- **Development Effort:** ~4-6 hours for analysis, code delta, testing, and documentation.
- **Testing Effort:** ~2-3 hours for test script creation and validation.
- **Deployment Effort:** Minimal, as code is self-contained and ready for Databricks.
- **Justification:** All changes are backward compatible, maintain auditability, and enrich business reporting as requested.

---

## Ready-to-Run Output
- **home_tile_reporting_etl_Pipeline.py**: Fully self-contained ETL pipeline with all delta modifications, annotations, and sample data.
- **home_tile_reporting_etl_Test.py**: Python script for insert/update scenarios, outputs markdown report with results.

---

**No Azure or GCP code included. All changes are documented inline and in the header.**

---

For full code, see:
- Output/home_tile_reporting_etl_Pipeline.py
- Output/home_tile_reporting_etl_Test.py
