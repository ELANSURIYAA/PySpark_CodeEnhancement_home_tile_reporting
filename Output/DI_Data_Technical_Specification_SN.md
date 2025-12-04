=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target reporting metrics with tile category
=============================================

# Technical Specification for Home Tile Reporting Enhancement - Add Tile Metadata

## Introduction

### Business Context
Product Analytics has requested the addition of tile-level metadata to enrich reporting dashboards. Currently, all tile metrics (views, clicks, CTRs, interstitial interactions) are aggregated only at tile_id level with no business grouping. The business wants to include Tile Category in the daily reporting outputs to enable better reporting and performance tracking at a functional level.

### Enhancement Scope
This enhancement involves:
- Adding a new source table `SOURCE_TILE_METADATA` containing tile category, tile name, and business grouping
- Modifying the existing ETL pipeline to incorporate metadata enrichment
- Extending the target daily summary table with tile category information
- Ensuring backward compatibility and maintaining data quality

### Business Benefits
- View performance metrics by category (e.g., "Personal Finance Tiles vs Health Tiles")
- Enable category-level CTR comparisons
- Support product managers tracking feature adoption
- Improve dashboard drilldowns in Power BI/Tableau
- Allow future segmentation (e.g., enable/disable tiles by category)

## Code Changes

### 1. ETL Pipeline Modifications (`home_tile_reporting_etl.py`)

#### 1.1 Configuration Updates
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

#### 1.2 Source Data Reading
```python
# Add metadata table reading
df_metadata = spark.table(SOURCE_TILE_METADATA)
```

#### 1.3 Daily Summary Aggregation Enhancement
**Current Code Location:** Lines 45-65 in `home_tile_reporting_etl.py`

**Modified Logic:**
```python
# Enhanced daily summary with metadata join
df_daily_summary_enriched = (
    df_daily_summary.join(
        df_metadata.select("tile_id", "tile_category", "tile_name"), 
        "tile_id", 
        "left"
    )
    .withColumn(
        "tile_category", 
        F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))
    )
    .withColumn(
        "tile_name", 
        F.coalesce(F.col("tile_name"), F.lit("UNKNOWN"))
    )
    .select(
        "date",
        "tile_id",
        "tile_category",
        "tile_name",
        "unique_tile_views",
        "unique_tile_clicks",
        "unique_interstitial_views",
        "unique_interstitial_primary_clicks",
        "unique_interstitial_secondary_clicks"
    )
)
```

#### 1.4 Write Operation Update
**Current Code Location:** Lines 85-95 in `home_tile_reporting_etl.py`

**Modified Logic:**
```python
# Update the overwrite partition call
overwrite_partition(df_daily_summary_enriched, TARGET_DAILY_SUMMARY)
```

### 2. Error Handling and Data Quality Checks
```python
# Add data quality validation
def validate_metadata_join(df_before, df_after):
    """
    Validate that metadata join doesn't change record counts
    """
    count_before = df_before.count()
    count_after = df_after.count()
    
    if count_before != count_after:
        raise ValueError(f"Record count mismatch: Before={count_before}, After={count_after}")
    
    print(f"Validation passed: {count_after} records maintained")

# Apply validation
validate_metadata_join(df_daily_summary, df_daily_summary_enriched)
```

## Data Model Updates

### 1. New Source Table: `SOURCE_TILE_METADATA`

**Table Structure:**
```sql
CREATE TABLE IF NOT EXISTS analytics_db.SOURCE_TILE_METADATA 
(
    tile_id        STRING    COMMENT 'Tile identifier',
    tile_name      STRING    COMMENT 'User-friendly tile name',
    tile_category  STRING    COMMENT 'Business or functional category of tile',
    is_active      BOOLEAN   COMMENT 'Indicates if tile is currently active',
    updated_ts     TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'Master metadata for homepage tiles, used for business categorization and reporting enrichment';
```

**Key Characteristics:**
- **Storage Format:** Delta Lake
- **Update Pattern:** SCD Type 1 (overwrite)
- **Data Refresh:** Manual/On-demand updates by business teams
- **Primary Key:** tile_id

### 2. Target Table Enhancement: `TARGET_HOME_TILE_DAILY_SUMMARY`

**Updated Schema:**
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
(
    date                                DATE       COMMENT 'Reporting date',
    tile_id                             STRING     COMMENT 'Tile identifier',
    tile_category                       STRING     COMMENT 'Business category of the tile',
    tile_name                           STRING     COMMENT 'User-friendly tile name',
    unique_tile_views                   LONG       COMMENT 'Distinct users who viewed the tile',
    unique_tile_clicks                  LONG       COMMENT 'Distinct users who clicked the tile',
    unique_interstitial_views           LONG       COMMENT 'Distinct users who viewed interstitial',
    unique_interstitial_primary_clicks  LONG       COMMENT 'Distinct users who clicked primary button',
    unique_interstitial_secondary_clicks LONG      COMMENT 'Distinct users who clicked secondary button'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily aggregated tile-level metrics for reporting with business metadata';
```

**Schema Changes:**
- **Added Columns:**
  - `tile_category STRING` - Business/functional category
  - `tile_name STRING` - User-friendly display name
- **Backward Compatibility:** Maintained through default values

### 3. Data Lineage Impact

**Updated Data Flow:**
```
SOURCE_HOME_TILE_EVENTS ──┐
                          ├─► ETL Pipeline ──► TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_INTERSTITIAL_EVENTS ┤                      (Enhanced with metadata)
                          │
SOURCE_TILE_METADATA ─────┘
```

## Source-to-Target Mapping

### 1. Enhanced Daily Summary Mapping

| Source Field | Source Table | Target Field | Target Table | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------------|
| date | Derived | date | TARGET_HOME_TILE_DAILY_SUMMARY | `F.lit(PROCESS_DATE)` |
| tile_id | SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | Direct mapping |
| tile_category | SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | `F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))` |
| tile_name | SOURCE_TILE_METADATA | tile_name | TARGET_HOME_TILE_DAILY_SUMMARY | `F.coalesce(F.col("tile_name"), F.lit("UNKNOWN"))` |
| user_id | SOURCE_HOME_TILE_EVENTS | unique_tile_views | TARGET_HOME_TILE_DAILY_SUMMARY | `F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id")))` |
| user_id | SOURCE_HOME_TILE_EVENTS | unique_tile_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | `F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id")))` |
| user_id | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_views | TARGET_HOME_TILE_DAILY_SUMMARY | `F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id")))` |
| user_id | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_primary_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | `F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id")))` |
| user_id | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_secondary_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | `F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id")))` |

### 2. Metadata Enrichment Rules

| Business Rule | Implementation | Default Behavior |
|---------------|----------------|------------------|
| Active tiles only | Filter `is_active = true` in metadata join | Include all tiles regardless of active status |
| Missing metadata | Left join to preserve all tile data | Default to "UNKNOWN" category and name |
| Multiple categories per tile | Not supported - one category per tile | Use most recent category |
| Historical consistency | No backfill required | New column populated from enhancement date forward |

### 3. Join Strategy

**Join Type:** LEFT JOIN
**Join Key:** tile_id
**Rationale:** Preserve all tile metrics even if metadata is missing

```sql
SELECT 
    ds.date,
    ds.tile_id,
    COALESCE(tm.tile_category, 'UNKNOWN') as tile_category,
    COALESCE(tm.tile_name, 'UNKNOWN') as tile_name,
    ds.unique_tile_views,
    ds.unique_tile_clicks,
    ds.unique_interstitial_views,
    ds.unique_interstitial_primary_clicks,
    ds.unique_interstitial_secondary_clicks
FROM daily_summary ds
LEFT JOIN tile_metadata tm ON ds.tile_id = tm.tile_id
```

## Assumptions and Constraints

### Assumptions
1. **Metadata Availability:** SOURCE_TILE_METADATA will be populated before ETL execution
2. **Data Quality:** tile_id values in metadata table match those in event tables
3. **Update Frequency:** Metadata updates are infrequent and don't require historical backfill
4. **Performance Impact:** Additional join operation will have minimal performance impact
5. **Business Logic:** Default "UNKNOWN" category is acceptable for missing metadata

### Technical Constraints
1. **Schema Evolution:** Target table schema change requires coordinated deployment
2. **Backward Compatibility:** Existing dashboards must continue to function
3. **Data Lineage:** New table must be included in data governance documentation
4. **Testing Requirements:** Comprehensive unit tests required for join logic
5. **Monitoring:** Additional data quality checks needed for metadata completeness

### Business Constraints
1. **No Historical Backfill:** Enhancement applies to new data only
2. **Global KPIs Unchanged:** No modifications to TARGET_HOME_TILE_GLOBAL_KPIS table
3. **Dashboard Updates:** BI team responsible for incorporating new fields
4. **Metadata Maintenance:** Product team owns metadata table content

### Performance Considerations
1. **Join Performance:** LEFT JOIN on tile_id should be efficient with proper indexing
2. **Memory Usage:** Additional columns increase memory footprint minimally
3. **Storage Impact:** New metadata table is small (< 1000 records expected)
4. **Query Performance:** Category-based filtering will improve dashboard performance

## References

### JIRA Stories
- **SCRUM-7819:** Add New Source Table & Extend Target Reporting Metrics

### Technical Documentation
- **Source DDL:** `Input/SourceDDL.sql`
- **Target DDL:** `Input/TargetDDL.sql`
- **Metadata DDL:** `Input/SOURCE_TILE_METADATA.sql`
- **ETL Pipeline:** `Input/home_tile_reporting_etl.py`

### Data Governance
- **Data Catalog:** Update required for new SOURCE_TILE_METADATA table
- **Data Dictionary:** Update required for new target table columns
- **Lineage Documentation:** Update required for enhanced data flow

### Testing Requirements
- **Unit Tests:** Metadata join logic validation
- **Integration Tests:** End-to-end pipeline execution
- **Data Quality Tests:** Record count validation and schema compliance
- **Regression Tests:** Existing functionality preservation

---

**Document Status:** Draft  
**Review Required:** Data Architecture Team, BI Team, Product Analytics Team  
**Implementation Timeline:** To be determined based on sprint planning  
**Risk Level:** Low - Additive enhancement with backward compatibility maintained