=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target reporting metrics with tile category
=============================================

# Technical Specification for Home Tile Reporting Enhancement - Add SOURCE_TILE_METADATA Integration

## Introduction

### Overview
This technical specification outlines the implementation details for enhancing the existing Home Tile Reporting ETL pipeline by integrating a new source table `SOURCE_TILE_METADATA` and extending the target reporting metrics with tile category information. This enhancement addresses the business requirement to enable category-level performance tracking and reporting.

### Business Context
Product Analytics has requested the addition of tile-level metadata to enrich reporting dashboards. Currently, all tile metrics are aggregated only at the tile_id level without business grouping. The enhancement will enable:
- Performance metrics by category (e.g., "Personal Finance Tiles vs Health Tiles")
- Category-level CTR comparisons
- Product manager tracking of feature adoption
- Improved dashboard drilldowns in Power BI/Tableau
- Future segmentation capabilities

### Scope
- Add new Delta source table `analytics_db.SOURCE_TILE_METADATA`
- Modify existing ETL pipeline to join metadata
- Update target table schema to include `tile_category`
- Ensure backward compatibility
- Maintain data quality and integrity

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
df_metadata = spark.table(SOURCE_TILE_METADATA).filter(F.col("is_active") == True)
```

#### 1.3 Daily Summary Aggregation Enhancement
```python
# Modified daily summary with metadata join
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

#### 1.4 Updated Write Operation
```python
# Replace existing write operation
overwrite_partition(df_daily_summary_enriched, TARGET_DAILY_SUMMARY)
```

### 2. Schema Validation Enhancement
```python
# Add schema validation function
def validate_schema(df, expected_columns):
    actual_columns = set(df.columns)
    expected_columns = set(expected_columns)
    
    if not expected_columns.issubset(actual_columns):
        missing_cols = expected_columns - actual_columns
        raise ValueError(f"Missing columns: {missing_cols}")
    
    return True

# Expected schema for daily summary
EXPECTED_DAILY_SUMMARY_COLUMNS = [
    "date", "tile_id", "tile_category", "tile_name",
    "unique_tile_views", "unique_tile_clicks", 
    "unique_interstitial_views", "unique_interstitial_primary_clicks", 
    "unique_interstitial_secondary_clicks"
]

# Validate before writing
validate_schema(df_daily_summary_enriched, EXPECTED_DAILY_SUMMARY_COLUMNS)
```

## Data Model Updates

### 1. New Source Table: `analytics_db.SOURCE_TILE_METADATA`

**Purpose**: Master metadata table for homepage tiles containing business categorization and descriptive information.

**Schema**:
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

### 2. Updated Target Table: `reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY`

**Schema Changes**:
```sql
-- Add new columns to existing table
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
ADD COLUMNS (
    tile_category STRING COMMENT 'Functional category of the tile',
    tile_name     STRING COMMENT 'User-friendly tile name'
);
```

**Updated Complete Schema**:
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
(
    date                                DATE       COMMENT 'Reporting date',
    tile_id                             STRING     COMMENT 'Tile identifier',
    tile_category                       STRING     COMMENT 'Functional category of the tile',
    tile_name                           STRING     COMMENT 'User-friendly tile name',
    unique_tile_views                   LONG       COMMENT 'Distinct users who viewed the tile',
    unique_tile_clicks                  LONG       COMMENT 'Distinct users who clicked the tile',
    unique_interstitial_views           LONG       COMMENT 'Distinct users who viewed interstitial',
    unique_interstitial_primary_clicks  LONG       COMMENT 'Distinct users who clicked primary button',
    unique_interstitial_secondary_clicks LONG      COMMENT 'Distinct users who clicked secondary button'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily aggregated tile-level metrics for reporting with business categorization';
```

### 3. Data Relationships

```
SOURCE_HOME_TILE_EVENTS (1) -----> (M) TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_INTERSTITIAL_EVENTS (1) --> (M) TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_TILE_METADATA (1) --------> (M) TARGET_HOME_TILE_DAILY_SUMMARY
                                       ^
                                       |
                                   (LEFT JOIN on tile_id)
```

## Source-to-Target Mapping

### 1. Enhanced Daily Summary Mapping

| Source Table | Source Column | Target Table | Target Column | Transformation Rule |
|--------------|---------------|--------------|---------------|--------------------|
| SOURCE_HOME_TILE_EVENTS | event_ts | TARGET_HOME_TILE_DAILY_SUMMARY | date | `to_date(event_ts)` |
| SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | tile_id | Direct mapping |
| SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | tile_category | `COALESCE(tile_category, 'UNKNOWN')` |
| SOURCE_TILE_METADATA | tile_name | TARGET_HOME_TILE_DAILY_SUMMARY | tile_name | Direct mapping via LEFT JOIN |
| SOURCE_HOME_TILE_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_views | `COUNT(DISTINCT user_id) WHERE event_type = 'TILE_VIEW'` |
| SOURCE_HOME_TILE_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_clicks | `COUNT(DISTINCT user_id) WHERE event_type = 'TILE_CLICK'` |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_views | `COUNT(DISTINCT user_id) WHERE interstitial_view_flag = TRUE` |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_primary_clicks | `COUNT(DISTINCT user_id) WHERE primary_button_click_flag = TRUE` |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_secondary_clicks | `COUNT(DISTINCT user_id) WHERE secondary_button_click_flag = TRUE` |

### 2. Metadata Join Logic

```sql
-- Join Logic
SELECT 
    ds.date,
    ds.tile_id,
    COALESCE(tm.tile_category, 'UNKNOWN') as tile_category,
    tm.tile_name,
    ds.unique_tile_views,
    ds.unique_tile_clicks,
    ds.unique_interstitial_views,
    ds.unique_interstitial_primary_clicks,
    ds.unique_interstitial_secondary_clicks
FROM daily_summary ds
LEFT JOIN SOURCE_TILE_METADATA tm 
    ON ds.tile_id = tm.tile_id 
    AND tm.is_active = TRUE
```

### 3. Data Quality Rules

| Rule Type | Description | Implementation |
|-----------|-------------|----------------|
| Null Handling | Handle missing tile_category | `COALESCE(tile_category, 'UNKNOWN')` |
| Active Filter | Only use active metadata | `WHERE is_active = TRUE` |
| Backward Compatibility | Maintain existing metrics | Preserve all existing aggregation logic |
| Schema Validation | Ensure all expected columns | Pre-write schema validation |

## Assumptions and Constraints

### Assumptions
1. **Data Availability**: SOURCE_TILE_METADATA table will be populated before ETL execution
2. **Active Flag**: Only tiles with `is_active = TRUE` should be considered for metadata enrichment
3. **Backward Compatibility**: Existing reports and dashboards can handle additional columns
4. **Default Category**: Tiles without metadata mapping will be categorized as "UNKNOWN"
5. **Performance**: LEFT JOIN operation will not significantly impact ETL performance

### Constraints
1. **Schema Evolution**: Target table schema changes require coordination with downstream consumers
2. **Data Governance**: New metadata table requires proper data stewardship and maintenance
3. **Testing**: Comprehensive testing required to ensure no regression in existing metrics
4. **Deployment**: Changes must be deployed in coordination with metadata table creation
5. **Monitoring**: Enhanced monitoring needed for metadata table freshness and completeness

### Technical Constraints
1. **Delta Lake Compatibility**: All changes must maintain Delta Lake format compatibility
2. **Partition Strategy**: Maintain existing partitioning strategy for performance
3. **Idempotency**: ETL must remain idempotent for reprocessing scenarios
4. **Resource Usage**: Memory and compute usage should not increase significantly

## Implementation Plan

### Phase 1: Infrastructure Setup
1. Create SOURCE_TILE_METADATA table
2. Populate initial metadata
3. Update target table schema

### Phase 2: ETL Enhancement
1. Modify ETL pipeline code
2. Add schema validation
3. Update error handling

### Phase 3: Testing & Validation
1. Unit testing
2. Integration testing
3. Performance testing
4. Data quality validation

### Phase 4: Deployment
1. Deploy to development environment
2. User acceptance testing
3. Production deployment
4. Post-deployment monitoring

## References

1. **JIRA Story**: SCRUM-7819 - Add New Source Table & Extend Target Reporting Metrics
2. **Source Files**:
   - `Input/home_tile_reporting_etl.py` - Current ETL implementation
   - `Input/SourceDDL.sql` - Source table schemas
   - `Input/TargetDDL.sql` - Target table schemas
   - `Input/SOURCE_TILE_METADATA.sql` - New metadata table schema
3. **Business Requirements**: Product Analytics dashboard enhancement for category-level reporting
4. **Technical Standards**: Delta Lake, PySpark, Databricks platform standards

---

**Document Status**: Draft  
**Last Updated**: 2025-12-04  
**Next Review**: Upon implementation completion