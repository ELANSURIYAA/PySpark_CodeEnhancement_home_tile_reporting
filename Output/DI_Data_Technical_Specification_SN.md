=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target reporting metrics with tile category
=============================================

# Technical Specification for Home Tile Reporting Enhancement - Tile Category Integration

## Introduction

### Overview
This technical specification outlines the implementation of JIRA story SCRUM-7567, which requires adding a new source table `SOURCE_TILE_METADATA` and extending the existing home tile reporting pipeline to include tile category information in the daily summary reports.

### Business Objective
Product Analytics has requested the addition of tile-level metadata to enrich reporting dashboards. Currently, all tile metrics are aggregated only at tile_id level with no business grouping. The enhancement will enable:
- Performance tracking at functional level (Offers, Health Checks, Payments)
- Category-level CTR comparisons
- Product manager tracking of feature adoption
- Improved dashboard drilldowns in Power BI/Tableau
- Future segmentation capabilities

### Scope
- Add new Delta source table `analytics_db.SOURCE_TILE_METADATA`
- Modify existing ETL pipeline to join metadata
- Update target table schema to include `tile_category`
- Ensure backward compatibility

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
    df_daily_summary.join(df_metadata, "tile_id", "left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
    .select(
        "date",
        "tile_id",
        "tile_category",  # New column
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
# Update the overwrite partition call
overwrite_partition(df_daily_summary_enriched, TARGET_DAILY_SUMMARY)
```

### 2. Error Handling and Data Quality
```python
# Add data quality checks
def validate_metadata_join(df):
    """
    Validate that metadata join doesn't cause data loss
    """
    original_count = df_daily_summary.count()
    enriched_count = df.count()
    
    if original_count != enriched_count:
        raise Exception(f"Data loss detected: Original={original_count}, Enriched={enriched_count}")
    
    return df

# Apply validation
df_daily_summary_enriched = validate_metadata_join(df_daily_summary_enriched)
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
- Primary Key: `tile_id`
- Lookup table for tile metadata
- Contains business categorization information
- Includes active/inactive flag for filtering

### 2. Updated Target Table: `TARGET_HOME_TILE_DAILY_SUMMARY`

**Schema Changes:**
```sql
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
ADD COLUMN tile_category STRING COMMENT 'Functional category of the tile';
```

**Updated Table Structure:**
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
(
    date                                DATE       COMMENT 'Reporting date',
    tile_id                             STRING     COMMENT 'Tile identifier',
    tile_category                       STRING     COMMENT 'Functional category of the tile', -- NEW COLUMN
    unique_tile_views                   LONG       COMMENT 'Distinct users who viewed the tile',
    unique_tile_clicks                  LONG       COMMENT 'Distinct users who clicked the tile',
    unique_interstitial_views           LONG       COMMENT 'Distinct users who viewed interstitial',
    unique_interstitial_primary_clicks  LONG       COMMENT 'Distinct users who clicked primary button',
    unique_interstitial_secondary_clicks LONG      COMMENT 'Distinct users who clicked secondary button'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily aggregated tile-level metrics for reporting with category enrichment';
```

### 3. Data Relationships

```
SOURCE_HOME_TILE_EVENTS ──┐
                          ├─► TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_INTERSTITIAL_EVENTS ┘     ▲
                                  │
SOURCE_TILE_METADATA ─────────────┘ (LEFT JOIN on tile_id)
```

## Source-to-Target Mapping

### 1. Enhanced Daily Summary Mapping

| Source Field | Source Table | Target Field | Target Table | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------------|
| event_ts | SOURCE_HOME_TILE_EVENTS | date | TARGET_HOME_TILE_DAILY_SUMMARY | `to_date(event_ts)` |
| tile_id | SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | Direct mapping |
| tile_category | SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | `COALESCE(tile_category, 'UNKNOWN')` |
| user_id + event_type='TILE_VIEW' | SOURCE_HOME_TILE_EVENTS | unique_tile_views | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT user_id WHERE event_type='TILE_VIEW')` |
| user_id + event_type='TILE_CLICK' | SOURCE_HOME_TILE_EVENTS | unique_tile_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT user_id WHERE event_type='TILE_CLICK')` |
| user_id + interstitial_view_flag=true | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_views | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT user_id WHERE interstitial_view_flag=true)` |
| user_id + primary_button_click_flag=true | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_primary_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT user_id WHERE primary_button_click_flag=true)` |
| user_id + secondary_button_click_flag=true | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_secondary_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT user_id WHERE secondary_button_click_flag=true)` |

### 2. Metadata Enrichment Rules

| Condition | tile_category Value | Business Rule |
|-----------|-------------------|---------------|
| tile_id exists in SOURCE_TILE_METADATA AND is_active=true | Use actual tile_category | Normal enrichment |
| tile_id exists in SOURCE_TILE_METADATA AND is_active=false | 'INACTIVE' | Inactive tile handling |
| tile_id does not exist in SOURCE_TILE_METADATA | 'UNKNOWN' | Default for missing metadata |

### 3. Join Logic
```sql
LEFT JOIN analytics_db.SOURCE_TILE_METADATA m 
  ON t.tile_id = m.tile_id 
  AND m.is_active = true
```

## Assumptions and Constraints

### Assumptions
1. **Data Availability**: SOURCE_TILE_METADATA will be populated before ETL execution
2. **Data Quality**: tile_id values in metadata table match those in event tables
3. **Performance**: Metadata table size is manageable for broadcast join (< 10MB)
4. **Backward Compatibility**: Existing reports will continue to function with new schema
5. **Default Handling**: 'UNKNOWN' category is acceptable for business reporting

### Constraints
1. **Schema Evolution**: Target table schema change requires coordination with downstream consumers
2. **Data Governance**: New column must follow existing naming conventions
3. **Performance Impact**: Additional join may impact ETL runtime (estimated <5% increase)
4. **Storage**: Additional string column will increase storage requirements minimally
5. **Testing**: Comprehensive testing required to ensure no data loss during join operations

### Technical Constraints
1. **Delta Lake**: All tables must use Delta format for ACID compliance
2. **Partitioning**: Maintain existing partition strategy on date column
3. **Idempotency**: ETL must remain idempotent for reprocessing scenarios
4. **Error Handling**: Pipeline must gracefully handle missing metadata

## Implementation Phases

### Phase 1: Infrastructure Setup
1. Create SOURCE_TILE_METADATA table
2. Populate initial metadata
3. Update target table schema

### Phase 2: ETL Enhancement
1. Modify PySpark ETL code
2. Add data quality validations
3. Update error handling

### Phase 3: Testing & Validation
1. Unit testing
2. Integration testing
3. Performance validation
4. Data quality verification

### Phase 4: Deployment
1. Deploy to development environment
2. User acceptance testing
3. Production deployment
4. Monitor and validate

## References

1. **JIRA Story**: SCRUM-7567 - "Add New Source Table & Extend Target Reporting Metrics"
2. **Source Files**:
   - `Input/home_tile_reporting_etl.py` - Current ETL implementation
   - `Input/SourceDDL.sql` - Existing source table schemas
   - `Input/TargetDDL.sql` - Existing target table schemas
   - `Input/SOURCE_TILE_METADATA.sql` - New metadata table schema
3. **Business Requirements**: Product Analytics dashboard enhancement for category-level reporting
4. **Technical Standards**: Delta Lake, PySpark, Databricks platform standards

---

**Document Version**: 1.0  
**Last Updated**: 2025-12-03  
**Review Status**: Draft  
**Approvers**: Data Engineering Team, Product Analytics Team