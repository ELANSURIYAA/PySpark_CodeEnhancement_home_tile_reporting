=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target reporting with tile category
=============================================

# Technical Specification for Home Tile Reporting Enhancement - Add Tile Metadata

## Introduction

### Business Context
This enhancement addresses the business requirement to enrich home tile reporting with metadata categorization. The Product Analytics team has requested the addition of tile-level metadata to enable better reporting and performance tracking at functional levels (e.g., Offers, Health Checks, Payments).

### Objective
Integrate the new `SOURCE_TILE_METADATA` table into the existing ETL pipeline to provide tile category information in the daily summary reports, enabling category-level performance analysis and improved dashboard capabilities.

### Scope
- Add new Delta source table `analytics_db.SOURCE_TILE_METADATA`
- Modify existing ETL pipeline to incorporate metadata
- Update target table schema to include `tile_category`
- Ensure backward compatibility and maintain data integrity

## Code Changes

### 1. ETL Pipeline Modifications (home_tile_reporting_etl.py)

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
        df_metadata.select("tile_id", "tile_category"), 
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

### 2. Complete Modified ETL Code Structure

#### Key Changes Summary:
- **Line 25**: Add SOURCE_TILE_METADATA configuration
- **Line 45**: Add metadata table reading logic
- **Line 75**: Modify daily summary to include left join with metadata
- **Line 95**: Update select statement to include tile_category
- **Line 125**: Ensure tile_category is handled in write operations

## Data Model Updates

### 1. New Source Table: SOURCE_TILE_METADATA

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

### 2. Updated Target Table: TARGET_HOME_TILE_DAILY_SUMMARY

**Schema Changes:**
```sql
-- Add new column to existing table
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
ADD COLUMN tile_category STRING COMMENT 'Functional category of the tile';
```

**Updated Complete Schema:**
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
(
    date                                DATE       COMMENT 'Reporting date',
    tile_id                             STRING     COMMENT 'Tile identifier',
    tile_category                       STRING     COMMENT 'Functional category of the tile',
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
                          ├── ETL Pipeline ──> TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_INTERSTITIAL_EVENTS ─┤                           ↑
                          │                           │
SOURCE_TILE_METADATA ─────┘                    (enriched with tile_category)
```

## Source-to-Target Mapping

### 1. Enhanced Daily Summary Mapping

| Source Field | Source Table | Target Field | Target Table | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------------|
| event_ts | SOURCE_HOME_TILE_EVENTS | date | TARGET_HOME_TILE_DAILY_SUMMARY | `to_date(event_ts)` filtered by PROCESS_DATE |
| tile_id | SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | Direct mapping |
| tile_category | SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | `LEFT JOIN` on tile_id, `COALESCE` with "UNKNOWN" |
| user_id + event_type | SOURCE_HOME_TILE_EVENTS | unique_tile_views | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT user_id WHERE event_type = 'TILE_VIEW')` |
| user_id + event_type | SOURCE_HOME_TILE_EVENTS | unique_tile_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT user_id WHERE event_type = 'TILE_CLICK')` |
| user_id + interstitial_view_flag | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_views | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT user_id WHERE interstitial_view_flag = true)` |
| user_id + primary_button_click_flag | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_primary_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT user_id WHERE primary_button_click_flag = true)` |
| user_id + secondary_button_click_flag | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_secondary_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT user_id WHERE secondary_button_click_flag = true)` |

### 2. Metadata Enrichment Logic

| Condition | Transformation Rule | Result |
|-----------|-------------------|--------|
| tile_id exists in SOURCE_TILE_METADATA AND is_active = true | Use tile_category from metadata | Actual category value |
| tile_id does not exist in SOURCE_TILE_METADATA OR is_active = false | Apply default value | "UNKNOWN" |

### 3. Join Strategy

```sql
-- Pseudocode for the join logic
SELECT 
    daily_summary.*,
    COALESCE(metadata.tile_category, 'UNKNOWN') as tile_category
FROM daily_summary_base daily_summary
LEFT JOIN (
    SELECT tile_id, tile_category 
    FROM SOURCE_TILE_METADATA 
    WHERE is_active = true
) metadata ON daily_summary.tile_id = metadata.tile_id
```

## Assumptions and Constraints

### Assumptions
1. **Data Availability**: SOURCE_TILE_METADATA will be populated before ETL execution
2. **Data Quality**: tile_id in metadata table matches tile_id in event tables
3. **Performance**: LEFT JOIN operation will not significantly impact ETL performance
4. **Backward Compatibility**: Existing reports will continue to function with new schema
5. **Default Handling**: "UNKNOWN" category is acceptable for tiles without metadata

### Constraints
1. **Schema Evolution**: Target table schema change requires coordinated deployment
2. **Data Consistency**: Metadata updates should be synchronized with ETL runs
3. **Performance Impact**: Additional join operation may increase processing time
4. **Storage**: New column will increase storage requirements
5. **Dependency**: ETL pipeline now depends on metadata table availability

### Technical Constraints
1. **Delta Lake Compatibility**: All operations must maintain Delta Lake ACID properties
2. **Partition Strategy**: Existing date partitioning strategy remains unchanged
3. **Idempotency**: Enhanced ETL must maintain idempotent behavior
4. **Error Handling**: Pipeline should handle metadata table unavailability gracefully

## Implementation Plan

### Phase 1: Infrastructure Setup
1. Create SOURCE_TILE_METADATA table in analytics_db
2. Populate initial metadata entries
3. Update target table schema with new column

### Phase 2: ETL Enhancement
1. Modify ETL pipeline code
2. Update configuration parameters
3. Implement join logic and default handling

### Phase 3: Testing and Validation
1. Unit testing for new functionality
2. Integration testing with sample data
3. Performance testing for join operations
4. Backward compatibility validation

### Phase 4: Deployment
1. Deploy schema changes
2. Deploy updated ETL code
3. Execute initial run with validation
4. Monitor performance and data quality

## References

### JIRA Story
- **Story ID**: SCRUM-7567
- **Title**: Add New Source Table & Extend Target Reporting Metrics
- **Status**: To Do
- **Reporter**: AAVA

### Source Files
- `Input/home_tile_reporting_etl.py` - Current ETL implementation
- `Input/SourceDDL.sql` - Source table schemas
- `Input/TargetDDL.sql` - Target table schemas  
- `Input/SOURCE_TILE_METADATA.sql` - New metadata table schema

### Technical Dependencies
- Apache Spark / PySpark
- Delta Lake
- Databricks Runtime
- Hive Metastore

---

**Document Version**: 1.0  
**Last Updated**: 2025-12-03  
**Next Review Date**: TBD based on implementation timeline