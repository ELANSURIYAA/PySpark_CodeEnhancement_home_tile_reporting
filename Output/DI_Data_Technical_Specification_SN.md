=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for Home Tile Reporting ETL enhancement with new SOURCE_TILE_METADATA integration
=============================================

# Technical Specification for Home Tile Reporting Enhancement

## Introduction

### Overview
This technical specification outlines the enhancement of the existing Home Tile Reporting ETL pipeline to incorporate the new `SOURCE_TILE_METADATA` table. The enhancement will enrich the reporting capabilities by adding tile metadata information to provide better business context and categorization for analytics and reporting.

### Business Requirements
- Integrate tile metadata (tile_name, tile_category, is_active) into existing reporting tables
- Enhance daily summary reports with business-friendly tile names and categories
- Filter out inactive tiles from reporting to ensure data accuracy
- Maintain backward compatibility with existing data pipeline
- Support idempotent processing for daily partition overwrite

### Scope
- Modification of existing ETL pipeline (`home_tile_reporting_etl.py`)
- Updates to target data models to include metadata fields
- Implementation of source-to-target mapping with transformation rules
- Ensure data quality and consistency across all reporting tables

## Code Changes

### 1. Configuration Updates

**File:** `home_tile_reporting_etl.py`

**Changes Required:**
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

### 2. Source Data Reading Enhancement

**Current Implementation:**
```python
df_tile = (
    spark.table(SOURCE_HOME_TILE_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
)
```

**Enhanced Implementation:**
```python
# Read tile metadata
df_tile_metadata = spark.table(SOURCE_TILE_METADATA).filter(F.col("is_active") == True)

# Enhanced tile events with metadata join
df_tile = (
    spark.table(SOURCE_HOME_TILE_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
    .join(df_tile_metadata, "tile_id", "inner")  # Only active tiles
)

# Enhanced interstitial events with metadata join
df_inter = (
    spark.table(SOURCE_INTERSTITIAL_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
    .join(df_tile_metadata, "tile_id", "inner")  # Only active tiles
)
```

### 3. Aggregation Logic Enhancement

**Enhanced Daily Summary Aggregation:**
```python
df_tile_agg = (
    df_tile.groupBy("tile_id", "tile_name", "tile_category")
    .agg(
        F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
        F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
    )
)

df_inter_agg = (
    df_inter.groupBy("tile_id", "tile_name", "tile_category")
    .agg(
        F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
        F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
        F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
    )
)
```

### 4. Enhanced Daily Summary Output

**Updated Final Selection:**
```python
df_daily_summary = (
    df_tile_agg.join(df_inter_agg, ["tile_id", "tile_name", "tile_category"], "outer")
    .withColumn("date", F.lit(PROCESS_DATE))
    .select(
        "date",
        "tile_id",
        "tile_name",
        "tile_category",
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
    )
)
```

## Data Model Updates

### 1. Source Data Model Changes

**New Source Table Added:**
- `analytics_db.SOURCE_TILE_METADATA`
  - Contains master data for tile information
  - Provides business context and categorization
  - Includes active/inactive flag for data filtering

### 2. Target Data Model Updates

**Enhanced TARGET_HOME_TILE_DAILY_SUMMARY:**
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
(
    date                                DATE       COMMENT 'Reporting date',
    tile_id                             STRING     COMMENT 'Tile identifier',
    tile_name                           STRING     COMMENT 'User-friendly tile name',
    tile_category                       STRING     COMMENT 'Business category of tile',
    unique_tile_views                   LONG       COMMENT 'Distinct users who viewed the tile',
    unique_tile_clicks                  LONG       COMMENT 'Distinct users who clicked the tile',
    unique_interstitial_views           LONG       COMMENT 'Distinct users who viewed interstitial',
    unique_interstitial_primary_clicks  LONG       COMMENT 'Distinct users who clicked primary button',
    unique_interstitial_secondary_clicks LONG      COMMENT 'Distinct users who clicked secondary button'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily aggregated tile-level metrics with metadata for reporting';
```

**TARGET_HOME_TILE_GLOBAL_KPIS remains unchanged** as it contains aggregated metrics without tile-level details.

### 3. Data Relationships

```
SOURCE_TILE_METADATA (1) -----> (M) SOURCE_HOME_TILE_EVENTS
SOURCE_TILE_METADATA (1) -----> (M) SOURCE_INTERSTITIAL_EVENTS
```

## Source-to-Target Mapping

### 1. SOURCE_TILE_METADATA to TARGET_HOME_TILE_DAILY_SUMMARY

| Source Field | Target Field | Transformation Rule | Data Type | Business Rule |
|--------------|--------------|-------------------|-----------|---------------|
| tile_id | tile_id | Direct mapping | STRING | Primary key for joining |
| tile_name | tile_name | Direct mapping | STRING | Business-friendly name |
| tile_category | tile_category | Direct mapping | STRING | Business categorization |
| is_active | N/A | Filter condition | BOOLEAN | Only active tiles (is_active = true) |
| updated_ts | N/A | Not mapped | TIMESTAMP | Metadata only |

### 2. Enhanced Event Data Mapping

| Source Table | Source Field | Target Field | Transformation Rule |
|--------------|--------------|--------------|-------------------|
| SOURCE_HOME_TILE_EVENTS | event_ts | date | DATE(event_ts) = PROCESS_DATE |
| SOURCE_HOME_TILE_EVENTS | user_id | unique_tile_views | COUNT(DISTINCT user_id) WHERE event_type = 'TILE_VIEW' |
| SOURCE_HOME_TILE_EVENTS | user_id | unique_tile_clicks | COUNT(DISTINCT user_id) WHERE event_type = 'TILE_CLICK' |
| SOURCE_INTERSTITIAL_EVENTS | user_id | unique_interstitial_views | COUNT(DISTINCT user_id) WHERE interstitial_view_flag = true |
| SOURCE_INTERSTITIAL_EVENTS | user_id | unique_interstitial_primary_clicks | COUNT(DISTINCT user_id) WHERE primary_button_click_flag = true |
| SOURCE_INTERSTITIAL_EVENTS | user_id | unique_interstitial_secondary_clicks | COUNT(DISTINCT user_id) WHERE secondary_button_click_flag = true |

### 3. Transformation Rules

#### Data Quality Rules
1. **Active Tiles Only**: Filter `SOURCE_TILE_METADATA.is_active = true`
2. **Date Filtering**: Process only events where `DATE(event_ts) = PROCESS_DATE`
3. **Inner Join Strategy**: Use INNER JOIN to ensure only tiles with metadata are processed
4. **Null Handling**: Use COALESCE for metric fields to default to 0

#### Business Logic Rules
1. **Unique User Counting**: Use COUNT(DISTINCT user_id) for all metrics
2. **Event Type Filtering**: Apply conditional aggregation based on event_type and flags
3. **Idempotent Processing**: Overwrite daily partitions to support reprocessing

## Assumptions and Constraints

### Assumptions
1. `SOURCE_TILE_METADATA` table is populated and maintained by upstream processes
2. All tiles in event tables have corresponding metadata records
3. The `is_active` flag accurately reflects current tile status
4. Tile metadata changes are not frequent and don't require historical tracking
5. Daily processing window allows for complete data availability

### Constraints
1. **Performance**: Additional JOIN operations may impact processing time
2. **Data Dependency**: ETL pipeline now depends on tile metadata availability
3. **Storage**: Additional fields in target table will increase storage requirements
4. **Backward Compatibility**: Existing downstream consumers need to handle new fields

### Technical Constraints
1. **Memory Usage**: Metadata table should be small enough to broadcast if needed
2. **Processing Window**: Ensure metadata is available before event processing
3. **Data Freshness**: Metadata updates should be reflected in the same processing cycle

## References

### Source Files
1. `Input/home_tile_reporting_etl.py` - Current ETL implementation
2. `Input/SourceDDL.sql` - Source table definitions
3. `Input/TargetDDL.sql` - Target table definitions
4. `Input/SOURCE_TILE_METADATA.sql` - New metadata table definition

### Related Documentation
- Data Architecture Guidelines
- ETL Development Standards
- Data Quality Framework
- Production Deployment Procedures

### Stakeholders
- Data Engineering Team
- Analytics Team
- Business Intelligence Team
- Data Governance Team

---

**Document Status:** Draft
**Review Required:** Yes
**Implementation Priority:** High
**Estimated Effort:** 3-5 days