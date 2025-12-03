=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for Home Tile Reporting ETL enhancement with SOURCE_TILE_METADATA integration
=============================================

# Technical Specification for Home Tile Reporting Enhancement

## Introduction

### Purpose
This technical specification outlines the enhancement to the existing Home Tile Reporting ETL pipeline to integrate the new `SOURCE_TILE_METADATA` table. The enhancement will enrich the reporting capabilities by adding tile metadata information to the existing aggregated metrics.

### Scope
The enhancement includes:
- Integration of `SOURCE_TILE_METADATA` table into the existing ETL pipeline
- Updates to data models to include tile metadata fields
- Enhanced source-to-target mapping with metadata enrichment
- Code modifications to support the new data source

### Business Requirements
- Enrich home tile reporting with business categorization
- Provide tile name and category information in daily summaries
- Enable filtering and grouping by tile categories in reports
- Maintain backward compatibility with existing reporting structure

## Code Changes

### 1. Configuration Updates

**File:** `home_tile_reporting_etl.py`

**Changes Required:**
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

### 2. Data Reading Logic Enhancement

**Location:** Read Source Tables section

**Current Code:**
```python
df_tile = (
    spark.table(SOURCE_HOME_TILE_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
)
```

**Enhanced Code:**
```python
# Read tile metadata
df_metadata = spark.table(SOURCE_TILE_METADATA).filter(F.col("is_active") == True)

# Enhanced tile events with metadata join
df_tile = (
    spark.table(SOURCE_HOME_TILE_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
    .join(df_metadata, "tile_id", "left")
)

# Enhanced interstitial events with metadata join
df_inter = (
    spark.table(SOURCE_INTERSTITIAL_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
    .join(df_metadata, "tile_id", "left")
)
```

### 3. Aggregation Logic Updates

**Location:** Daily Tile Summary Aggregation section

**Enhanced Aggregation:**
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

### 4. Final Selection Enhancement

**Enhanced Daily Summary:**
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

**New Source Table:** `analytics_db.SOURCE_TILE_METADATA`
- **Purpose:** Master data for tile metadata and business categorization
- **Key Fields:** tile_id, tile_name, tile_category, is_active, updated_ts
- **Relationship:** One-to-many with SOURCE_HOME_TILE_EVENTS and SOURCE_INTERSTITIAL_EVENTS

### 2. Target Data Model Updates

**Enhanced Target Table:** `reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY`

**Required DDL Changes:**
```sql
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
ADD COLUMNS (
    tile_name STRING COMMENT 'User-friendly tile name from metadata',
    tile_category STRING COMMENT 'Business category of the tile'
);
```

**Updated Target Schema:**
- date (DATE) - Reporting date
- tile_id (STRING) - Tile identifier
- **tile_name (STRING)** - *New field from metadata*
- **tile_category (STRING)** - *New field from metadata*
- unique_tile_views (LONG) - Distinct users who viewed the tile
- unique_tile_clicks (LONG) - Distinct users who clicked the tile
- unique_interstitial_views (LONG) - Distinct users who viewed interstitial
- unique_interstitial_primary_clicks (LONG) - Distinct users who clicked primary button
- unique_interstitial_secondary_clicks (LONG) - Distinct users who clicked secondary button

### 3. Data Relationships

```
SOURCE_TILE_METADATA (1) ----< SOURCE_HOME_TILE_EVENTS (M)
SOURCE_TILE_METADATA (1) ----< SOURCE_INTERSTITIAL_EVENTS (M)
```

## Source-to-Target Mapping

### Enhanced Mapping Table

| Source Table | Source Field | Target Table | Target Field | Transformation Rule | Data Type | Comments |
|--------------|--------------|--------------|--------------|-------------------|-----------|----------|
| SOURCE_HOME_TILE_EVENTS | event_ts | TARGET_HOME_TILE_DAILY_SUMMARY | date | DATE(event_ts) | DATE | Extract date from timestamp |
| SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | tile_id | Direct mapping | STRING | Primary key for aggregation |
| **SOURCE_TILE_METADATA** | **tile_name** | **TARGET_HOME_TILE_DAILY_SUMMARY** | **tile_name** | **Direct mapping via LEFT JOIN** | **STRING** | **New enrichment field** |
| **SOURCE_TILE_METADATA** | **tile_category** | **TARGET_HOME_TILE_DAILY_SUMMARY** | **tile_category** | **Direct mapping via LEFT JOIN** | **STRING** | **New enrichment field** |
| SOURCE_HOME_TILE_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_views | COUNT(DISTINCT user_id) WHERE event_type='TILE_VIEW' | LONG | Aggregated metric |
| SOURCE_HOME_TILE_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_clicks | COUNT(DISTINCT user_id) WHERE event_type='TILE_CLICK' | LONG | Aggregated metric |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_views | COUNT(DISTINCT user_id) WHERE interstitial_view_flag=true | LONG | Aggregated metric |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_primary_clicks | COUNT(DISTINCT user_id) WHERE primary_button_click_flag=true | LONG | Aggregated metric |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_secondary_clicks | COUNT(DISTINCT user_id) WHERE secondary_button_click_flag=true | LONG | Aggregated metric |

### Transformation Rules

#### 1. Metadata Enrichment Rule
```sql
LEFT JOIN SOURCE_TILE_METADATA m ON events.tile_id = m.tile_id AND m.is_active = true
```

#### 2. Date Partitioning Rule
```sql
WHERE DATE(event_ts) = '${PROCESS_DATE}'
```

#### 3. Aggregation Rules
- **Unique Views:** `COUNT(DISTINCT CASE WHEN event_type = 'TILE_VIEW' THEN user_id END)`
- **Unique Clicks:** `COUNT(DISTINCT CASE WHEN event_type = 'TILE_CLICK' THEN user_id END)`
- **Interstitial Metrics:** `COUNT(DISTINCT CASE WHEN flag = true THEN user_id END)`

#### 4. Null Handling Rule
```sql
COALESCE(aggregated_value, 0) -- Replace NULL with 0 for all metrics
```

### Global KPIs Enhancement

**Additional Category-Level KPIs:**
```python
df_category_kpis = (
    df_daily_summary.groupBy("date", "tile_category")
    .agg(
        F.sum("unique_tile_views").alias("category_tile_views"),
        F.sum("unique_tile_clicks").alias("category_tile_clicks"),
        F.sum("unique_interstitial_views").alias("category_interstitial_views")
    )
    .withColumn(
        "category_ctr",
        F.when(F.col("category_tile_views") > 0,
               F.col("category_tile_clicks") / F.col("category_tile_views")).otherwise(0.0)
    )
)
```

## Assumptions and Constraints

### Assumptions
1. **Data Quality:** SOURCE_TILE_METADATA contains valid and up-to-date tile information
2. **Referential Integrity:** All tile_ids in event tables exist in metadata table
3. **Performance:** LEFT JOIN with metadata table will not significantly impact ETL performance
4. **Business Logic:** Only active tiles (is_active = true) should be considered for reporting
5. **Backward Compatibility:** Existing reports and dashboards can handle additional fields

### Constraints
1. **Data Availability:** Metadata table must be populated before ETL execution
2. **Schema Evolution:** Target table schema changes require coordination with downstream consumers
3. **Processing Window:** Enhanced ETL should complete within existing SLA windows
4. **Storage Impact:** Additional fields will increase storage requirements for target tables
5. **Memory Usage:** JOIN operations may require additional Spark cluster resources

### Technical Constraints
1. **Spark Configuration:** May need to adjust spark.sql.adaptive.coalescePartitions.enabled for optimal performance
2. **Delta Lake:** Ensure Delta Lake version supports schema evolution for target tables
3. **Partitioning:** Maintain existing date-based partitioning strategy
4. **Idempotency:** Enhanced pipeline must maintain idempotent behavior for reprocessing

## References

### Source Files
1. **ETL Pipeline:** `Input/home_tile_reporting_etl.py`
2. **Source DDL:** `Input/SourceDDL.sql`
3. **Target DDL:** `Input/TargetDDL.sql`
4. **Metadata DDL:** `Input/SOURCE_TILE_METADATA.sql`

### Technical Dependencies
1. **Apache Spark:** Version 3.x with Delta Lake support
2. **PySpark:** Compatible version with Spark cluster
3. **Delta Lake:** For ACID transactions and schema evolution
4. **Databricks Runtime:** Recommended for production deployment

### Business Dependencies
1. **Metadata Management:** Process for maintaining tile metadata
2. **Data Governance:** Approval for schema changes to target tables
3. **Reporting Tools:** Updates to downstream BI tools and dashboards
4. **Documentation:** User guides for new reporting capabilities

---

**Document Version:** 1.0  
**Last Updated:** 2025-12-02  
**Next Review Date:** 2025-12-16  
**Approval Status:** Pending Review