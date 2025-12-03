=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for integrating SOURCE_TILE_METADATA into Home Tile Reporting ETL pipeline
=============================================

# Technical Specification for Home Tile Reporting Enhancement

## Introduction

This technical specification outlines the enhancement of the existing Home Tile Reporting ETL pipeline to incorporate the new `SOURCE_TILE_METADATA` table. The enhancement will enrich the reporting capabilities by adding tile metadata information such as tile names, categories, and active status to the existing aggregated metrics.

### Business Objective
The enhancement aims to provide business users with enriched reporting capabilities by including tile metadata in the daily summary reports, enabling better categorization and analysis of tile performance metrics.

### Scope
- Integration of `SOURCE_TILE_METADATA` table into the existing ETL pipeline
- Enhancement of target tables to include metadata fields
- Updates to aggregation logic to incorporate metadata enrichment
- Maintenance of existing functionality and performance

## Code Changes

### 1. Configuration Updates

**File:** `home_tile_reporting_etl.py`

**Changes Required:**
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

### 2. Source Data Reading Enhancement

**Current Code Section:**
```python
# READ SOURCE TABLES
df_tile = (
    spark.table(SOURCE_HOME_TILE_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
)

df_inter = (
    spark.table(SOURCE_INTERSTITIAL_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
)
```

**Enhanced Code:**
```python
# READ SOURCE TABLES
df_tile = (
    spark.table(SOURCE_HOME_TILE_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
)

df_inter = (
    spark.table(SOURCE_INTERSTITIAL_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
)

# READ TILE METADATA
df_metadata = (
    spark.table(SOURCE_TILE_METADATA)
    .filter(F.col("is_active") == True)
    .select("tile_id", "tile_name", "tile_category")
)
```

### 3. Daily Summary Aggregation Enhancement

**Current Code Section:**
```python
df_daily_summary = (
    df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    .withColumn("date", F.lit(PROCESS_DATE))
    .select(
        "date",
        "tile_id",
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
    )
)
```

**Enhanced Code:**
```python
df_daily_summary = (
    df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    .join(df_metadata, "tile_id", "left")  # Left join to include all tiles
    .withColumn("date", F.lit(PROCESS_DATE))
    .select(
        "date",
        "tile_id",
        F.coalesce("tile_name", F.lit("Unknown")).alias("tile_name"),
        F.coalesce("tile_category", F.lit("Uncategorized")).alias("tile_category"),
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
    )
)
```

### 4. Global KPIs Enhancement

**Enhanced Code for Category-wise KPIs:**
```python
# Category-wise Global KPIs
df_category_kpis = (
    df_daily_summary.groupBy("date", "tile_category")
    .agg(
        F.sum("unique_tile_views").alias("category_tile_views"),
        F.sum("unique_tile_clicks").alias("category_tile_clicks"),
        F.sum("unique_interstitial_views").alias("category_interstitial_views"),
        F.sum("unique_interstitial_primary_clicks").alias("category_primary_clicks"),
        F.sum("unique_interstitial_secondary_clicks").alias("category_secondary_clicks")
    )
    .withColumn(
        "category_ctr",
        F.when(F.col("category_tile_views") > 0,
               F.col("category_tile_clicks") / F.col("category_tile_views")).otherwise(0.0)
    )
)
```

## Data Model Updates

### 1. Source Data Model Changes

**New Source Table Added:**
- `analytics_db.SOURCE_TILE_METADATA`
  - Contains master data for tile information
  - Provides business context for tile categorization
  - Includes active/inactive status for filtering

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

**New Target Table for Category KPIs:**
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_CATEGORY_KPIS
(
    date                      DATE   COMMENT 'Reporting date',
    tile_category            STRING COMMENT 'Tile business category',
    category_tile_views      LONG   COMMENT 'Total unique tile views for category',
    category_tile_clicks     LONG   COMMENT 'Total unique tile clicks for category',
    category_interstitial_views LONG COMMENT 'Total interstitial views for category',
    category_primary_clicks  LONG   COMMENT 'Total primary button clicks for category',
    category_secondary_clicks LONG  COMMENT 'Total secondary button clicks for category',
    category_ctr            DOUBLE COMMENT 'Category-level CTR'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily category-level KPIs for business analysis';
```

## Source-to-Target Mapping

### 1. SOURCE_TILE_METADATA to TARGET_HOME_TILE_DAILY_SUMMARY

| Source Field | Target Field | Transformation Rule | Data Type | Comments |
|--------------|--------------|-------------------|-----------|----------|
| tile_id | tile_id | Direct mapping | STRING | Primary key for joining |
| tile_name | tile_name | COALESCE(tile_name, 'Unknown') | STRING | Default for missing metadata |
| tile_category | tile_category | COALESCE(tile_category, 'Uncategorized') | STRING | Default for missing categories |
| is_active | N/A | Filter condition (is_active = true) | BOOLEAN | Used for filtering active tiles |

### 2. Enhanced Aggregation Mapping

| Source Tables | Target Field | Transformation Rule | Data Type | Comments |
|---------------|--------------|-------------------|-----------|----------|
| SOURCE_HOME_TILE_EVENTS | unique_tile_views | COUNT(DISTINCT user_id) WHERE event_type = 'TILE_VIEW' | LONG | Grouped by tile_id |
| SOURCE_HOME_TILE_EVENTS | unique_tile_clicks | COUNT(DISTINCT user_id) WHERE event_type = 'TILE_CLICK' | LONG | Grouped by tile_id |
| SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_views | COUNT(DISTINCT user_id) WHERE interstitial_view_flag = true | LONG | Grouped by tile_id |
| SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_primary_clicks | COUNT(DISTINCT user_id) WHERE primary_button_click_flag = true | LONG | Grouped by tile_id |
| SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_secondary_clicks | COUNT(DISTINCT user_id) WHERE secondary_button_click_flag = true | LONG | Grouped by tile_id |

### 3. Category-Level KPIs Mapping

| Source Field | Target Field | Transformation Rule | Data Type | Comments |
|--------------|--------------|-------------------|-----------|----------|
| tile_category | tile_category | Direct mapping from metadata | STRING | Grouping dimension |
| unique_tile_views | category_tile_views | SUM(unique_tile_views) GROUP BY tile_category | LONG | Category-level aggregation |
| unique_tile_clicks | category_tile_clicks | SUM(unique_tile_clicks) GROUP BY tile_category | LONG | Category-level aggregation |
| category_tile_clicks, category_tile_views | category_ctr | category_tile_clicks / category_tile_views | DOUBLE | Calculated CTR by category |

## Assumptions and Constraints

### Assumptions
1. **Data Quality:** SOURCE_TILE_METADATA contains valid and up-to-date tile information
2. **Data Availability:** Metadata table is updated before ETL execution
3. **Performance:** Left join with metadata table will not significantly impact performance
4. **Business Rules:** Only active tiles (is_active = true) should be included in reporting
5. **Default Values:** Missing metadata should be handled with default values rather than nulls

### Constraints
1. **Backward Compatibility:** Existing reports and dashboards must continue to function
2. **Data Governance:** All new fields must follow existing naming conventions
3. **Performance SLA:** ETL execution time should not increase by more than 20%
4. **Storage:** Additional metadata fields will increase storage requirements
5. **Idempotency:** Enhanced pipeline must maintain idempotent behavior for reruns

### Technical Constraints
1. **Join Performance:** Metadata table size should be reasonable for broadcast join
2. **Schema Evolution:** Target table schema changes require careful deployment
3. **Partition Strategy:** Maintain existing partitioning strategy for performance
4. **Delta Lake:** All tables must use Delta format for ACID transactions

## Implementation Considerations

### 1. Deployment Strategy
- **Phase 1:** Deploy enhanced source table (SOURCE_TILE_METADATA)
- **Phase 2:** Update target table schemas with new columns
- **Phase 3:** Deploy enhanced ETL code
- **Phase 4:** Validate data quality and performance

### 2. Testing Strategy
- **Unit Tests:** Test individual transformation functions
- **Integration Tests:** Test end-to-end pipeline with sample data
- **Performance Tests:** Validate ETL execution time within SLA
- **Data Quality Tests:** Verify metadata enrichment accuracy

### 3. Monitoring and Alerting
- **Data Quality Checks:** Monitor for missing metadata
- **Performance Monitoring:** Track ETL execution time
- **Business Metrics:** Monitor category-level KPI trends

## References

1. **Source Files:**
   - `Input/home_tile_reporting_etl.py` - Current ETL implementation
   - `Input/SourceDDL.sql` - Source table schemas
   - `Input/TargetDDL.sql` - Target table schemas
   - `Input/SOURCE_TILE_METADATA.sql` - New metadata table schema

2. **Technical Standards:**
   - Delta Lake best practices for data lakehouse architecture
   - PySpark coding standards and performance optimization
   - Data governance and security compliance requirements

3. **Business Requirements:**
   - Home tile reporting enhancement specifications
   - Category-based analytics requirements
   - Performance and scalability requirements

---

**Document Version:** 1.0  
**Last Updated:** 2025-12-02  
**Review Status:** Draft  
**Approved By:** [To be filled during review process]