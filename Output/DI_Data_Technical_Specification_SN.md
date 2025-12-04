=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for integrating SOURCE_TILE_METADATA into Home Tile Reporting ETL pipeline
=============================================

# Technical Specification for Home Tile Reporting Enhancement - Metadata Integration

## Introduction

### Purpose
This technical specification outlines the enhancement required to integrate the new `SOURCE_TILE_METADATA` table into the existing Home Tile Reporting ETL pipeline. The enhancement will enrich the reporting data with tile metadata information including tile names, categories, and active status.

### Scope
The enhancement involves:
- Integrating `SOURCE_TILE_METADATA` table into the existing ETL pipeline
- Enriching target tables with metadata information
- Updating data models to accommodate new fields
- Maintaining backward compatibility with existing processes

### Business Requirements
- Enrich tile reporting with business-friendly tile names and categories
- Enable filtering and grouping by tile categories in reports
- Provide active/inactive tile status for better business insights
- Maintain data lineage and audit capabilities

## Code Changes

### 1. Configuration Updates

**File:** `home_tile_reporting_etl.py`

**Changes Required:**
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

### 2. Data Reading Section Enhancement

**Current Code Location:** Lines 45-55

**Enhancement Required:**
```python
# Add metadata table reading
df_metadata = (
    spark.table(SOURCE_TILE_METADATA)
    .filter(F.col("is_active") == True)  # Only active tiles
    .select("tile_id", "tile_name", "tile_category")
)
```

### 3. Daily Summary Aggregation Enhancement

**Current Code Location:** Lines 57-85

**Enhancement Required:**
```python
# Enhanced daily summary with metadata enrichment
df_daily_summary_enriched = (
    df_daily_summary
    .join(df_metadata, "tile_id", "left")  # Left join to preserve all tiles
    .select(
        "date",
        "tile_id",
        F.coalesce("tile_name", F.lit("Unknown Tile")).alias("tile_name"),
        F.coalesce("tile_category", F.lit("Uncategorized")).alias("tile_category"),
        "unique_tile_views",
        "unique_tile_clicks",
        "unique_interstitial_views",
        "unique_interstitial_primary_clicks",
        "unique_interstitial_secondary_clicks"
    )
)
```

### 4. Global KPIs Enhancement

**Current Code Location:** Lines 87-110

**Enhancement Required:**
```python
# Enhanced global KPIs with category-level aggregations
df_global_by_category = (
    df_daily_summary_enriched.groupBy("date", "tile_category")
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

### 5. Write Operations Update

**Current Code Location:** Lines 112-125

**Enhancement Required:**
```python
# Update write operations for enriched tables
overwrite_partition(df_daily_summary_enriched, TARGET_DAILY_SUMMARY)
overwrite_partition(df_global, TARGET_GLOBAL_KPIS)
overwrite_partition(df_global_by_category, TARGET_CATEGORY_KPIS)  # New table
```

## Data Model Updates

### 1. Source Data Model Updates

**New Source Table:** `SOURCE_TILE_METADATA`
- **Purpose:** Master metadata for homepage tiles
- **Key Fields:** tile_id, tile_name, tile_category, is_active, updated_ts
- **Relationship:** One-to-many with tile events (tile_id as foreign key)

### 2. Target Data Model Updates

#### 2.1 Enhanced TARGET_HOME_TILE_DAILY_SUMMARY

**New DDL:**
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
(
    date                                DATE       COMMENT 'Reporting date',
    tile_id                             STRING     COMMENT 'Tile identifier',
    tile_name                           STRING     COMMENT 'Business-friendly tile name',
    tile_category                       STRING     COMMENT 'Business category of tile',
    unique_tile_views                   LONG       COMMENT 'Distinct users who viewed the tile',
    unique_tile_clicks                  LONG       COMMENT 'Distinct users who clicked the tile',
    unique_interstitial_views           LONG       COMMENT 'Distinct users who viewed interstitial',
    unique_interstitial_primary_clicks  LONG       COMMENT 'Distinct users who clicked primary button',
    unique_interstitial_secondary_clicks LONG      COMMENT 'Distinct users who clicked secondary button'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily aggregated tile-level metrics with metadata enrichment';
```

#### 2.2 New TARGET_HOME_TILE_CATEGORY_KPIS

**New Table DDL:**
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_CATEGORY_KPIS
(
    date                      DATE   COMMENT 'Reporting date',
    tile_category             STRING COMMENT 'Tile business category',
    category_tile_views       LONG   COMMENT 'Total unique tile views for category',
    category_tile_clicks      LONG   COMMENT 'Total unique tile clicks for category',
    category_interstitial_views LONG COMMENT 'Total interstitial views for category',
    category_primary_clicks   LONG   COMMENT 'Total primary button clicks for category',
    category_secondary_clicks LONG   COMMENT 'Total secondary button clicks for category',
    category_ctr              DOUBLE COMMENT 'Category-level CTR'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily category-level KPIs for business reporting';
```

### 3. Data Lineage Updates

**New Dependencies:**
- `TARGET_HOME_TILE_DAILY_SUMMARY` now depends on `SOURCE_TILE_METADATA`
- `TARGET_HOME_TILE_CATEGORY_KPIS` depends on `SOURCE_TILE_METADATA`
- Metadata refresh frequency should align with ETL schedule

## Source-to-Target Mapping

### 1. SOURCE_TILE_METADATA to TARGET_HOME_TILE_DAILY_SUMMARY

| Source Field | Target Field | Transformation Rule | Data Type | Comments |
|--------------|--------------|-------------------|-----------|----------|
| tile_id | tile_id | Direct mapping | STRING | Primary key for join |
| tile_name | tile_name | COALESCE(tile_name, 'Unknown Tile') | STRING | Handle missing metadata |
| tile_category | tile_category | COALESCE(tile_category, 'Uncategorized') | STRING | Default for missing categories |
| is_active | N/A | Filter condition (is_active = true) | BOOLEAN | Used for filtering only |

### 2. SOURCE_TILE_METADATA to TARGET_HOME_TILE_CATEGORY_KPIS

| Source Field | Target Field | Transformation Rule | Data Type | Comments |
|--------------|--------------|-------------------|-----------|----------|
| tile_category | tile_category | Direct mapping after grouping | STRING | Aggregation key |
| tile_id | N/A | Used for joining and aggregation | STRING | Not stored in target |

### 3. Aggregation Rules

#### Daily Summary Level
- **Granularity:** date + tile_id + tile_name + tile_category
- **Metrics:** All existing metrics preserved
- **Join Type:** LEFT JOIN to preserve tiles without metadata
- **Default Values:** 'Unknown Tile', 'Uncategorized' for missing metadata

#### Category Level
- **Granularity:** date + tile_category
- **Metrics:** SUM of all tile-level metrics within category
- **CTR Calculation:** category_tile_clicks / category_tile_views

### 4. Data Quality Rules

| Rule | Description | Implementation |
|------|-------------|----------------|
| Metadata Completeness | Handle missing tile metadata gracefully | Use COALESCE with default values |
| Active Tiles Only | Only process active tiles from metadata | Filter is_active = true |
| Data Consistency | Ensure tile_id consistency across tables | Implement data validation checks |
| Historical Preservation | Maintain historical data for deactivated tiles | Use LEFT JOIN strategy |

## Assumptions and Constraints

### Assumptions
1. **Data Availability:** `SOURCE_TILE_METADATA` table is available and populated before ETL execution
2. **Data Quality:** Tile metadata is maintained with reasonable data quality standards
3. **Performance:** Additional join operations will not significantly impact ETL performance
4. **Backward Compatibility:** Existing downstream consumers can handle new fields

### Constraints
1. **Schema Evolution:** Target table schema changes require coordination with downstream systems
2. **Data Governance:** New fields must comply with existing data governance policies
3. **Performance Impact:** Additional table joins may increase processing time
4. **Storage Impact:** New fields will increase storage requirements

### Technical Constraints
1. **Delta Lake Compatibility:** All changes must be compatible with Delta Lake format
2. **Partition Strategy:** Maintain existing partitioning strategy for performance
3. **Idempotency:** ETL must remain idempotent with partition overwrite strategy
4. **Error Handling:** Implement robust error handling for metadata join failures

### Business Constraints
1. **Data Freshness:** Metadata updates should be reflected in next ETL run
2. **Reporting Continuity:** No disruption to existing reporting processes
3. **Data Retention:** Follow existing data retention policies for new fields

## Implementation Plan

### Phase 1: Infrastructure Setup
1. Create new target table `TARGET_HOME_TILE_CATEGORY_KPIS`
2. Update existing table schema for `TARGET_HOME_TILE_DAILY_SUMMARY`
3. Validate `SOURCE_TILE_METADATA` table availability

### Phase 2: Code Enhancement
1. Update ETL pipeline code with metadata integration
2. Implement data quality checks and error handling
3. Add logging and monitoring for new data flows

### Phase 3: Testing and Validation
1. Unit testing for new transformation logic
2. Integration testing with sample data
3. Performance testing for impact assessment
4. Data validation and quality checks

### Phase 4: Deployment
1. Deploy to development environment
2. User acceptance testing
3. Production deployment with rollback plan
4. Monitor and validate post-deployment

## References

### Source Files
1. `home_tile_reporting_etl.py` - Main ETL pipeline code
2. `SourceDDL.sql` - Source table definitions
3. `TargetDDL.sql` - Target table definitions
4. `SOURCE_TILE_METADATA.sql` - New metadata table definition

### Related Documentation
1. Data Architecture Guidelines
2. ETL Development Standards
3. Data Governance Policies
4. Delta Lake Best Practices

### Stakeholders
1. **Business Users:** Enhanced reporting capabilities
2. **Data Engineers:** Implementation and maintenance
3. **Data Analysts:** New analytical dimensions
4. **DevOps Team:** Deployment and monitoring

---

**Document Version:** 1.0  
**Last Updated:** 2025-12-02  
**Next Review Date:** 2025-12-16  
**Approval Status:** Pending Review