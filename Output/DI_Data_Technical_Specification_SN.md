=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for Home Tile Reporting ETL enhancement with metadata integration
=============================================

# Technical Specification for Home Tile Reporting Enhancement

## Introduction

### Project Overview
This technical specification outlines the enhancement of the existing Home Tile Reporting ETL pipeline to integrate the new `SOURCE_TILE_METADATA` table. The enhancement will enrich the reporting capabilities by adding tile metadata information to provide business context and categorization for homepage tile analytics.

### Business Objectives
- Enhance reporting with tile metadata for better business insights
- Provide tile categorization and naming for improved dashboard readability
- Maintain data lineage and traceability through metadata integration
- Support active/inactive tile filtering for accurate reporting

### Scope
- Integration of `SOURCE_TILE_METADATA` table into existing ETL pipeline
- Enhancement of target data models to include metadata fields
- Update of source-to-target mapping with metadata transformations
- Maintenance of existing functionality and performance

## Code Changes

### 1. Configuration Updates

#### New Source Table Configuration
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

### 2. Data Reading Enhancement

#### Read Tile Metadata
```python
# Read tile metadata for enrichment
df_metadata = (
    spark.table(SOURCE_TILE_METADATA)
    .filter(F.col("is_active") == True)  # Only active tiles
    .select(
        "tile_id",
        "tile_name",
        "tile_category",
        "updated_ts"
    )
)
```

### 3. ETL Logic Modifications

#### Enhanced Daily Summary with Metadata
```python
# Enhanced daily summary aggregation with metadata join
df_daily_summary_enhanced = (
    df_daily_summary
    .join(df_metadata, "tile_id", "left")
    .select(
        "date",
        "tile_id",
        F.coalesce("tile_name", F.lit("Unknown Tile")).alias("tile_name"),
        F.coalesce("tile_category", F.lit("Uncategorized")).alias("tile_category"),
        "unique_tile_views",
        "unique_tile_clicks",
        "unique_interstitial_views",
        "unique_interstitial_primary_clicks",
        "unique_interstitial_secondary_clicks",
        F.current_timestamp().alias("etl_processed_ts")
    )
)
```

#### Category-Level Aggregation
```python
# New category-level aggregation for enhanced reporting
df_category_summary = (
    df_daily_summary_enhanced
    .groupBy("date", "tile_category")
    .agg(
        F.sum("unique_tile_views").alias("category_tile_views"),
        F.sum("unique_tile_clicks").alias("category_tile_clicks"),
        F.sum("unique_interstitial_views").alias("category_interstitial_views"),
        F.sum("unique_interstitial_primary_clicks").alias("category_primary_clicks"),
        F.sum("unique_interstitial_secondary_clicks").alias("category_secondary_clicks"),
        F.count("tile_id").alias("active_tiles_count")
    )
    .withColumn(
        "category_ctr",
        F.when(F.col("category_tile_views") > 0,
               F.col("category_tile_clicks") / F.col("category_tile_views")).otherwise(0.0)
    )
)
```

### 4. Error Handling and Data Quality

```python
# Data quality checks for metadata integration
def validate_metadata_join(df_summary, df_metadata):
    """
    Validate metadata join and log any missing tile metadata
    """
    missing_metadata = (
        df_summary.select("tile_id")
        .distinct()
        .join(df_metadata, "tile_id", "left_anti")
    )
    
    missing_count = missing_metadata.count()
    if missing_count > 0:
        print(f"Warning: {missing_count} tiles missing metadata")
        missing_metadata.show()
    
    return missing_count

# Add validation call
validate_metadata_join(df_daily_summary, df_metadata)
```

## Data Model Updates

### 1. Enhanced Target Daily Summary Table

#### Updated DDL for TARGET_HOME_TILE_DAILY_SUMMARY
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
(
    date                                DATE       COMMENT 'Reporting date',
    tile_id                             STRING     COMMENT 'Tile identifier',
    tile_name                           STRING     COMMENT 'User-friendly tile name from metadata',
    tile_category                       STRING     COMMENT 'Business category of the tile',
    unique_tile_views                   LONG       COMMENT 'Distinct users who viewed the tile',
    unique_tile_clicks                  LONG       COMMENT 'Distinct users who clicked the tile',
    unique_interstitial_views           LONG       COMMENT 'Distinct users who viewed interstitial',
    unique_interstitial_primary_clicks  LONG       COMMENT 'Distinct users who clicked primary button',
    unique_interstitial_secondary_clicks LONG      COMMENT 'Distinct users who clicked secondary button',
    etl_processed_ts                    TIMESTAMP  COMMENT 'ETL processing timestamp'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily aggregated tile-level metrics with metadata enrichment';
```

### 2. New Category Summary Table

#### New DDL for TARGET_HOME_TILE_CATEGORY_SUMMARY
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_CATEGORY_SUMMARY
(
    date                      DATE   COMMENT 'Reporting date',
    tile_category            STRING COMMENT 'Business category of tiles',
    category_tile_views      LONG   COMMENT 'Total unique tile views in category',
    category_tile_clicks     LONG   COMMENT 'Total unique tile clicks in category',
    category_interstitial_views LONG COMMENT 'Total interstitial views in category',
    category_primary_clicks  LONG   COMMENT 'Total primary button clicks in category',
    category_secondary_clicks LONG  COMMENT 'Total secondary button clicks in category',
    category_ctr             DOUBLE COMMENT 'Category-level click-through rate',
    active_tiles_count       LONG   COMMENT 'Number of active tiles in category',
    etl_processed_ts         TIMESTAMP COMMENT 'ETL processing timestamp'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily category-level aggregated metrics';
```

### 3. Enhanced Global KPIs Table

#### Updated DDL for TARGET_HOME_TILE_GLOBAL_KPIS
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS
(
    date                      DATE   COMMENT 'Reporting date',
    total_tile_views          LONG   COMMENT 'Total unique tile views across tiles',
    total_tile_clicks         LONG   COMMENT 'Total unique tile clicks across tiles',
    total_interstitial_views  LONG   COMMENT 'Total interstitial views',
    total_primary_clicks      LONG   COMMENT 'Total primary button clicks',
    total_secondary_clicks    LONG   COMMENT 'Total secondary button clicks',
    overall_ctr               DOUBLE COMMENT 'Tile Clicks / Tile Views',
    overall_primary_ctr       DOUBLE COMMENT 'Primary Clicks / Interstitial Views',
    overall_secondary_ctr     DOUBLE COMMENT 'Secondary Clicks / Interstitial Views',
    total_active_tiles        LONG   COMMENT 'Total number of active tiles',
    total_categories          LONG   COMMENT 'Total number of tile categories',
    etl_processed_ts          TIMESTAMP COMMENT 'ETL processing timestamp'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily global KPIs with metadata insights';
```

## Source-to-Target Mapping

### 1. SOURCE_TILE_METADATA to TARGET_HOME_TILE_DAILY_SUMMARY

| Source Field | Target Field | Transformation Rule | Data Type | Comments |
|--------------|--------------|-------------------|-----------|----------|
| tile_id | tile_id | Direct mapping | STRING | Primary key for join |
| tile_name | tile_name | COALESCE(tile_name, 'Unknown Tile') | STRING | Default for missing metadata |
| tile_category | tile_category | COALESCE(tile_category, 'Uncategorized') | STRING | Default category assignment |
| is_active | N/A | Filter condition (is_active = true) | BOOLEAN | Only active tiles included |
| updated_ts | N/A | Used for data lineage tracking | TIMESTAMP | Metadata freshness indicator |

### 2. Enhanced Aggregation Mapping

| Source Tables | Target Field | Transformation Rule | Data Type | Comments |
|---------------|--------------|-------------------|-----------|----------|
| SOURCE_HOME_TILE_EVENTS + SOURCE_TILE_METADATA | tile_name | LEFT JOIN on tile_id | STRING | Enriched with metadata |
| SOURCE_HOME_TILE_EVENTS + SOURCE_TILE_METADATA | tile_category | LEFT JOIN on tile_id | STRING | Business categorization |
| Multiple Sources | etl_processed_ts | CURRENT_TIMESTAMP() | TIMESTAMP | Processing audit trail |

### 3. Category-Level Aggregation Mapping

| Source Field | Target Field | Transformation Rule | Data Type | Comments |
|--------------|--------------|-------------------|-----------|----------|
| tile_category | tile_category | GROUP BY tile_category | STRING | Aggregation key |
| unique_tile_views | category_tile_views | SUM(unique_tile_views) | LONG | Category-level sum |
| unique_tile_clicks | category_tile_clicks | SUM(unique_tile_clicks) | LONG | Category-level sum |
| unique_interstitial_views | category_interstitial_views | SUM(unique_interstitial_views) | LONG | Category-level sum |
| unique_interstitial_primary_clicks | category_primary_clicks | SUM(unique_interstitial_primary_clicks) | LONG | Category-level sum |
| unique_interstitial_secondary_clicks | category_secondary_clicks | SUM(unique_interstitial_secondary_clicks) | LONG | Category-level sum |
| tile_id | active_tiles_count | COUNT(DISTINCT tile_id) | LONG | Active tiles per category |
| Calculated | category_ctr | category_tile_clicks / category_tile_views | DOUBLE | Category click-through rate |

### 4. Global KPIs Enhancement Mapping

| Source Field | Target Field | Transformation Rule | Data Type | Comments |
|--------------|--------------|-------------------|-----------|----------|
| tile_id (from metadata) | total_active_tiles | COUNT(DISTINCT tile_id WHERE is_active=true) | LONG | Active tiles count |
| tile_category (from metadata) | total_categories | COUNT(DISTINCT tile_category) | LONG | Category diversity metric |
| Current timestamp | etl_processed_ts | CURRENT_TIMESTAMP() | TIMESTAMP | Processing audit |

## Assumptions and Constraints

### Assumptions
1. **Metadata Availability**: SOURCE_TILE_METADATA table is populated and maintained by upstream systems
2. **Data Quality**: Tile metadata is reasonably complete with minimal missing values
3. **Performance**: Metadata table size is manageable for broadcast joins
4. **Active Tiles**: Only active tiles (is_active = true) should be included in reporting
5. **Backward Compatibility**: Existing downstream consumers can handle new fields

### Constraints
1. **Data Freshness**: Metadata updates may have latency, requiring graceful handling of missing metadata
2. **Schema Evolution**: Target table schema changes require coordination with downstream systems
3. **Performance Impact**: Additional joins may impact ETL performance for large datasets
4. **Storage**: New fields will increase storage requirements for target tables
5. **Idempotency**: Enhanced ETL must maintain idempotent behavior for reprocessing

### Technical Constraints
1. **Memory Usage**: Metadata broadcast join must fit in driver/executor memory
2. **Partition Strategy**: New fields should not impact existing partitioning strategy
3. **Delta Lake**: All operations must be compatible with Delta Lake format
4. **Spark Version**: Code must be compatible with existing Spark cluster version

## Implementation Considerations

### 1. Deployment Strategy
- **Phase 1**: Deploy enhanced DDL scripts for target tables
- **Phase 2**: Deploy updated ETL pipeline with metadata integration
- **Phase 3**: Validate data quality and performance
- **Phase 4**: Enable downstream reporting enhancements

### 2. Testing Strategy
- Unit tests for metadata join logic
- Integration tests with sample metadata
- Performance testing with production-scale data
- Data quality validation tests

### 3. Monitoring and Alerting
- Monitor metadata join success rates
- Alert on missing metadata beyond threshold
- Track ETL performance impact
- Monitor target table growth

## References

### Source Files
1. `Input/home_tile_reporting_etl.py` - Existing ETL pipeline
2. `Input/SourceDDL.sql` - Source table definitions
3. `Input/TargetDDL.sql` - Target table definitions  
4. `Input/SOURCE_TILE_METADATA.sql` - New metadata table definition

### Related Documentation
- Data Architecture Guidelines
- ETL Development Standards
- Delta Lake Best Practices
- Spark Performance Tuning Guide

---

**Document Version**: 1.0  
**Last Updated**: 2025-12-02  
**Review Status**: Draft  
**Approval Required**: Data Architecture Team, Business Stakeholders