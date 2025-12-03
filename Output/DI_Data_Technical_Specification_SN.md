=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target reporting with tile category
=============================================

# Technical Specification for Home Tile Reporting Enhancement

## Introduction

### Business Context
This technical specification outlines the enhancement to the existing Home Tile Reporting ETL pipeline to incorporate a new source table `SOURCE_TILE_METADATA` and extend the target reporting metrics with tile category information. The enhancement aims to enable better reporting and performance tracking at a functional level by adding business grouping capabilities.

### Objectives
- Add new Delta source table `analytics_db.SOURCE_TILE_METADATA`
- Enrich existing ETL pipeline to include tile category metadata
- Modify target table `TARGET_HOME_TILE_DAILY_SUMMARY` to include tile category
- Maintain backward compatibility and ensure no schema drift
- Enable category-level performance analysis and CTR comparisons

### Enhancement Scope
- **In Scope**: New source table creation, ETL pipeline modifications, target table schema updates
- **Out of Scope**: Global KPI table changes, historical data backfill, dashboard UI updates

## Code Changes

### 1. ETL Pipeline Modifications (`home_tile_reporting_etl.py`)

#### 1.1 Configuration Updates
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

#### 1.2 Source Data Reading Enhancement
```python
# Read tile metadata table
df_tile_metadata = spark.table(SOURCE_TILE_METADATA)

# Filter for active tiles only
df_tile_metadata_active = df_tile_metadata.filter(F.col("is_active") == True)
```

#### 1.3 Daily Summary Aggregation Logic Update
```python
# Enhanced daily summary with metadata join
df_daily_summary_enhanced = (
    df_daily_summary
    .join(df_tile_metadata_active, "tile_id", "left")
    .select(
        "date",
        "tile_id",
        F.coalesce("tile_category", F.lit("UNKNOWN")).alias("tile_category"),
        "unique_tile_views",
        "unique_tile_clicks",
        "unique_interstitial_views",
        "unique_interstitial_primary_clicks",
        "unique_interstitial_secondary_clicks"
    )
)
```

#### 1.4 Write Operation Update
```python
# Update target table write with new schema
overwrite_partition(df_daily_summary_enhanced, TARGET_DAILY_SUMMARY)
```

### 2. Error Handling and Data Quality
```python
# Add data quality checks
def validate_metadata_join(df):
    """Validate that metadata join doesn't cause data loss"""
    unknown_count = df.filter(F.col("tile_category") == "UNKNOWN").count()
    total_count = df.count()
    
    if unknown_count > total_count * 0.1:  # More than 10% unknown
        print(f"Warning: {unknown_count}/{total_count} tiles have UNKNOWN category")
    
    return df

df_daily_summary_enhanced = validate_metadata_join(df_daily_summary_enhanced)
```

## Data Model Updates

### 1. New Source Table: `SOURCE_TILE_METADATA`

#### Table Structure
| Column Name | Data Type | Description | Constraints |
|-------------|-----------|-------------|-------------|
| tile_id | STRING | Tile identifier | Primary Key |
| tile_name | STRING | User-friendly tile name | NOT NULL |
| tile_category | STRING | Business/functional category | NOT NULL |
| is_active | BOOLEAN | Active status indicator | NOT NULL |
| updated_ts | TIMESTAMP | Last update timestamp | NOT NULL |

#### Business Categories
- Personal Finance
- Health Checks
- Payments
- Offers
- Notifications
- UNKNOWN (default)

### 2. Target Table Schema Update: `TARGET_HOME_TILE_DAILY_SUMMARY`

#### Updated Schema
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
COMMENT 'Daily aggregated tile-level metrics with category enrichment';
```

### 3. Data Lineage Impact
```
SOURCE_HOME_TILE_EVENTS ──┐
                           ├── ETL Pipeline ──> TARGET_HOME_TILE_DAILY_SUMMARY (Enhanced)
SOURCE_INTERSTITIAL_EVENTS ┤
                           │
SOURCE_TILE_METADATA ──────┘
```

## Source-to-Target Mapping

### 1. Enhanced Daily Summary Mapping

| Source Table | Source Column | Target Table | Target Column | Transformation Rule |
|--------------|---------------|--------------|---------------|--------------------|
| SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | tile_id | Direct mapping |
| SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | tile_category | Left join on tile_id, default to "UNKNOWN" |
| Aggregated | date | TARGET_HOME_TILE_DAILY_SUMMARY | date | Process date literal |
| Aggregated | unique_tile_views | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_views | COUNT(DISTINCT user_id) WHERE event_type='TILE_VIEW' |
| Aggregated | unique_tile_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_clicks | COUNT(DISTINCT user_id) WHERE event_type='TILE_CLICK' |
| SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_views | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_views | COUNT(DISTINCT user_id) WHERE interstitial_view_flag=true |
| SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_primary_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_primary_clicks | COUNT(DISTINCT user_id) WHERE primary_button_click_flag=true |
| SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_secondary_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_secondary_clicks | COUNT(DISTINCT user_id) WHERE secondary_button_click_flag=true |

### 2. Metadata Enrichment Logic

```sql
-- Pseudocode for metadata enrichment
SELECT 
    ds.date,
    ds.tile_id,
    COALESCE(tm.tile_category, 'UNKNOWN') as tile_category,
    ds.unique_tile_views,
    ds.unique_tile_clicks,
    ds.unique_interstitial_views,
    ds.unique_interstitial_primary_clicks,
    ds.unique_interstitial_secondary_clicks
FROM daily_summary ds
LEFT JOIN tile_metadata tm ON ds.tile_id = tm.tile_id AND tm.is_active = true
```

### 3. Data Quality Rules

| Rule | Description | Implementation |
|------|-------------|----------------|
| Completeness | All tile_ids should have category | Default to "UNKNOWN" if no match |
| Consistency | Category values must be standardized | Validate against approved list |
| Timeliness | Metadata should be current | Filter by is_active = true |
| Accuracy | Join should not duplicate records | Use LEFT JOIN with unique tile_id |

## Assumptions and Constraints

### Assumptions
1. **Data Availability**: SOURCE_TILE_METADATA will be populated and maintained by the product team
2. **Performance**: Left join operation will not significantly impact ETL performance
3. **Backward Compatibility**: Existing downstream consumers can handle additional column
4. **Data Volume**: Metadata table will remain relatively small (<10K records)
5. **Update Frequency**: Metadata updates will be infrequent (weekly/monthly)

### Constraints
1. **Schema Evolution**: Must maintain backward compatibility for existing consumers
2. **Performance**: ETL runtime should not increase by more than 10%
3. **Data Quality**: No loss of existing data during enhancement
4. **Partitioning**: Maintain existing partition strategy on target table
5. **Resource Usage**: No additional cluster resources required

### Technical Constraints
1. **Delta Lake**: All tables must use Delta format for ACID compliance
2. **Spark Version**: Compatible with existing Databricks runtime
3. **Memory**: Join operation must fit within current memory allocation
4. **Idempotency**: Pipeline must remain idempotent for reprocessing

## Implementation Considerations

### 1. Deployment Strategy
- **Phase 1**: Create SOURCE_TILE_METADATA table and populate with initial data
- **Phase 2**: Deploy ETL code changes with feature flag
- **Phase 3**: Update target table schema (backward compatible)
- **Phase 4**: Enable feature flag and validate results
- **Phase 5**: Update documentation and notify downstream consumers

### 2. Testing Strategy
- **Unit Tests**: Validate join logic and default value handling
- **Integration Tests**: End-to-end pipeline execution with sample data
- **Performance Tests**: Measure impact on ETL runtime
- **Data Quality Tests**: Validate completeness and accuracy of enrichment

### 3. Monitoring and Alerting
- Monitor percentage of "UNKNOWN" categories
- Alert on significant changes in category distribution
- Track ETL performance metrics
- Validate record counts remain consistent

## References

### JIRA Stories
- **SCRUM-7567**: Add New Source Table & Extend Target Reporting Metrics

### Technical Documentation
- Home Tile Reporting ETL Pipeline Documentation
- Data Model Documentation
- Delta Lake Best Practices
- Databricks ETL Standards

### Data Sources
- `analytics_db.SOURCE_HOME_TILE_EVENTS`
- `analytics_db.SOURCE_INTERSTITIAL_EVENTS`
- `analytics_db.SOURCE_TILE_METADATA` (new)

### Target Tables
- `reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY` (enhanced)
- `reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS` (unchanged)

---

**Document Version**: 1.0  
**Last Updated**: 2025-12-03  
**Review Status**: Draft  
**Approved By**: Pending Review