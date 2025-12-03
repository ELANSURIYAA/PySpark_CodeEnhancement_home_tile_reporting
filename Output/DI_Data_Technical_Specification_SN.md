=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target reporting with tile category enrichment
=============================================

# Technical Specification for Home Tile Reporting Enhancement - Tile Metadata Integration

## Introduction

### Business Context
This enhancement addresses the business requirement to enrich home tile reporting with metadata categorization. The current ETL pipeline aggregates tile metrics at the tile_id level without business grouping. Product Analytics has requested the addition of tile-level metadata to enable category-based reporting and performance tracking.

### Enhancement Scope
- Add new source table `SOURCE_TILE_METADATA` for tile categorization
- Extend existing ETL pipeline to incorporate metadata enrichment
- Update target table schema to include `tile_category` column
- Maintain backward compatibility and data integrity

### Business Benefits
- Enable performance metrics by functional category (Offers, Health Checks, Payments)
- Support category-level CTR comparisons
- Improve dashboard drilldowns and business insights
- Enable future tile segmentation capabilities

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
df_metadata = (
    spark.table(SOURCE_TILE_METADATA)
    .filter(F.col("is_active") == True)
    .select("tile_id", "tile_name", "tile_category")
)
```

#### 1.3 Daily Summary Aggregation Enhancement
```python
# Modified daily summary with metadata join
df_daily_summary_enriched = (
    df_daily_summary
    .join(df_metadata, "tile_id", "left")
    .withColumn(
        "tile_category", 
        F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))
    )
    .withColumn(
        "tile_name", 
        F.coalesce(F.col("tile_name"), F.lit("Unknown Tile"))
    )
    .select(
        "date",
        "tile_id",
        "tile_name",
        "tile_category",
        "unique_tile_views",
        "unique_tile_clicks",
        "unique_interstitial_views",
        "unique_interstitial_primary_clicks",
        "unique_interstitial_secondary_clicks"
    )
)
```

#### 1.4 Error Handling and Validation
```python
# Add validation for metadata table availability
def validate_metadata_table():
    try:
        metadata_count = spark.table(SOURCE_TILE_METADATA).count()
        print(f"Metadata table contains {metadata_count} records")
        return True
    except Exception as e:
        print(f"Warning: Metadata table not available: {e}")
        return False

# Call validation before processing
metadata_available = validate_metadata_table()
```

### 2. Target Table Write Logic Updates
```python
# Update overwrite function to handle new schema
overwrite_partition(df_daily_summary_enriched, TARGET_DAILY_SUMMARY)
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

**Sample Data:**
| tile_id | tile_name | tile_category | is_active | updated_ts |
|---------|-----------|---------------|-----------|------------|
| TILE_001 | Credit Score Check | Personal Finance | true | 2025-12-01 |
| TILE_002 | Health Assessment | Health & Wellness | true | 2025-12-01 |
| TILE_003 | Payment Options | Payments | true | 2025-12-01 |

### 2. Updated Target Table: `TARGET_HOME_TILE_DAILY_SUMMARY`

**Enhanced Schema:**
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
(
    date                                DATE       COMMENT 'Reporting date',
    tile_id                             STRING     COMMENT 'Tile identifier',
    tile_name                           STRING     COMMENT 'User-friendly tile name',
    tile_category                       STRING     COMMENT 'Functional category of the tile',
    unique_tile_views                   LONG       COMMENT 'Distinct users who viewed the tile',
    unique_tile_clicks                  LONG       COMMENT 'Distinct users who clicked the tile',
    unique_interstitial_views           LONG       COMMENT 'Distinct users who viewed interstitial',
    unique_interstitial_primary_clicks  LONG       COMMENT 'Distinct users who clicked primary button',
    unique_interstitial_secondary_clicks LONG      COMMENT 'Distinct users who clicked secondary button'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily aggregated tile-level metrics with metadata enrichment for reporting';
```

**Schema Changes:**
- **Added:** `tile_name STRING` - User-friendly tile name from metadata
- **Added:** `tile_category STRING` - Business category for grouping and analysis

### 3. Data Lineage Impact

**New Data Flow:**
```
SOURCE_HOME_TILE_EVENTS ──┐
                          ├─► ETL Pipeline ─► TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_INTERSTITIAL_EVENTS ┤                    (Enhanced with metadata)
                          │
SOURCE_TILE_METADATA ─────┘
```

## Source-to-Target Mapping

### 1. Enhanced Daily Summary Mapping

| Source Field | Source Table | Target Field | Transformation Rule | Data Type |
|--------------|--------------|--------------|-------------------|----------|
| event_ts | SOURCE_HOME_TILE_EVENTS | date | `to_date(event_ts)` | DATE |
| tile_id | SOURCE_HOME_TILE_EVENTS | tile_id | Direct mapping | STRING |
| tile_name | SOURCE_TILE_METADATA | tile_name | `COALESCE(tile_name, 'Unknown Tile')` | STRING |
| tile_category | SOURCE_TILE_METADATA | tile_category | `COALESCE(tile_category, 'UNKNOWN')` | STRING |
| user_id | SOURCE_HOME_TILE_EVENTS | unique_tile_views | `COUNT(DISTINCT user_id) WHERE event_type = 'TILE_VIEW'` | LONG |
| user_id | SOURCE_HOME_TILE_EVENTS | unique_tile_clicks | `COUNT(DISTINCT user_id) WHERE event_type = 'TILE_CLICK'` | LONG |
| user_id | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_views | `COUNT(DISTINCT user_id) WHERE interstitial_view_flag = true` | LONG |
| user_id | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_primary_clicks | `COUNT(DISTINCT user_id) WHERE primary_button_click_flag = true` | LONG |
| user_id | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_secondary_clicks | `COUNT(DISTINCT user_id) WHERE secondary_button_click_flag = true` | LONG |

### 2. Join Logic

**Primary Join:**
```sql
LEFT JOIN SOURCE_TILE_METADATA metadata 
    ON events.tile_id = metadata.tile_id 
    AND metadata.is_active = true
```

**Null Handling:**
- `tile_category`: Default to "UNKNOWN" when no metadata exists
- `tile_name`: Default to "Unknown Tile" when no metadata exists
- Ensures backward compatibility for existing tiles without metadata

### 3. Business Rules

| Rule ID | Description | Implementation |
|---------|-------------|----------------|
| BR001 | Only active tiles from metadata should be used | `WHERE is_active = true` |
| BR002 | Missing metadata should not break pipeline | `LEFT JOIN` with `COALESCE` defaults |
| BR003 | Historical data compatibility | Existing tiles get "UNKNOWN" category |
| BR004 | Date partitioning maintained | `PARTITIONED BY (date)` preserved |

## Assumptions and Constraints

### Assumptions
1. **Metadata Availability**: SOURCE_TILE_METADATA table will be populated before ETL execution
2. **Data Quality**: tile_id in metadata table matches tile_id in event tables
3. **Performance**: LEFT JOIN on tile_id will not significantly impact ETL performance
4. **Backward Compatibility**: Existing dashboards can handle new columns
5. **Data Governance**: Metadata table will be maintained by Product team

### Constraints
1. **Schema Evolution**: Target table schema change requires coordinated deployment
2. **Historical Data**: No backfill of historical data with categories (out of scope)
3. **Global KPIs**: No changes to TARGET_HOME_TILE_GLOBAL_KPIS table
4. **Dashboard Updates**: BI team responsible for dashboard modifications
5. **Data Retention**: Metadata table follows same retention policy as source tables

### Technical Constraints
1. **Delta Lake Compatibility**: All tables must use Delta format
2. **Partition Strategy**: Date-based partitioning maintained for performance
3. **Idempotency**: ETL must support reprocessing without data duplication
4. **Error Handling**: Pipeline should gracefully handle missing metadata

## Implementation Plan

### Phase 1: Infrastructure Setup
1. Create SOURCE_TILE_METADATA table in analytics_db
2. Insert initial metadata records
3. Update table permissions and access controls

### Phase 2: ETL Enhancement
1. Modify home_tile_reporting_etl.py with metadata integration
2. Update TARGET_HOME_TILE_DAILY_SUMMARY schema
3. Implement error handling and validation

### Phase 3: Testing and Validation
1. Unit testing for metadata join logic
2. Integration testing with sample data
3. Performance testing with production data volumes
4. Backward compatibility validation

### Phase 4: Deployment
1. Deploy schema changes to target table
2. Deploy updated ETL pipeline
3. Monitor initial runs for data quality
4. Coordinate with BI team for dashboard updates

## References

### JIRA Stories
- **SCRUM-7567**: Add New Source Table SOURCE_TILE_METADATA and Extend Target Reporting Metrics

### Technical Documentation
- **Source Code**: `Input/home_tile_reporting_etl.py`
- **Source DDL**: `Input/SourceDDL.sql`
- **Target DDL**: `Input/TargetDDL.sql`
- **Metadata DDL**: `Input/SOURCE_TILE_METADATA.sql`

### Data Lineage
- **Source Tables**: analytics_db.SOURCE_HOME_TILE_EVENTS, analytics_db.SOURCE_INTERSTITIAL_EVENTS, analytics_db.SOURCE_TILE_METADATA
- **Target Tables**: reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY, reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS

### Business Stakeholders
- **Product Analytics Team**: Requirements and acceptance criteria
- **BI Team**: Dashboard integration and visualization
- **Data Engineering Team**: Implementation and maintenance

---

**Document Version**: 1.0  
**Last Updated**: 2025-12-03  
**Review Status**: Draft  
**Approval Required**: Technical Lead, Product Owner