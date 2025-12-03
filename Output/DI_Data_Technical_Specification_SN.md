=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target reporting with tile category
=============================================

# Technical Specification for Home Tile Reporting Enhancement - Tile Category Integration

## Introduction

### Project Overview
This technical specification outlines the implementation details for SCRUM-7567: "Add New Source Table & Extend Target Reporting Metrics". The enhancement involves integrating a new source table `SOURCE_TILE_METADATA` into the existing Home Tile Reporting ETL pipeline to enrich reporting dashboards with tile category information.

### Business Objective
Product Analytics has requested the addition of tile-level metadata to enable:
- Performance tracking at functional category level (e.g., Offers, Health Checks, Payments)
- Category-level CTR comparisons
- Enhanced dashboard drilldowns in Power BI/Tableau
- Future segmentation capabilities for tile management

### Current State
- ETL pipeline processes `SOURCE_HOME_TILE_EVENTS` and `SOURCE_INTERSTITIAL_EVENTS`
- Aggregates metrics at tile_id level without business grouping
- Outputs to `TARGET_HOME_TILE_DAILY_SUMMARY` and `TARGET_HOME_TILE_GLOBAL_KPIS`

### Target State
- New source table `SOURCE_TILE_METADATA` containing tile category mappings
- Enhanced `TARGET_HOME_TILE_DAILY_SUMMARY` with `tile_category` column
- Enriched ETL pipeline with metadata joins
- Backward compatibility maintained

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
    .select("tile_id", "tile_category", "tile_name")
)
```

#### 1.3 Daily Summary Aggregation Enhancement
```python
# Modified daily summary with metadata join
df_daily_summary = (
    df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    .join(df_metadata, "tile_id", "left")  # Left join to maintain all tiles
    .withColumn("date", F.lit(PROCESS_DATE))
    .select(
        "date",
        "tile_id",
        F.coalesce("tile_category", F.lit("UNKNOWN")).alias("tile_category"),  # Default for unmapped tiles
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
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

### 2. Target Table Schema Updates

#### 2.1 TARGET_HOME_TILE_DAILY_SUMMARY Enhancement
```sql
-- Add new column to existing table
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
ADD COLUMN tile_category STRING COMMENT 'Functional category of the tile';
```

## Data Model Updates

### 1. New Source Table: SOURCE_TILE_METADATA

#### Schema Definition
| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|--------------|
| tile_id | STRING | No | Primary key - Tile identifier |
| tile_name | STRING | Yes | User-friendly tile name |
| tile_category | STRING | Yes | Business/functional category |
| is_active | BOOLEAN | No | Active status flag |
| updated_ts | TIMESTAMP | Yes | Last update timestamp |

#### Business Rules
- `tile_id` serves as the primary key and join key
- `is_active = true` tiles are included in reporting
- `tile_category` values should follow business taxonomy
- Table supports SCD Type 1 updates

### 2. Enhanced Target Table: TARGET_HOME_TILE_DAILY_SUMMARY

#### Updated Schema
| Column Name | Data Type | Description | Change Type |
|-------------|-----------|-------------|-------------|
| date | DATE | Reporting date | Existing |
| tile_id | STRING | Tile identifier | Existing |
| **tile_category** | **STRING** | **Functional category** | **NEW** |
| unique_tile_views | LONG | Distinct tile viewers | Existing |
| unique_tile_clicks | LONG | Distinct tile clickers | Existing |
| unique_interstitial_views | LONG | Distinct interstitial viewers | Existing |
| unique_interstitial_primary_clicks | LONG | Distinct primary button clicks | Existing |
| unique_interstitial_secondary_clicks | LONG | Distinct secondary button clicks | Existing |

### 3. Data Lineage Updates

```
SOURCE_HOME_TILE_EVENTS ──┐
                          ├──► TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_INTERSTITIAL_EVENTS ┤
                          │
SOURCE_TILE_METADATA ─────┘

SOURCE_HOME_TILE_EVENTS ──┐
                          ├──► TARGET_HOME_TILE_GLOBAL_KPIS (No Change)
SOURCE_INTERSTITIAL_EVENTS ┘
```

## Source-to-Target Mapping

### 1. SOURCE_TILE_METADATA to TARGET_HOME_TILE_DAILY_SUMMARY

| Source Table | Source Column | Target Table | Target Column | Transformation Rule |
|--------------|---------------|--------------|---------------|--------------------|
| SOURCE_TILE_METADATA | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | tile_id | Direct mapping (join key) |
| SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | tile_category | COALESCE(tile_category, 'UNKNOWN') |

### 2. Enhanced Aggregation Logic

#### Join Strategy
```sql
-- Conceptual SQL representation
SELECT 
    CURRENT_DATE as date,
    t.tile_id,
    COALESCE(m.tile_category, 'UNKNOWN') as tile_category,
    t.unique_tile_views,
    t.unique_tile_clicks,
    i.unique_interstitial_views,
    i.unique_interstitial_primary_clicks,
    i.unique_interstitial_secondary_clicks
FROM tile_aggregates t
FULL OUTER JOIN interstitial_aggregates i ON t.tile_id = i.tile_id
LEFT JOIN SOURCE_TILE_METADATA m ON t.tile_id = m.tile_id AND m.is_active = true
```

### 3. Data Quality Rules

| Rule | Description | Implementation |
|------|-------------|----------------|
| Completeness | All tiles must have category | Default to 'UNKNOWN' if no metadata |
| Consistency | Category values follow taxonomy | Validation in metadata ingestion |
| Referential Integrity | All tile_ids in events should exist in metadata | Left join preserves all event data |
| Backward Compatibility | Existing metrics unchanged | Maintain all existing aggregation logic |

## Assumptions and Constraints

### Assumptions
1. **Metadata Availability**: SOURCE_TILE_METADATA will be populated before ETL execution
2. **Data Freshness**: Metadata updates are infrequent (weekly/monthly)
3. **Category Taxonomy**: Business will maintain consistent category naming
4. **Performance Impact**: Additional join will not significantly impact ETL runtime
5. **Schema Evolution**: Target table can be altered without breaking downstream consumers

### Constraints
1. **Backward Compatibility**: Existing metrics and counts must remain unchanged
2. **Data Retention**: Historical data backfill is out of scope
3. **Global KPIs**: No changes to TARGET_HOME_TILE_GLOBAL_KPIS table
4. **Dashboard Updates**: BI team responsible for dashboard modifications
5. **Idempotency**: Daily partition overwrite behavior must be maintained

### Technical Constraints
1. **Delta Lake**: All tables use Delta format for ACID transactions
2. **Partitioning**: Maintain date-based partitioning strategy
3. **Spark Version**: Compatible with existing Databricks runtime
4. **Memory**: Additional metadata join should not exceed cluster memory limits

## Implementation Plan

### Phase 1: Infrastructure Setup
1. Create SOURCE_TILE_METADATA table in analytics_db
2. Add table to data catalog and schema registry
3. Insert initial metadata records for testing

### Phase 2: ETL Enhancement
1. Update home_tile_reporting_etl.py with metadata join logic
2. Add data validation and error handling
3. Update target table schema with new column

### Phase 3: Testing and Validation
1. Unit tests for metadata join logic
2. Integration tests for end-to-end pipeline
3. Data quality validation for category mappings
4. Performance testing with production data volumes

### Phase 4: Deployment
1. Deploy enhanced ETL pipeline to staging environment
2. Execute full regression testing
3. Production deployment with monitoring
4. Post-deployment validation

## Testing Strategy

### Unit Tests
```python
def test_metadata_join_with_category():
    # Test successful metadata enrichment
    assert result_df.filter("tile_category != 'UNKNOWN'").count() > 0

def test_metadata_join_without_category():
    # Test default category assignment
    assert result_df.filter("tile_category = 'UNKNOWN'").count() >= 0

def test_backward_compatibility():
    # Test existing metrics unchanged
    assert old_counts == new_counts
```

### Integration Tests
1. End-to-end pipeline execution with sample data
2. Schema validation for target tables
3. Partition overwrite functionality
4. Data lineage verification

## Risk Assessment

### High Risk
- **Schema Drift**: Target table schema changes may impact downstream consumers
- **Performance Degradation**: Additional join may slow ETL execution

### Medium Risk
- **Data Quality**: Inconsistent category values in metadata table
- **Metadata Staleness**: Outdated metadata leading to incorrect categorization

### Low Risk
- **Join Logic**: Well-established left join pattern
- **Backward Compatibility**: Existing aggregation logic preserved

## References

### JIRA Stories
- [SCRUM-7567](https://aavademo.atlassian.net/browse/SCRUM-7567) - Add New Source Table & Extend Target Reporting Metrics

### Source Files
- `Input/home_tile_reporting_etl.py` - Current ETL implementation
- `Input/SourceDDL.sql` - Source table definitions
- `Input/TargetDDL.sql` - Target table definitions
- `Input/SOURCE_TILE_METADATA.sql` - New metadata table DDL

### Documentation
- Data Dictionary: Home Tile Reporting Tables
- ETL Pipeline Documentation
- Data Lineage Diagrams
- Business Requirements Document

---

**Document Status**: Draft  
**Review Required**: Data Architecture Team, BI Team, Product Analytics  
**Implementation Timeline**: 2-3 sprints  
**Dependencies**: Metadata table population, BI dashboard updates