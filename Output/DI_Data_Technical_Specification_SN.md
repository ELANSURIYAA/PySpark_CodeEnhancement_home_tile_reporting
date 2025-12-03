=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target reporting metrics with tile category
=============================================

# Technical Specification for Home Tile Reporting Enhancement - Add SOURCE_TILE_METADATA

## Introduction

### Purpose
This technical specification outlines the implementation details for adding a new source table `SOURCE_TILE_METADATA` and extending the existing home tile reporting pipeline to include tile category information in the target reporting tables.

### Business Context
Product Analytics has requested the addition of tile-level metadata to enrich reporting dashboards. Currently, all tile metrics are aggregated only at tile_id level with no business grouping. The enhancement will enable:
- Performance tracking at functional level (Offers, Health Checks, Payments)
- Category-level CTR comparisons
- Product manager tracking of feature adoption
- Improved dashboard drilldowns in Power BI/Tableau

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

#### 1.2 Read Source Tables Section
```python
# Add metadata table read after existing source reads
df_metadata = (
    spark.table(SOURCE_TILE_METADATA)
    .filter(F.col("is_active") == True)
    .select("tile_id", "tile_name", "tile_category")
)
```

#### 1.3 Daily Summary Aggregation Enhancement
```python
# Modify the final daily summary join to include metadata
df_daily_summary = (
    df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    .join(df_metadata, "tile_id", "left")  # Left join to preserve all tiles
    .withColumn("date", F.lit(PROCESS_DATE))
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
    .select(
        "date",
        "tile_id",
        "tile_category",  # New column
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
    )
)
```

#### 1.4 Error Handling and Logging
```python
# Add validation for metadata table availability
def validate_metadata_table():
    try:
        metadata_count = spark.table(SOURCE_TILE_METADATA).count()
        print(f"Metadata table contains {metadata_count} records")
        return True
    except Exception as e:
        print(f"Warning: Metadata table not available: {e}")
        print("Proceeding with UNKNOWN category for all tiles")
        return False

# Call validation before processing
metadata_available = validate_metadata_table()
```

### 2. Schema Evolution Handling
```python
# Add schema evolution support for target table
def ensure_schema_compatibility(df, table_name):
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.autoMerge.enabled' = 'true')")
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
```

## Data Model Updates

### 1. New Source Table: SOURCE_TILE_METADATA

**Table:** `analytics_db.SOURCE_TILE_METADATA`

| Column Name | Data Type | Constraints | Description |
|-------------|-----------|-------------|-------------|
| tile_id | STRING | NOT NULL, PK | Tile identifier |
| tile_name | STRING | NOT NULL | User-friendly tile name |
| tile_category | STRING | NOT NULL | Business/functional category |
| is_active | BOOLEAN | NOT NULL | Active status flag |
| updated_ts | TIMESTAMP | NOT NULL | Last update timestamp |

**Business Rules:**
- Primary key: tile_id
- Only active tiles (is_active = true) should be used in reporting
- Default category: "UNKNOWN" for unmapped tiles

### 2. Target Table Schema Updates

**Table:** `reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY`

**New Column Addition:**
```sql
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
ADD COLUMN tile_category STRING COMMENT 'Functional category of the tile'
AFTER tile_id;
```

**Updated Schema:**
| Column Name | Data Type | Description | Change Type |
|-------------|-----------|-------------|-------------|
| date | DATE | Reporting date | Existing |
| tile_id | STRING | Tile identifier | Existing |
| tile_category | STRING | Functional category | **NEW** |
| unique_tile_views | LONG | Distinct users who viewed | Existing |
| unique_tile_clicks | LONG | Distinct users who clicked | Existing |
| unique_interstitial_views | LONG | Distinct interstitial views | Existing |
| unique_interstitial_primary_clicks | LONG | Distinct primary clicks | Existing |
| unique_interstitial_secondary_clicks | LONG | Distinct secondary clicks | Existing |

## Source-to-Target Mapping

### 1. SOURCE_TILE_METADATA to TARGET_HOME_TILE_DAILY_SUMMARY

| Source Table | Source Column | Target Table | Target Column | Transformation Rule |
|--------------|---------------|--------------|---------------|--------------------|
| SOURCE_TILE_METADATA | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | tile_id | Direct mapping (JOIN key) |
| SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | tile_category | COALESCE(tile_category, 'UNKNOWN') |
| SOURCE_TILE_METADATA | is_active | - | - | Filter condition (is_active = true) |

### 2. Enhanced Data Flow Mapping

```
SOURCE_HOME_TILE_EVENTS ──┐
                          ├── JOIN (tile_id) ──┐
SOURCE_INTERSTITIAL_EVENTS ┘                   │
                                               ├── LEFT JOIN (tile_id) ──> TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_TILE_METADATA ──────────────────────────┘
(filtered: is_active = true)
```

### 3. Transformation Rules

| Business Rule | Implementation |
|---------------|----------------|
| Default Category | `COALESCE(metadata.tile_category, 'UNKNOWN')` |
| Active Tiles Only | `WHERE metadata.is_active = true` |
| Preserve All Tiles | `LEFT JOIN` ensures tiles without metadata are retained |
| Backward Compatibility | Existing metrics calculations remain unchanged |

### 4. Data Quality Rules

| Rule | Validation Logic |
|------|------------------|
| No NULL categories | `tile_category IS NOT NULL` |
| Valid tile_id mapping | `tile_id IN (SELECT DISTINCT tile_id FROM SOURCE_HOME_TILE_EVENTS)` |
| Record count consistency | `COUNT(*) matches pre-enhancement counts` |

## Assumptions and Constraints

### Assumptions
1. **Data Availability**: SOURCE_TILE_METADATA will be populated before ETL execution
2. **Referential Integrity**: All tile_ids in event tables should have corresponding metadata entries
3. **Schema Evolution**: Target table supports schema evolution without breaking existing queries
4. **Performance**: LEFT JOIN with metadata table will not significantly impact ETL performance
5. **Data Freshness**: Metadata table is updated in near real-time or daily before ETL runs

### Constraints
1. **Backward Compatibility**: Existing dashboards and queries must continue to work
2. **No Historical Backfill**: Historical data will not be updated with categories (unless requested)
3. **Global KPIs Unchanged**: TARGET_HOME_TILE_GLOBAL_KPIS table remains unmodified
4. **Default Category**: Tiles without metadata will show "UNKNOWN" category
5. **Performance SLA**: ETL runtime should not increase by more than 10%

### Technical Constraints
1. **Delta Lake Compatibility**: All tables use Delta format
2. **Partition Strategy**: Maintain existing partitioning by date
3. **Schema Registry**: New table must be registered in data catalog
4. **Access Control**: Maintain existing security permissions

## Implementation Plan

### Phase 1: Infrastructure Setup
1. Create SOURCE_TILE_METADATA table
2. Insert sample metadata for testing
3. Update schema registry

### Phase 2: ETL Enhancement
1. Modify PySpark ETL code
2. Add metadata join logic
3. Update target table schema
4. Implement error handling

### Phase 3: Testing & Validation
1. Unit testing for new functionality
2. Integration testing with sample data
3. Performance testing
4. Regression testing

### Phase 4: Deployment
1. Deploy to development environment
2. User acceptance testing
3. Production deployment
4. Monitoring and validation

## Testing Strategy

### Unit Tests
```python
def test_metadata_join():
    # Test successful metadata join
    # Test default category assignment
    # Test active filter application
    pass

def test_schema_compatibility():
    # Test target table schema evolution
    # Test backward compatibility
    pass

def test_data_quality():
    # Test record count consistency
    # Test null handling
    pass
```

### Integration Tests
- End-to-end pipeline execution
- Data validation against business rules
- Performance benchmarking

## References

1. **JIRA Story**: SCRUM-7567 - "Add New Source Table & Extend Target Reporting Metrics"
2. **Source Files**:
   - `Input/home_tile_reporting_etl.py` - Current ETL implementation
   - `Input/SourceDDL.sql` - Source table schemas
   - `Input/TargetDDL.sql` - Target table schemas
   - `Input/SOURCE_TILE_METADATA.sql` - New metadata table DDL
3. **Business Requirements**: Product Analytics dashboard enhancement request
4. **Technical Standards**: Delta Lake, PySpark, Databricks platform standards

---

**Document Control**
- Version: 1.0
- Status: Draft
- Review Required: Technical Lead, Data Architect
- Approval Required: Product Owner, Engineering Manager