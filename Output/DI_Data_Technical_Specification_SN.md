=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target reporting metrics with tile category
=============================================

# Technical Specification for Home Tile Reporting Enhancement

## Introduction

### Business Context
This enhancement addresses the business requirement to add tile-level metadata enrichment to the existing Home Tile Reporting ETL pipeline. The Product Analytics team has requested the addition of tile category information to enable better reporting and performance tracking at a functional level (e.g., Offers, Health Checks, Payments).

### Objectives
- Add new source table `SOURCE_TILE_METADATA` to store tile metadata
- Extend the existing ETL pipeline to incorporate tile category information
- Update target table schema to include `tile_category` column
- Maintain backward compatibility and ensure no disruption to existing metrics

### Scope
- **In Scope**: New source table creation, ETL pipeline modifications, target table schema updates
- **Out of Scope**: Global KPI table changes, historical data backfill, dashboard UI updates

## Code Changes

### 1. ETL Pipeline Modifications (`home_tile_reporting_etl.py`)

#### 1.1 Configuration Updates
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

#### 1.2 Data Reading Section
```python
# Read tile metadata table
df_metadata = spark.table(SOURCE_TILE_METADATA).filter(F.col("is_active") == True)
```

#### 1.3 Daily Summary Aggregation Enhancement
```python
# Enhanced daily summary with metadata join
df_daily_summary_enriched = (
    df_daily_summary
    .join(df_metadata.select("tile_id", "tile_category", "tile_name"), "tile_id", "left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
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

#### 1.4 Updated Write Logic
```python
# Update the overwrite_partition function call
overwrite_partition(df_daily_summary_enriched, TARGET_DAILY_SUMMARY)
```

### 2. Error Handling and Data Quality
```python
# Add data quality checks
def validate_metadata_join(df):
    """
    Validate that metadata join doesn't cause data loss
    """
    original_count = df_daily_summary.count()
    enriched_count = df.count()
    
    if original_count != enriched_count:
        raise Exception(f"Data loss detected: Original={original_count}, Enriched={enriched_count}")
    
    return df

# Apply validation
df_daily_summary_enriched = validate_metadata_join(df_daily_summary_enriched)
```

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

**Key Characteristics:**
- **Primary Key**: tile_id
- **Data Type**: Delta table
- **Update Pattern**: SCD Type 1 (overwrite)
- **Data Volume**: Low (estimated < 1000 records)

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
    tile_category                       STRING     COMMENT 'Functional category of the tile',  -- NEW
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

## Source-to-Target Mapping

### 1. Data Flow Architecture

```
SOURCE_HOME_TILE_EVENTS ──┐
                          ├─► ETL Pipeline ─► TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_INTERSTITIAL_EVENTS ┤
                          │
SOURCE_TILE_METADATA ─────┘
```

### 2. Field Mapping Table

| Source Table | Source Field | Target Table | Target Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------------|
| SOURCE_HOME_TILE_EVENTS | event_ts | TARGET_HOME_TILE_DAILY_SUMMARY | date | `to_date(event_ts)` |
| SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | tile_id | Direct mapping |
| SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | tile_category | Left join on tile_id, default "UNKNOWN" |
| SOURCE_HOME_TILE_EVENTS | user_id (TILE_VIEW) | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_views | `countDistinct(user_id) WHERE event_type='TILE_VIEW'` |
| SOURCE_HOME_TILE_EVENTS | user_id (TILE_CLICK) | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_clicks | `countDistinct(user_id) WHERE event_type='TILE_CLICK'` |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_views | `countDistinct(user_id) WHERE interstitial_view_flag=true` |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_primary_clicks | `countDistinct(user_id) WHERE primary_button_click_flag=true` |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_secondary_clicks | `countDistinct(user_id) WHERE secondary_button_click_flag=true` |

### 3. Transformation Rules

#### 3.1 Metadata Enrichment Logic
```sql
-- Pseudocode for tile category enrichment
SELECT 
    daily_summary.*,
    COALESCE(metadata.tile_category, 'UNKNOWN') as tile_category
FROM daily_summary 
LEFT JOIN metadata ON daily_summary.tile_id = metadata.tile_id 
    AND metadata.is_active = true
```

#### 3.2 Data Quality Rules
- **Null Handling**: tile_category defaults to "UNKNOWN" when no metadata exists
- **Join Integrity**: Left join ensures no data loss from original aggregation
- **Active Filter**: Only active tiles from metadata are considered

### 4. Sample Data Transformation

**Before Enhancement:**
```
date       | tile_id | unique_tile_views | unique_tile_clicks
2025-12-01 | TILE_001| 1500             | 150
2025-12-01 | TILE_002| 2000             | 300
```

**After Enhancement:**
```
date       | tile_id | tile_category | unique_tile_views | unique_tile_clicks
2025-12-01 | TILE_001| OFFERS       | 1500             | 150
2025-12-01 | TILE_002| HEALTH       | 2000             | 300
```

## Assumptions and Constraints

### Assumptions
1. **Data Availability**: SOURCE_TILE_METADATA will be populated before ETL execution
2. **Data Quality**: tile_id in metadata table matches tile_id in event tables
3. **Performance**: Metadata table size remains small (< 1000 records)
4. **Backward Compatibility**: Existing downstream consumers can handle new column

### Constraints
1. **Schema Evolution**: Target table schema change requires coordination with BI team
2. **Data Retention**: No historical backfill planned for tile_category
3. **Performance Impact**: Additional join operation may slightly increase processing time
4. **Dependency**: ETL pipeline depends on metadata table availability

### Technical Constraints
1. **Delta Lake Compatibility**: All tables must support Delta format
2. **Partition Strategy**: Maintain existing date-based partitioning
3. **Idempotency**: Pipeline must support reprocessing without data duplication

## Implementation Plan

### Phase 1: Infrastructure Setup
1. Create SOURCE_TILE_METADATA table
2. Populate initial metadata records
3. Update table permissions and access controls

### Phase 2: ETL Enhancement
1. Modify home_tile_reporting_etl.py
2. Add metadata reading logic
3. Update aggregation and join logic
4. Implement data quality validations

### Phase 3: Target Schema Update
1. Add tile_category column to TARGET_HOME_TILE_DAILY_SUMMARY
2. Update write operations
3. Test schema compatibility

### Phase 4: Testing and Validation
1. Unit testing for new functionality
2. Integration testing with sample data
3. Performance testing
4. Backward compatibility validation

## References

### JIRA Stories
- **SCRUM-7819**: Add New Source Table & Extend Target Reporting Metrics

### Source Files
- `Input/home_tile_reporting_etl.py`: Current ETL implementation
- `Input/SourceDDL.sql`: Source table definitions
- `Input/TargetDDL.sql`: Target table definitions
- `Input/SOURCE_TILE_METADATA.sql`: New metadata table definition

### Technical Documentation
- Delta Lake Documentation
- PySpark SQL Functions Reference
- Data Pipeline Best Practices Guide

---

**Cost Estimation and Justification**

**Token Usage Analysis:**
- **Input Tokens**: ~4,500 tokens (including prompt, SQL files, Python code, and JIRA content)
- **Output Tokens**: ~2,800 tokens (technical specification document)
- **Model Used**: GPT-4 (detected from processing capabilities)

**Cost Calculation:**
- Input Cost = 4,500 tokens × $0.03/1K tokens = $0.135
- Output Cost = 2,800 tokens × $0.06/1K tokens = $0.168
- **Total Cost = $0.303**

**Cost Breakdown Formula:**
```
Total Cost = (Input_Tokens × Input_Rate) + (Output_Tokens × Output_Rate)
Total Cost = (4,500 × $0.00003) + (2,800 × $0.00006)
Total Cost = $0.135 + $0.168 = $0.303
```

*Note: Pricing based on GPT-4 standard rates as of December 2024. Actual costs may vary based on specific model version and pricing updates.*