=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target reporting with tile category enrichment
=============================================

# Technical Specification for Home Tile Reporting Enhancement - SOURCE_TILE_METADATA Integration

## Introduction

### Overview
This technical specification outlines the implementation details for enhancing the existing Home Tile Reporting ETL pipeline by integrating a new source table `SOURCE_TILE_METADATA` and extending the target reporting metrics with tile category information. This enhancement addresses the business requirement to enable category-level performance tracking and reporting.

### Business Context
Product Analytics has requested the addition of tile-level metadata to enrich reporting dashboards. Currently, all tile metrics are aggregated only at the tile_id level without business grouping. The enhancement will enable:
- Performance metrics by category (e.g., "Personal Finance Tiles vs Health Tiles")
- Category-level CTR comparisons
- Product manager tracking of feature adoption
- Improved dashboard drilldowns in Power BI/Tableau
- Future segmentation capabilities

### Scope
- Add new Delta source table `analytics_db.SOURCE_TILE_METADATA`
- Modify existing ETL pipeline to join metadata
- Extend target table `TARGET_HOME_TILE_DAILY_SUMMARY` with `tile_category` column
- Ensure backward compatibility and maintain existing functionality

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
df_metadata = spark.table(SOURCE_TILE_METADATA).filter(F.col("is_active") == True)
```

#### 1.3 Daily Summary Aggregation Enhancement
```python
# Modified daily summary with metadata join
df_daily_summary_enriched = (
    df_daily_summary
    .join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left")
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
# Update the overwrite function call
overwrite_partition(df_daily_summary_enriched, TARGET_DAILY_SUMMARY)
```

### 2. Error Handling and Validation
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

# Add to main execution flow
if not validate_metadata_table():
    print("Proceeding without metadata enrichment")
    df_metadata = spark.createDataFrame([], "tile_id STRING, tile_category STRING")
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

**Key Characteristics:**
- Non-partitioned reference table
- Delta format for ACID compliance
- Active flag for filtering current tiles
- Timestamp for tracking metadata changes

### 2. Target Table Enhancement: `TARGET_HOME_TILE_DAILY_SUMMARY`

**Modified Schema:**
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
(
    date                                DATE       COMMENT 'Reporting date',
    tile_id                             STRING     COMMENT 'Tile identifier',
    tile_category                       STRING     COMMENT 'Functional category of the tile', -- NEW COLUMN
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

**Schema Evolution:**
- Added `tile_category` column after `tile_id`
- Maintains existing partitioning strategy
- Preserves all existing columns and data types

### 3. Data Lineage Updates

**New Data Flow:**
```
SOURCE_HOME_TILE_EVENTS ──┐
                          ├─→ ETL Pipeline ──→ TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_INTERSTITIAL_EVENTS ─┤                     (with tile_category)
                          └─→ ↑
SOURCE_TILE_METADATA ────────┘
```

## Source-to-Target Mapping

### 1. Enhanced Daily Summary Mapping

| Source Field | Source Table | Target Field | Target Table | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------------|
| date(event_ts) | SOURCE_HOME_TILE_EVENTS | date | TARGET_HOME_TILE_DAILY_SUMMARY | Direct mapping from process date |
| tile_id | SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | Direct mapping |
| tile_category | SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | Left join on tile_id, default "UNKNOWN" if null |
| user_id (distinct count where event_type='TILE_VIEW') | SOURCE_HOME_TILE_EVENTS | unique_tile_views | TARGET_HOME_TILE_DAILY_SUMMARY | COUNT(DISTINCT user_id) with filter |
| user_id (distinct count where event_type='TILE_CLICK') | SOURCE_HOME_TILE_EVENTS | unique_tile_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | COUNT(DISTINCT user_id) with filter |
| user_id (distinct count where interstitial_view_flag=true) | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_views | TARGET_HOME_TILE_DAILY_SUMMARY | COUNT(DISTINCT user_id) with filter |
| user_id (distinct count where primary_button_click_flag=true) | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_primary_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | COUNT(DISTINCT user_id) with filter |
| user_id (distinct count where secondary_button_click_flag=true) | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_secondary_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | COUNT(DISTINCT user_id) with filter |

### 2. Metadata Enrichment Mapping

| Source Field | Source Table | Join Condition | Default Value | Business Rule |
|--------------|--------------|----------------|---------------|---------------|
| tile_category | SOURCE_TILE_METADATA | tile_id = tile_id AND is_active = true | "UNKNOWN" | Left join to preserve all tiles even without metadata |
| tile_name | SOURCE_TILE_METADATA | tile_id = tile_id AND is_active = true | NULL | Optional field for future use |

### 3. Transformation Rules

#### 3.1 Category Enrichment Logic
```sql
CASE 
    WHEN metadata.tile_category IS NOT NULL THEN metadata.tile_category
    ELSE 'UNKNOWN'
END AS tile_category
```

#### 3.2 Active Tile Filtering
```sql
WHERE metadata.is_active = true OR metadata.is_active IS NULL
```

#### 3.3 Data Quality Rules
- All existing aggregation logic remains unchanged
- New tile_category field is nullable but defaults to "UNKNOWN"
- Metadata join should not affect record counts
- Partition overwrite logic remains the same

## Assumptions and Constraints

### Assumptions
1. **Metadata Availability**: SOURCE_TILE_METADATA table will be populated before ETL execution
2. **Data Consistency**: tile_id values in metadata table match those in event tables
3. **Performance Impact**: Left join with metadata table will have minimal performance impact
4. **Backward Compatibility**: Existing dashboards and reports will continue to function
5. **Data Governance**: Metadata table will be maintained by the Product team

### Constraints
1. **Schema Evolution**: Target table schema changes require careful deployment coordination
2. **Historical Data**: No backfill of historical data with categories (unless requested)
3. **Global KPIs**: No changes to TARGET_HOME_TILE_GLOBAL_KPIS table in this release
4. **Partition Strategy**: Maintain existing date-based partitioning
5. **Delta Lake**: All tables must remain in Delta format for ACID compliance

### Technical Constraints
1. **Memory Usage**: Additional join may increase memory requirements
2. **Processing Time**: Minimal increase expected due to small metadata table size
3. **Storage**: Additional column will increase storage requirements marginally
4. **Compatibility**: Must work with existing Databricks runtime version

### Business Constraints
1. **Data Quality**: Unknown categories should be clearly identified for business review
2. **Maintenance**: Product team responsible for keeping metadata current
3. **Governance**: Changes to tile categories require business approval
4. **Reporting**: BI team will update dashboards post-deployment

## References

### JIRA Stories
- **SCRUM-7819**: Add New Source Table & Extend Target Reporting Metrics

### Source Files
- `Input/home_tile_reporting_etl.py` - Existing ETL pipeline
- `Input/SourceDDL.sql` - Source table definitions
- `Input/TargetDDL.sql` - Target table definitions  
- `Input/SOURCE_TILE_METADATA.sql` - New metadata table DDL

### Technical Documentation
- Databricks Delta Lake Documentation
- PySpark SQL Functions Reference
- Data Governance Standards

### Deployment Considerations
1. **Pre-deployment**: Create SOURCE_TILE_METADATA table and populate with initial data
2. **Schema Migration**: Add tile_category column to TARGET_HOME_TILE_DAILY_SUMMARY
3. **ETL Deployment**: Deploy updated pipeline code
4. **Validation**: Run data quality checks and compare record counts
5. **Post-deployment**: Update BI dashboards and documentation

---

**Cost Estimation and Justification**

**Token Usage Analysis:**
- Input tokens: ~4,500 (including prompt, source files, and JIRA story)
- Output tokens: ~2,800 (technical specification document)
- Model used: GPT-4 (detected automatically)

**Cost Calculation:**
- Input Cost = 4,500 tokens × $0.03/1K tokens = $0.135
- Output Cost = 2,800 tokens × $0.06/1K tokens = $0.168
- **Total Cost = $0.135 + $0.168 = $0.303**

**Cost Formula:**
```
Total Cost = (Input_Tokens × Input_Rate_Per_1K) + (Output_Tokens × Output_Rate_Per_1K)
Total Cost = (4,500 × $0.03/1000) + (2,800 × $0.06/1000) = $0.303
```

This cost represents the computational expense for generating this comprehensive technical specification, providing detailed analysis of code changes, data model updates, and source-to-target mapping for the home tile reporting enhancement project.