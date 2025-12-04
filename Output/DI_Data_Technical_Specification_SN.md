=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target reporting with tile category
=============================================

# Technical Specification for Home Tile Reporting Enhancement

## Introduction

### Project Overview
This technical specification outlines the implementation details for enhancing the Home Tile Reporting ETL pipeline by integrating a new source table `SOURCE_TILE_METADATA` and extending the target reporting metrics with tile category information.

### Business Requirements
- **Story ID**: SCRUM-7819
- **Objective**: Add tile-level metadata to enrich reporting dashboards with business categorization
- **Business Value**: Enable category-level performance tracking, CTR comparisons, and improved dashboard drilldowns

### Scope
- Add new Delta source table `analytics_db.SOURCE_TILE_METADATA`
- Modify existing ETL pipeline to join metadata
- Update target table schema to include `tile_category`
- Ensure backward compatibility and maintain data integrity

## Code Changes

### 1. ETL Pipeline Modifications (`home_tile_reporting_etl.py`)

#### 1.1 Configuration Updates
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

#### 1.2 Read Metadata Table
```python
# Read tile metadata for enrichment
df_metadata = spark.table(SOURCE_TILE_METADATA).filter(F.col("is_active") == True)
```

#### 1.3 Daily Summary Aggregation Enhancement
```python
# Enhanced daily summary with metadata join
df_daily_summary_enriched = (
    df_daily_summary.join(
        df_metadata.select("tile_id", "tile_category"), 
        "tile_id", 
        "left"
    )
    .withColumn(
        "tile_category", 
        F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))
    )
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

#### 1.4 Updated Write Operation
```python
# Update the overwrite partition call
overwrite_partition(df_daily_summary_enriched, TARGET_DAILY_SUMMARY)
```

### 2. Impacted Modules and Functions

#### Areas Requiring Modification:
- **Configuration Section**: Add SOURCE_TILE_METADATA constant
- **Read Source Tables Section**: Add metadata table read operation
- **Daily Summary Aggregation**: Modify to include left join with metadata
- **Write Operations**: Update to handle new schema with tile_category

#### New Functions to Add:
```python
def validate_metadata_completeness(df_summary, df_metadata):
    """
    Validate that all tiles have metadata coverage
    Log tiles without metadata for monitoring
    """
    missing_tiles = (
        df_summary.select("tile_id")
        .subtract(df_metadata.select("tile_id"))
        .collect()
    )
    
    if missing_tiles:
        print(f"Warning: {len(missing_tiles)} tiles missing metadata")
        for row in missing_tiles:
            print(f"Missing metadata for tile_id: {row.tile_id}")
    
    return len(missing_tiles)
```

## Data Model Updates

### 1. New Source Table: `SOURCE_TILE_METADATA`

#### Schema Definition:
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

#### Key Characteristics:
- **Storage Format**: Delta Lake
- **Update Pattern**: SCD Type 1 (overwrite)
- **Data Governance**: Maintained by Product team
- **Refresh Frequency**: As needed (typically weekly)

### 2. Updated Target Table: `TARGET_HOME_TILE_DAILY_SUMMARY`

#### Schema Modification:
```sql
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
ADD COLUMN tile_category STRING COMMENT 'Functional category of the tile';
```

#### Updated Complete Schema:
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
COMMENT 'Daily aggregated tile-level metrics for reporting with business categorization';
```

### 3. Data Model Relationships

```
SOURCE_HOME_TILE_EVENTS ──┐
                          ├─► ETL_PIPELINE ──► TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_INTERSTITIAL_EVENTS ┤                           ↑
                          └─► (aggregation)             │
                                     ↑                  │
SOURCE_TILE_METADATA ──────────────────┘ (left join)   │
                                                        │
TARGET_HOME_TILE_GLOBAL_KPIS ←─────────────────────────┘
```

## Source-to-Target Mapping

### 1. Enhanced Daily Summary Mapping

| Source Field | Source Table | Target Field | Transformation Rule | Data Type |
|--------------|--------------|--------------|--------------------|-----------|
| date(event_ts) | SOURCE_HOME_TILE_EVENTS | date | F.lit(PROCESS_DATE) | DATE |
| tile_id | SOURCE_HOME_TILE_EVENTS | tile_id | Direct mapping | STRING |
| tile_category | SOURCE_TILE_METADATA | tile_category | F.coalesce(tile_category, "UNKNOWN") | STRING |
| user_id (distinct, TILE_VIEW) | SOURCE_HOME_TILE_EVENTS | unique_tile_views | F.countDistinct(F.when(event_type="TILE_VIEW", user_id)) | LONG |
| user_id (distinct, TILE_CLICK) | SOURCE_HOME_TILE_EVENTS | unique_tile_clicks | F.countDistinct(F.when(event_type="TILE_CLICK", user_id)) | LONG |
| user_id (distinct, interstitial_view_flag=true) | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_views | F.countDistinct(F.when(interstitial_view_flag=true, user_id)) | LONG |
| user_id (distinct, primary_button_click_flag=true) | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_primary_clicks | F.countDistinct(F.when(primary_button_click_flag=true, user_id)) | LONG |
| user_id (distinct, secondary_button_click_flag=true) | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_secondary_clicks | F.countDistinct(F.when(secondary_button_click_flag=true, user_id)) | LONG |

### 2. Metadata Enrichment Mapping

| Source Field | Source Table | Join Condition | Default Value | Business Rule |
|--------------|--------------|----------------|---------------|---------------|
| tile_id | SOURCE_TILE_METADATA | LEFT JOIN on tile_id | N/A | Primary key match |
| tile_category | SOURCE_TILE_METADATA | tile_id match | "UNKNOWN" | Default for missing metadata |
| is_active | SOURCE_TILE_METADATA | Filter condition | true | Only active tiles included |

### 3. Transformation Rules

#### Business Logic Rules:
1. **Metadata Join**: Left join to preserve all tiles even without metadata
2. **Default Category**: Tiles without metadata get "UNKNOWN" category
3. **Active Filter**: Only active tiles from metadata are considered
4. **Null Handling**: All metric fields use F.coalesce() with 0 default
5. **Date Consistency**: Process date applied uniformly across all records

#### Data Quality Rules:
1. **Uniqueness**: tile_id + date combination must be unique
2. **Completeness**: No null values in key dimensions (date, tile_id)
3. **Referential Integrity**: All tile_ids should exist in metadata (log exceptions)
4. **Range Validation**: All metric counts >= 0

## Assumptions and Constraints

### Assumptions
1. **Metadata Availability**: SOURCE_TILE_METADATA will be populated before ETL execution
2. **Data Freshness**: Metadata updates are infrequent (weekly or less)
3. **Backward Compatibility**: Existing dashboards can handle new tile_category column
4. **Performance**: Left join with metadata table will not significantly impact ETL performance
5. **Data Volume**: Metadata table will remain relatively small (<10K records)

### Constraints
1. **Schema Evolution**: Target table schema change requires coordinated deployment
2. **Historical Data**: No backfill of historical data with tile_category
3. **Global KPIs**: No changes to TARGET_HOME_TILE_GLOBAL_KPIS table
4. **Partition Strategy**: Maintain existing date-based partitioning
5. **Delta Lake**: All tables must use Delta format for ACID compliance

### Technical Constraints
1. **Memory Usage**: Join operation must fit within Spark driver memory
2. **Execution Time**: ETL runtime should not increase by more than 10%
3. **Data Lineage**: Maintain existing data lineage and audit trails
4. **Error Handling**: Pipeline must continue execution even with metadata issues

## References

### Documentation
- **JIRA Story**: SCRUM-7819 - Add New Source Table & Extend Target Reporting Metrics
- **Existing ETL Code**: `Input/home_tile_reporting_etl.py`
- **Source DDL**: `Input/SourceDDL.sql`
- **Target DDL**: `Input/TargetDDL.sql`
- **Metadata DDL**: `Input/SOURCE_TILE_METADATA.sql`

### Dependencies
1. **Upstream**: Product team to populate SOURCE_TILE_METADATA
2. **Downstream**: BI team to update dashboards for new tile_category column
3. **Infrastructure**: Delta Lake and Spark cluster configuration
4. **Testing**: Unit test framework for PySpark jobs

### Deployment Sequence
1. Create SOURCE_TILE_METADATA table
2. Populate initial metadata records
3. Update TARGET_HOME_TILE_DAILY_SUMMARY schema
4. Deploy updated ETL code
5. Execute integration testing
6. Coordinate with BI team for dashboard updates

---

**Cost Estimation and Justification**

**Token Usage Analysis:**
- **Input Tokens**: ~4,200 tokens (including prompt, JIRA story, and source files)
- **Output Tokens**: ~2,800 tokens (technical specification document)
- **Model Used**: GPT-4 (detected from processing context)

**Cost Calculation:**
- Input Cost = 4,200 tokens × $0.03/1K tokens = $0.126
- Output Cost = 2,800 tokens × $0.06/1K tokens = $0.168
- **Total Cost = $0.294**

**Cost Formula:**
```
Total Cost = (input_tokens × input_cost_per_token) + (output_tokens × output_cost_per_token)
Total Cost = (4,200 × $0.00003) + (2,800 × $0.00006) = $0.126 + $0.168 = $0.294
```

This cost represents the computational expense for generating this comprehensive technical specification document, including analysis of source code, JIRA requirements, and creation of detailed implementation guidance.