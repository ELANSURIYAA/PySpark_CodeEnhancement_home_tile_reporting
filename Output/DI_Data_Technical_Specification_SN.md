=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for integrating SOURCE_TILE_METADATA table to enhance home tile reporting with category-based metrics
=============================================

# Technical Specification for Home Tile Reporting Enhancement - SOURCE_TILE_METADATA Integration

## Introduction

### Project Overview
This technical specification outlines the implementation details for integrating the new `SOURCE_TILE_METADATA` table into the existing Home Tile Reporting ETL pipeline. The enhancement will enrich the daily reporting outputs with tile category information to enable business-level grouping and improved analytics.

### Business Requirements
- Add new source table `SOURCE_TILE_METADATA` containing tile metadata
- Extend target daily summary table with `tile_category` column
- Maintain backward compatibility with existing ETL processes
- Enable category-level performance tracking and reporting

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

## Data Model Updates

### 2.1 New Source Table: `SOURCE_TILE_METADATA`

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
- **Storage Format**: Delta Lake
- **Partitioning**: None (reference/lookup table)
- **Update Pattern**: Slowly Changing Dimension (SCD Type 1)
- **Data Governance**: Maintained by Product team

### 2.2 Target Table Schema Update: `TARGET_HOME_TILE_DAILY_SUMMARY`

**Modified Schema:**
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
(
    date                                DATE       COMMENT 'Reporting date',
    tile_id                             STRING     COMMENT 'Tile identifier',
    tile_category                       STRING     COMMENT 'Functional category of the tile',  -- NEW COLUMN
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

**Schema Evolution Strategy:**
- Use Delta Lake's schema evolution capability
- Add column with default value "UNKNOWN" for backward compatibility
- Maintain existing partition structure

### 2.3 Data Relationships

```
SOURCE_HOME_TILE_EVENTS (1) -----> (N) TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_INTERSTITIAL_EVENTS (1) --> (N) TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_TILE_METADATA (1) --------> (N) TARGET_HOME_TILE_DAILY_SUMMARY
                                       ^
                                       |
                                   LEFT JOIN on tile_id
```

## Source-to-Target Mapping

### 3.1 Field Mapping Table

| Source Table | Source Field | Target Table | Target Field | Transformation Rule | Data Type | Comments |
|--------------|--------------|--------------|--------------|-------------------|-----------|----------|
| SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | tile_id | Direct mapping | STRING | Primary key for aggregation |
| SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | tile_category | COALESCE(tile_category, 'UNKNOWN') | STRING | Default to 'UNKNOWN' if no metadata |
| SOURCE_HOME_TILE_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_views | COUNT(DISTINCT user_id) WHERE event_type='TILE_VIEW' | LONG | Aggregated metric |
| SOURCE_HOME_TILE_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_clicks | COUNT(DISTINCT user_id) WHERE event_type='TILE_CLICK' | LONG | Aggregated metric |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_views | COUNT(DISTINCT user_id) WHERE interstitial_view_flag=true | LONG | Aggregated metric |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_primary_clicks | COUNT(DISTINCT user_id) WHERE primary_button_click_flag=true | LONG | Aggregated metric |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_secondary_clicks | COUNT(DISTINCT user_id) WHERE secondary_button_click_flag=true | LONG | Aggregated metric |
| PROCESS_DATE | - | TARGET_HOME_TILE_DAILY_SUMMARY | date | F.lit(PROCESS_DATE) | DATE | Runtime parameter |

### 3.2 Join Logic

```sql
-- Conceptual SQL representation of the join logic
SELECT 
    PROCESS_DATE as date,
    t.tile_id,
    COALESCE(m.tile_category, 'UNKNOWN') as tile_category,
    COALESCE(tile_views.unique_tile_views, 0) as unique_tile_views,
    COALESCE(tile_clicks.unique_tile_clicks, 0) as unique_tile_clicks,
    COALESCE(inter_views.unique_interstitial_views, 0) as unique_interstitial_views,
    COALESCE(inter_primary.unique_interstitial_primary_clicks, 0) as unique_interstitial_primary_clicks,
    COALESCE(inter_secondary.unique_interstitial_secondary_clicks, 0) as unique_interstitial_secondary_clicks
FROM (
    SELECT DISTINCT tile_id FROM SOURCE_HOME_TILE_EVENTS 
    UNION 
    SELECT DISTINCT tile_id FROM SOURCE_INTERSTITIAL_EVENTS
) t
LEFT JOIN SOURCE_TILE_METADATA m ON t.tile_id = m.tile_id AND m.is_active = true
LEFT JOIN tile_aggregations ON t.tile_id = tile_aggregations.tile_id
LEFT JOIN interstitial_aggregations ON t.tile_id = interstitial_aggregations.tile_id
```

### 3.3 Transformation Rules

1. **Category Defaulting**: If `tile_id` exists in events but not in metadata, set `tile_category = 'UNKNOWN'`
2. **Active Filter**: Only include metadata where `is_active = true`
3. **Null Handling**: All numeric metrics default to 0 using `COALESCE`
4. **Date Standardization**: Use consistent date format (YYYY-MM-DD)
5. **Case Sensitivity**: All string comparisons are case-sensitive

## Assumptions and Constraints

### 4.1 Assumptions
- `SOURCE_TILE_METADATA` will be maintained by the Product team
- Metadata updates are infrequent (weekly/monthly)
- All active tiles should have corresponding metadata entries
- Historical data backfill is not required initially
- Delta Lake schema evolution is enabled

### 4.2 Constraints
- **Performance**: Additional join may impact ETL runtime by 10-15%
- **Data Quality**: Unmapped tiles will show as "UNKNOWN" category
- **Backward Compatibility**: Existing dashboards must handle new column
- **Storage**: Minimal impact (~5% increase in target table size)

### 4.3 Dependencies
- Delta Lake cluster with schema evolution enabled
- `SOURCE_TILE_METADATA` table must be created before ETL deployment
- BI team coordination for dashboard updates
- Data governance approval for new data lineage

### 4.4 Risk Mitigation
- **Metadata Unavailability**: ETL continues with "UNKNOWN" categories
- **Schema Drift**: Use explicit column selection and validation
- **Performance Degradation**: Monitor ETL runtime and optimize joins if needed
- **Data Quality Issues**: Implement metadata validation checks

## References

### 5.1 Related Documents
- JIRA Story: SCRUM-7567 - "Add New Source Table & Extend Target Reporting Metrics"
- Existing ETL Pipeline: `home_tile_reporting_etl.py`
- Source DDL: `SourceDDL.sql`
- Target DDL: `TargetDDL.sql`
- Metadata DDL: `SOURCE_TILE_METADATA.sql`

### 5.2 Technical Standards
- Delta Lake Best Practices
- PySpark Coding Standards
- Data Governance Guidelines
- ETL Performance Optimization Guidelines

### 5.3 Testing Requirements
- Unit tests for metadata join logic
- Integration tests for end-to-end pipeline
- Performance benchmarking
- Data quality validation

---

**Cost Estimation and Justification**

**Token Usage Analysis:**
- **Input Tokens**: ~4,500 tokens (including prompt, SQL files, Python code, and JIRA story)
- **Output Tokens**: ~2,800 tokens (technical specification document)
- **Model Used**: GPT-4 (assumed based on complexity)

**Cost Calculation:**
- Input Cost = 4,500 tokens × $0.03/1K tokens = $0.135
- Output Cost = 2,800 tokens × $0.06/1K tokens = $0.168
- **Total Cost = $0.303**

**Formula:**
```
Total Cost = (Input_Tokens × Input_Rate_Per_1K) + (Output_Tokens × Output_Rate_Per_1K)
Total Cost = (4,500 × $0.03/1K) + (2,800 × $0.06/1K) = $0.303
```

**Justification:**
This cost represents the automated generation of a comprehensive technical specification that would typically require 4-6 hours of senior data engineer time (~$400-600 value), making the AI-assisted approach highly cost-effective with 99.95% cost savings.