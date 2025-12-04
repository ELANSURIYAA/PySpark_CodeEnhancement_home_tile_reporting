=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for integrating SOURCE_TILE_METADATA table to enhance home tile reporting with category-based metrics
=============================================

# Technical Specification for Home Tile Reporting Enhancement - Metadata Integration

## Introduction

### Purpose
This technical specification outlines the implementation details for integrating the new `SOURCE_TILE_METADATA` table into the existing home tile reporting ETL pipeline. The enhancement will enrich the daily summary reports with tile category information to enable business-level grouping and analysis.

### Business Context
Product Analytics has requested the addition of tile-level metadata to support:
- Performance tracking by functional categories (Offers, Health Checks, Payments)
- Category-level CTR comparisons
- Enhanced dashboard capabilities in Power BI/Tableau
- Future tile management by category

### Scope
- Integration of `SOURCE_TILE_METADATA` table into existing ETL pipeline
- Enhancement of `TARGET_HOME_TILE_DAILY_SUMMARY` table with `tile_category` column
- Backward compatibility maintenance
- Data quality and validation improvements

## Code Changes

### 1. ETL Pipeline Modifications

#### 1.1 Configuration Updates
**File:** `home_tile_reporting_etl.py`
**Section:** Configuration

```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

#### 1.2 Source Data Reading
**Location:** After existing source table reads

```python
# ------------------------------------------------------------------------------
# READ METADATA TABLE
# ------------------------------------------------------------------------------
df_metadata = (
    spark.table(SOURCE_TILE_METADATA)
    .filter(F.col("is_active") == True)
    .select("tile_id", "tile_name", "tile_category")
)
```

#### 1.3 Daily Summary Aggregation Enhancement
**Location:** Replace existing `df_daily_summary` creation

```python
# ------------------------------------------------------------------------------
# ENHANCED DAILY TILE SUMMARY AGGREGATION WITH METADATA
# ------------------------------------------------------------------------------
df_daily_summary_base = (
    df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    .withColumn("date", F.lit(PROCESS_DATE))
    .select(
        "date",
        "tile_id",
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
    )
)

# Join with metadata to enrich with tile_category
df_daily_summary = (
    df_daily_summary_base.join(df_metadata, "tile_id", "left")
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

#### 1.4 Error Handling and Logging
**Location:** Add after metadata read

```python
# Data quality checks
metadata_count = df_metadata.count()
tile_events_distinct = df_tile.select("tile_id").distinct().count()

print(f"Metadata records loaded: {metadata_count}")
print(f"Distinct tiles in events: {tile_events_distinct}")

if metadata_count == 0:
    print("WARNING: No metadata records found. All tiles will have UNKNOWN category.")
```

### 2. Schema Validation

#### 2.1 Pre-execution Validation
```python
# Validate metadata table schema
expected_metadata_columns = ["tile_id", "tile_name", "tile_category", "is_active", "updated_ts"]
actual_metadata_columns = df_metadata.columns

for col in expected_metadata_columns:
    if col not in actual_metadata_columns:
        raise ValueError(f"Missing required column in metadata table: {col}")
```

## Data Model Updates

### 1. Source Data Model Enhancement

#### New Table: `analytics_db.SOURCE_TILE_METADATA`
- **Purpose:** Master metadata for homepage tiles
- **Partitioning:** None (small lookup table)
- **Storage Format:** Delta
- **Update Pattern:** SCD Type 1 (overwrite)

### 2. Target Data Model Enhancement

#### Modified Table: `reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY`
**Schema Changes:**

| Column Name | Data Type | Position | Change Type | Comment |
|-------------|-----------|----------|-------------|----------|
| tile_category | STRING | After tile_id | ADD | Functional category of the tile |

**Updated DDL:**
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
COMMENT 'Daily aggregated tile-level metrics with category enrichment for reporting';
```

### 3. Data Lineage Impact

```
SOURCE_HOME_TILE_EVENTS ──┐
                          ├─► TARGET_HOME_TILE_DAILY_SUMMARY (Enhanced)
SOURCE_INTERSTITIAL_EVENTS ──┤
                          │
SOURCE_TILE_METADATA ─────┘

SOURCE_HOME_TILE_EVENTS ──┐
                          ├─► TARGET_HOME_TILE_GLOBAL_KPIS (Unchanged)
SOURCE_INTERSTITIAL_EVENTS ──┘
```

## Source-to-Target Mapping

### 1. Enhanced Daily Summary Mapping

| Source Table | Source Column | Target Table | Target Column | Transformation Rule | Data Type | Nullable |
|--------------|---------------|--------------|---------------|-------------------|-----------|----------|
| - | - | TARGET_HOME_TILE_DAILY_SUMMARY | date | F.lit(PROCESS_DATE) | DATE | No |
| SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | tile_id | Direct mapping | STRING | No |
| SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | tile_category | F.coalesce("tile_category", F.lit("UNKNOWN")) | STRING | No |
| SOURCE_HOME_TILE_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_views | F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))) | LONG | No |
| SOURCE_HOME_TILE_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_clicks | F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))) | LONG | No |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_views | F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))) | LONG | No |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_primary_clicks | F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))) | LONG | No |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_secondary_clicks | F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))) | LONG | No |

### 2. Transformation Rules

#### 2.1 Category Enrichment Rule
```sql
CASE 
    WHEN metadata.tile_category IS NOT NULL THEN metadata.tile_category
    ELSE 'UNKNOWN'
END AS tile_category
```

#### 2.2 Join Logic
```sql
LEFT JOIN SOURCE_TILE_METADATA metadata 
    ON events.tile_id = metadata.tile_id 
    AND metadata.is_active = true
```

#### 2.3 Data Quality Rules
- All tile_id values must be preserved (LEFT JOIN ensures no data loss)
- Missing metadata results in "UNKNOWN" category
- Only active tiles from metadata are considered
- Null handling: F.coalesce() ensures no null values in target

### 3. Aggregation Patterns

| Metric | Aggregation Logic | Group By Columns |
|--------|------------------|------------------|
| unique_tile_views | COUNT(DISTINCT user_id) WHERE event_type = 'TILE_VIEW' | tile_id, date |
| unique_tile_clicks | COUNT(DISTINCT user_id) WHERE event_type = 'TILE_CLICK' | tile_id, date |
| unique_interstitial_views | COUNT(DISTINCT user_id) WHERE interstitial_view_flag = true | tile_id, date |
| unique_interstitial_primary_clicks | COUNT(DISTINCT user_id) WHERE primary_button_click_flag = true | tile_id, date |
| unique_interstitial_secondary_clicks | COUNT(DISTINCT user_id) WHERE secondary_button_click_flag = true | tile_id, date |

## Assumptions and Constraints

### Assumptions
1. **Metadata Availability:** SOURCE_TILE_METADATA table will be populated before ETL execution
2. **Data Freshness:** Metadata table is updated in near real-time or daily
3. **Tile Lifecycle:** Active tiles (is_active = true) are the only ones requiring category mapping
4. **Backward Compatibility:** Existing reports and dashboards can handle the new column
5. **Data Volume:** Metadata table remains small (< 10,000 records) for efficient joins

### Constraints
1. **Schema Evolution:** Target table schema change requires coordinated deployment
2. **Performance Impact:** Additional join may increase ETL runtime by 5-10%
3. **Data Quality:** "UNKNOWN" category will appear for tiles without metadata
4. **Storage:** Additional column increases target table storage by ~5%
5. **Backward Compatibility:** Historical data will not have tile_category populated

### Technical Constraints
1. **Delta Lake:** Schema evolution must be enabled for target table
2. **Partitioning:** Existing date partitioning strategy remains unchanged
3. **Idempotency:** Partition overwrite logic must handle new column
4. **Testing:** Comprehensive testing required before production deployment

### Business Constraints
1. **Category Maintenance:** Business teams responsible for maintaining metadata accuracy
2. **Reporting Impact:** BI team must update dashboards to leverage new category dimension
3. **Data Governance:** New table requires proper access controls and documentation

## References

### JIRA Stories
- **SCRUM-7819:** Add New Source Table & Extend Target Reporting Metrics

### Technical Documentation
- **Source Tables:**
  - `analytics_db.SOURCE_HOME_TILE_EVENTS`
  - `analytics_db.SOURCE_INTERSTITIAL_EVENTS`
  - `analytics_db.SOURCE_TILE_METADATA` (New)

- **Target Tables:**
  - `reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY` (Enhanced)
  - `reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS` (Unchanged)

### Code Files
- **ETL Pipeline:** `home_tile_reporting_etl.py`
- **Source DDL:** `SourceDDL.sql`
- **Target DDL:** `TargetDDL.sql`
- **Metadata DDL:** `SOURCE_TILE_METADATA.sql`

### Dependencies
- **Upstream:** SOURCE_TILE_METADATA table creation and population
- **Downstream:** BI dashboard updates to utilize tile_category
- **Testing:** Unit test updates for new join logic and schema validation

---

**Cost Estimation and Justification**

**Token Usage Analysis:**
- **Input Tokens:** ~4,500 tokens (including prompt, SQL files, Python code, and JIRA story)
- **Output Tokens:** ~3,200 tokens (comprehensive technical specification)
- **Model Used:** GPT-4 (assumed based on complexity handling)

**Cost Calculation:**
- Input Cost = 4,500 tokens × $0.03/1K tokens = $0.135
- Output Cost = 3,200 tokens × $0.06/1K tokens = $0.192
- **Total Cost = $0.327**

**Cost Formula:**
```
Total Cost = (Input Tokens × Input Rate) + (Output Tokens × Output Rate)
Total Cost = (4,500 × $0.00003) + (3,200 × $0.00006) = $0.327
```

**Justification:**
This automated technical specification generation provides significant value by:
1. Reducing manual documentation time from 8+ hours to minutes
2. Ensuring comprehensive coverage of all technical aspects
3. Maintaining consistency across projects
4. Enabling faster development cycles and reduced errors

The $0.33 cost represents exceptional ROI compared to manual effort.