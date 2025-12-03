=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target summary with tile category
=============================================

# Technical Specification for Home Tile Reporting Enhancement - SOURCE_TILE_METADATA Integration

## Introduction

### Background
This technical specification outlines the enhancement to the existing Home Tile Reporting ETL pipeline to incorporate a new source table `SOURCE_TILE_METADATA` and extend the target summary table with tile category information. The enhancement addresses the business requirement to enable category-level reporting and analytics for homepage tiles.

### Business Requirements
Based on JIRA story PCE-3, the enhancement aims to:
- Add tile-level metadata to enrich reporting dashboards
- Enable business grouping of tiles by functional categories (e.g., Offers, Health Checks, Payments)
- Support category-level CTR comparisons and performance tracking
- Improve dashboard drilldowns and future segmentation capabilities

### Scope
- Integration of new `SOURCE_TILE_METADATA` table into existing ETL pipeline
- Extension of `TARGET_HOME_TILE_DAILY_SUMMARY` table with `tile_category` column
- Maintenance of backward compatibility with existing processes
- Implementation of proper error handling and default values

## Code Changes

### 1. ETL Pipeline Modifications (home_tile_reporting_etl.py)

#### 1.1 Configuration Updates
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

#### 1.2 Source Data Reading Enhancement
```python
# Read tile metadata table
df_metadata = (
    spark.table(SOURCE_TILE_METADATA)
    .filter(F.col("is_active") == True)
    .select("tile_id", "tile_name", "tile_category")
)
```

#### 1.3 Daily Summary Aggregation Logic Update
```python
# Enhanced daily summary with metadata join
df_daily_summary_enhanced = (
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

### 2. Target Table Schema Update

#### 2.1 Modified Target DDL
The `TARGET_HOME_TILE_DAILY_SUMMARY` table requires the following schema changes:
```sql
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
ADD COLUMNS (
    tile_name STRING COMMENT 'User-friendly tile name',
    tile_category STRING COMMENT 'Functional category of the tile'
);
```

## Data Model Updates

### 3. Source Data Model Enhancement

#### 3.1 New Source Table: SOURCE_TILE_METADATA
- **Purpose**: Master metadata table for homepage tiles
- **Key Attributes**:
  - `tile_id` (Primary Key): Unique tile identifier
  - `tile_name`: User-friendly display name
  - `tile_category`: Business functional category
  - `is_active`: Active status flag
  - `updated_ts`: Last modification timestamp

#### 3.2 Relationship Mapping
```
SOURCE_HOME_TILE_EVENTS (tile_id) ←→ SOURCE_TILE_METADATA (tile_id)
SOURCE_INTERSTITIAL_EVENTS (tile_id) ←→ SOURCE_TILE_METADATA (tile_id)
```

### 4. Target Data Model Enhancement

#### 4.1 Enhanced TARGET_HOME_TILE_DAILY_SUMMARY
- **New Columns Added**:
  - `tile_name STRING`: Enriched from metadata
  - `tile_category STRING`: Business category classification

#### 4.2 Data Lineage Impact
```
SOURCE_TILE_METADATA → TARGET_HOME_TILE_DAILY_SUMMARY (via LEFT JOIN)
```

## Source-to-Target Mapping

### 5. Detailed Field Mapping

| Source Table | Source Field | Target Table | Target Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------------|
| SOURCE_TILE_METADATA | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | tile_id | Direct mapping (JOIN key) |
| SOURCE_TILE_METADATA | tile_name | TARGET_HOME_TILE_DAILY_SUMMARY | tile_name | COALESCE(tile_name, 'Unknown Tile') |
| SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | tile_category | COALESCE(tile_category, 'UNKNOWN') |
| SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | tile_id | Existing aggregation logic |
| SOURCE_HOME_TILE_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_views | COUNT(DISTINCT user_id) WHERE event_type='TILE_VIEW' |
| SOURCE_HOME_TILE_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_clicks | COUNT(DISTINCT user_id) WHERE event_type='TILE_CLICK' |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_views | COUNT(DISTINCT user_id) WHERE interstitial_view_flag=true |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_primary_clicks | COUNT(DISTINCT user_id) WHERE primary_button_click_flag=true |
| SOURCE_INTERSTITIAL_EVENTS | user_id | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_secondary_clicks | COUNT(DISTINCT user_id) WHERE secondary_button_click_flag=true |

### 6. Transformation Rules

#### 6.1 Join Logic
```sql
LEFT JOIN SOURCE_TILE_METADATA m ON events.tile_id = m.tile_id AND m.is_active = true
```

#### 6.2 Default Value Handling
- **tile_category**: Default to "UNKNOWN" when no metadata exists
- **tile_name**: Default to "Unknown Tile" when no metadata exists
- **Rationale**: Ensures backward compatibility and prevents NULL values

#### 6.3 Data Quality Rules
- Filter metadata by `is_active = true` to exclude deprecated tiles
- Maintain existing aggregation logic for metrics
- Preserve existing partition strategy (by date)

## Assumptions and Constraints

### 7. Technical Assumptions
- SOURCE_TILE_METADATA table will be maintained by the product team
- Metadata updates will not require historical data backfill
- LEFT JOIN ensures no data loss if metadata is missing for some tiles
- Delta table format supports schema evolution for target table

### 8. Performance Considerations
- Metadata table is expected to be small (< 1000 records)
- JOIN operation will have minimal performance impact
- Existing partition strategy remains unchanged
- No impact on global KPI calculations

### 9. Data Governance
- New columns follow existing naming conventions
- Metadata table includes audit fields (updated_ts)
- Default values ensure data completeness
- Schema changes are backward compatible

### 10. Constraints
- No changes to TARGET_HOME_TILE_GLOBAL_KPIS table (out of scope)
- No historical data backfill required
- Dashboard updates handled by BI team separately
- Existing unit tests must continue to pass

## References

### 11. Related Documents
- **JIRA Story**: PCE-3 - Add New Source Table SOURCE_TILE_METADATA and Extend Target Summary Table with Tile Category
- **Source Files**:
  - `Input/home_tile_reporting_etl.py` - Current ETL implementation
  - `Input/SourceDDL.sql` - Existing source table schemas
  - `Input/TargetDDL.sql` - Existing target table schemas
  - `Input/SOURCE_TILE_METADATA.sql` - New metadata table schema

### 12. Implementation Dependencies
- Creation of SOURCE_TILE_METADATA table in analytics_db
- Schema evolution of TARGET_HOME_TILE_DAILY_SUMMARY table
- ETL pipeline code deployment
- Unit test updates and validation

### 13. Testing Strategy
- **Unit Tests**: Validate metadata join logic and default value handling
- **Integration Tests**: End-to-end pipeline execution with new schema
- **Regression Tests**: Ensure existing functionality remains intact
- **Data Quality Tests**: Validate tile_category population and accuracy

---

**Cost Estimation and Justification**

**Token Usage Analysis:**
- **Input Tokens**: Approximately 4,500 tokens (including prompt, file contents, and JIRA story)
- **Output Tokens**: Approximately 2,800 tokens (technical specification document)
- **Model Used**: GPT-4 (assumed based on complexity handling)

**Cost Calculation:**
- Input Cost = 4,500 tokens × $0.03/1K tokens = $0.135
- Output Cost = 2,800 tokens × $0.06/1K tokens = $0.168
- **Total Estimated Cost**: $0.303

**Justification:**
This comprehensive technical specification provides detailed implementation guidance, reducing development time and ensuring accurate implementation of the SOURCE_TILE_METADATA integration enhancement.