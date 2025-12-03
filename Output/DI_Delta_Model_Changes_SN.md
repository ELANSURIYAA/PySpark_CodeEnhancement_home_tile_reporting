=============================================
Author: Ascendion AAVA
Date: 
Description: Data Model Evolution Package for Home Tile Reporting Enhancement with Metadata Integration
=============================================

# Data Model Evolution Package (DMEA)
## Home Tile Reporting Enhancement - Metadata Integration

### Executive Summary
This Data Model Evolution Package documents the changes required to integrate tile metadata functionality into the existing Home Tile Reporting system. The enhancement adds business categorization capabilities to enable category-level reporting and analytics.

---

## 1. Delta Summary Report

### Impact Level: **MEDIUM**

### Overview of Changes
The enhancement involves adding a new source table for tile metadata and extending the existing target table to include tile categorization fields. This is a backward-compatible addition that enriches existing functionality without breaking current processes.

### Change Categories

#### **Additions**
- **New Source Table**: `analytics_db.SOURCE_TILE_METADATA`
- **New Target Columns**: 
  - `tile_name` (STRING) in TARGET_HOME_TILE_DAILY_SUMMARY
  - `tile_category` (STRING) in TARGET_HOME_TILE_DAILY_SUMMARY
- **New ETL Logic**: LEFT JOIN with metadata table for enrichment

#### **Modifications**
- **ETL Pipeline**: Enhanced aggregation logic to include metadata enrichment
- **Target Table Schema**: Extended with new columns while maintaining existing structure
- **Configuration**: Added new source table reference

#### **Deprecations**
- None - This is a purely additive enhancement

### Risk Assessment
- **Data Loss Risk**: **LOW** - No existing data is modified or removed
- **Breaking Changes**: **NONE** - Backward compatible implementation
- **Performance Impact**: **LOW** - Single LEFT JOIN addition with minimal overhead

---

## 2. DDL Change Scripts

### 2.1 Forward Migration Scripts

#### Create New Source Table
```sql
-- SOURCE_TILE_METADATA Table Creation
-- Purpose: Master metadata for homepage tiles
-- Impact: New table addition
-- Reference: JIRA SCRUM-7567

CREATE TABLE IF NOT EXISTS analytics_db.SOURCE_TILE_METADATA 
(
    tile_id        STRING    COMMENT 'Tile identifier - Primary Key'