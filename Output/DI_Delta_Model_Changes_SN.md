=============================================
Author: Ascendion AAVA
Date: 
Description: Data Model Evolution Package for Home Tile Reporting ETL enhancement with metadata integration
=============================================

# Data Model Evolution Package - Home Tile Reporting Enhancement

## 1. Delta Summary Report

### Overview of Changes
**Impact Level:** MEDIUM

**Change Summary:**
- **New Table Addition:** SOURCE_TILE_METADATA table integration
- **Target Schema Enhancement:** Addition of metadata fields to existing target tables
- **New Target Table:** TARGET_HOME_TILE_CATEGORY_KPIS for category-wise analytics
- **ETL Logic Enhancement:** Metadata enrichment and category-based aggregations

### Detailed Change Analysis

#### **Additions**
1. **New Source Table:**
   - `analytics_db.SOURCE_TILE_METADATA`
     - Fields: tile_id