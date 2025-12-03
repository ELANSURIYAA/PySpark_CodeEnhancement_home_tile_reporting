"""
===============================================================================
Author          : Ascendion AAVA
Date            : 
Description     : Home Tile Reporting ETL pipeline with tile category enrichment
===============================================================================
Functional Description:
    - Reads home tile and interstitial events from Delta source tables
    - Joins tile metadata for business category enrichment
    - Aggregates metrics by tile and category
    - Loads enriched results into target summary and KPI tables
    - Implements backward compatibility (category='UNKNOWN' if no mapping)
    - Idempotent partition overwrite