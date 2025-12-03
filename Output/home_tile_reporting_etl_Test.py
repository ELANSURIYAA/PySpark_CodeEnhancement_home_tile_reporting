"""
====================================================================
Author: Ascendion AAVA
Date: 
Description: Python-based test script for Home Tile Reporting ETL pipeline. Validates enrichment of tile_category and fallback to 'UNKNOWN'. Reports results in markdown format. No PyTest dependency.
====================================================================

import sys
import pandas as pd
from pyspark.sql import SparkSession