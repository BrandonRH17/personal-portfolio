# Databricks notebook source
"""
==============================================================================
GOLD LAYER - UPDATE GKG CONTROL DATE
==============================================================================

Purpose:
--------
This notebook is the final step in the Gold layer workflow for GDELT GKG
processing. It updates the TABLE_CONTROL with the successfully processed date,
enabling the next workflow run to process the following day.

Workflow Position:
------------------
Step 3 of 3 in Gold Layer GKG Processing:
    [1] Get Control Date
    [2] Build Weighted News Summary
  → [3] Update Control Date (this script)

Key Functionality:
------------------
1. Calculate yesterday's date for control table update
2. Update BRONZE.TABLE_CONTROL with new LAST_UPDATE_DATE
3. Enable incremental Gold layer processing for next run

Control Table Entry:
--------------------
Updates: 'gold-gdelt-gkg' record in BRONZE.TABLE_CONTROL

Layer Control Records:
- Bronze: 'gdelt_gkg'
- Silver: 'silver-gdelt-gkg'
- Gold: 'gold-gdelt-gkg'

Hackathon Note:
---------------
This script uses hardcoded yesterday's date instead of task values from
the control date retrieval step. This is simpler but less flexible than
using task values (can't do historical backfills easily).

Production Fix:
---------------
Should use task values from 00_get_gkg_control_date:
```python
next_date_to_process = dbutils.jobs.taskValues.get(
    "00_get_gkg_control_date",
    "next_date_to_process_gkg"
)
yesterday_date = next_date_to_process
```

Why This Matters:
-----------------
- Enables incremental processing (only new data on next run)
- Prevents reprocessing of already-aggregated data
- Maintains Gold layer processing history
- Supports recovery from failures

Dependencies:
-------------
- Previous task: 01_build_gdelt_gkg_weighted_news_summary (must complete successfully)
- BRONZE.TABLE_CONTROL table must exist
- Sufficient permissions to UPDATE table

Author: Neutrino Solutions Team (Factored Datathon 2024)
Date: 2024
==============================================================================
"""

# ============================================
# IMPORTS
# ============================================
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# ============================================
# SPARK SESSION INITIALIZATION
# ============================================
# Initialize Spark session for database updates
spark = SparkSession.builder.appName("Actualizar Fecha de Control").getOrCreate()

print("=" * 70)
print("GOLD LAYER - UPDATING GKG CONTROL DATE")
print("=" * 70)

# ============================================
# CALCULATE YESTERDAY'S DATE
# ============================================
"""
HACKATHON APPROACH (Same as Bronze GKG):

Uses yesterday's date directly instead of task values.
This is simpler but less flexible than using task values.

Limitation:
-----------
- Always processes yesterday (can't do historical backfills)
- Ignores TABLE_CONTROL state
- Can't recover from specific failure dates

Production Fix:
---------------
Should use task values from 00_get_gkg_control_date:
```python
next_date_to_process = dbutils.jobs.taskValues.get(
    "00_get_gkg_control_date",
    "next_date_to_process_gkg"
)
yesterday_date = next_date_to_process
```
"""
yesterday_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

print(f"Date to update in control table: {yesterday_date}")
print("⚠️  WARNING: Using yesterday's date (hardcoded), not task values")

# ============================================
# UPDATE CONTROL TABLE
# ============================================
"""
Update TABLE_CONTROL with Gold layer processing date.

Gold Layer Control Entry:
-------------------------
TABLE_NAME: 'gold-gdelt-gkg'
LAST_UPDATE_DATE: Yesterday's date (successfully processed)
LAST_MODIFIED: Current timestamp

This update enables the next workflow run to pick up where it left off.
"""
update_query = f"""
UPDATE BRONZE.TABLE_CONTROL
SET LAST_UPDATE_DATE = '{yesterday_date}',
    LAST_MODIFIED = CURRENT_TIMESTAMP
WHERE TABLE_NAME = 'gold-gdelt-gkg'
"""

try:
    # ============================================
    # EXECUTE UPDATE STATEMENT
    # ============================================
    spark.sql(update_query)

    print("✓ Control table update executed")
    print(f"  Table: BRONZE.TABLE_CONTROL")
    print(f"  Record: gold-gdelt-gkg")
    print(f"  New LAST_UPDATE_DATE: {yesterday_date}")
    print(f"  LAST_MODIFIED: CURRENT_TIMESTAMP")
    print("=" * 70)
    print(f"Tabla de control actualizada con la fecha de ayer: {yesterday_date}")

except Exception as e:
    # ============================================
    # ERROR HANDLING
    # ============================================
    print("ERROR: Failed to update control table")
    print(f"  Exception: {e}")
    print(f"  Query: {update_query}")
    print("\nTroubleshooting:")
    print("  1. Verify BRONZE.TABLE_CONTROL exists")
    print("  2. Check UPDATE permissions")
    print("  3. Verify record exists for 'gold-gdelt-gkg'")

    # Re-raise exception to fail the workflow
    raise

# ============================================
# END OF NOTEBOOK
# ============================================

"""
==============================================================================
GOLD LAYER PROCESSING COMPLETE
==============================================================================

Control Date → Weighted Aggregations → Update Control Date

The Gold layer workflow is now complete for this date. On the next run:

1. 00_get_gkg_control_date.py will read the updated control date
2. It will calculate next_date_to_process = yesterday_date + 1 day
3. 01_build_gdelt_gkg_weighted_news_summary.py will process that new date
4. This script (03_update_gkg_control_date.py) will update again

This creates a continuous incremental processing loop where each day's
Gold layer workflow processes the next date in sequence.

Production Improvements Needed:
--------------------------------
1. Use task values instead of hardcoded yesterday date
2. Add verification query after update to confirm success
3. Add validation that date being updated is correct (not skipping dates)
4. Consider using MERGE instead of UPDATE for idempotency

Example Verification Query:
----------------------------
```sql
SELECT TABLE_NAME, LAST_UPDATE_DATE, LAST_MODIFIED
FROM BRONZE.TABLE_CONTROL
WHERE TABLE_NAME = 'gold-gdelt-gkg'
```

==============================================================================
"""
