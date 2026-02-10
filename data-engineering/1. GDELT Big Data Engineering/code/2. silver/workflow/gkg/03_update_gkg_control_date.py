# Databricks notebook source
"""
==============================================================================
SILVER LAYER - UPDATE GKG CONTROL DATE
==============================================================================

Purpose:
--------
This notebook is the final step in the Silver layer workflow for GDELT GKG
processing. It updates the TABLE_CONTROL with the successfully processed date,
enabling the next workflow run to process the following day.

Workflow Position:
------------------
Step 3 of 3 in Silver Layer GKG Processing:
    [1] Get Control Date
    [2] Scrape and Parse Data
  → [3] Update Control Date (this script)

Key Functionality:
------------------
1. Calculate yesterday's date for control table update
2. Update BRONZE.TABLE_CONTROL with new LAST_UPDATE_DATE
3. Enable incremental Silver layer processing for next run

Control Table Entry:
--------------------
Updates: 'BRONZE-GDELT_GKG' record in BRONZE.TABLE_CONTROL
(Note: Different naming from retrieval which uses 'silver-gdelt-gkg')

IMPORTANT INCONSISTENCY:
------------------------
This script uses 'BRONZE-GDELT_GKG' as TABLE_NAME, but the retrieval script
(00_get_gkg_control_date.py) looks for 'silver-gdelt-gkg'. This naming
inconsistency is a hackathon bug that should be fixed in production.

Expected behavior:
- Retrieval: WHERE TABLE_NAME = 'silver-gdelt-gkg'
- Update: WHERE TABLE_NAME = 'silver-gdelt-gkg' (should match!)

Current behavior (BUG):
- Retrieval: WHERE TABLE_NAME = 'silver-gdelt-gkg'  ✓
- Update: WHERE TABLE_NAME = 'BRONZE-GDELT_GKG'     ✗ (doesn't match!)

This means the control date is never actually updated for Silver layer!

Why This Matters:
-----------------
- Enables incremental processing (only new data on next run)
- Prevents reprocessing of already-transformed data
- Maintains Silver layer processing history
- Supports recovery from failures

Dependencies:
-------------
- Previous task: 01_scrap_data.py (must complete successfully)
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
print("SILVER LAYER - UPDATING GKG CONTROL DATE")
print("=" * 70)

# ============================================
# CALCULATE YESTERDAY'S DATE
# ============================================
"""
HACKATHON APPROACH (Same as Bronze GKG):

Uses yesterday's date directly instead of task values.
This is simpler but less flexible than Events workflow pattern.

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
Update TABLE_CONTROL with Silver layer processing date.

CRITICAL BUG:
-------------
This query updates 'BRONZE-GDELT_GKG' but retrieval script looks for
'silver-gdelt-gkg'. The naming mismatch means this update has no effect!

The control date for Silver layer is never actually updated, so every
run thinks it needs to process the same date range.

Production Fix:
---------------
Change WHERE clause to match retrieval:
WHERE TABLE_NAME = 'silver-gdelt-gkg'
"""
update_query = f"""
UPDATE BRONZE.TABLE_CONTROL
SET LAST_UPDATE_DATE = '{yesterday_date}',
    LAST_MODIFIED = CURRENT_TIMESTAMP
WHERE TABLE_NAME = 'BRONZE-GDELT_GKG'
"""

try:
    # ============================================
    # EXECUTE UPDATE STATEMENT
    # ============================================
    spark.sql(update_query)

    print("✓ Control table update executed")
    print(f"  Table: BRONZE.TABLE_CONTROL")
    print(f"  Record: BRONZE-GDELT_GKG")
    print(f"  New LAST_UPDATE_DATE: {yesterday_date}")
    print(f"  LAST_MODIFIED: CURRENT_TIMESTAMP")
    print("=" * 70)
    print(f"Tabla de control actualizada con la fecha de ayer: {yesterday_date}")

    # ============================================
    # WARNING MESSAGE
    # ============================================
    print("\n⚠️  HACKATHON BUG ALERT:")
    print("  Retrieval looks for: 'silver-gdelt-gkg'")
    print("  Update modifies: 'BRONZE-GDELT_GKG'")
    print("  → Names don't match! Control date not actually updated.")
    print("  → Silver layer will reprocess same dates on next run.")

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
    print("  3. Verify record exists for 'BRONZE-GDELT_GKG'")
    print("  4. Check TABLE_NAME consistency with retrieval script")

    # Re-raise exception to fail the workflow
    raise

# ============================================
# END OF NOTEBOOK
# ============================================

"""
==============================================================================
PRODUCTION FIXES NEEDED
==============================================================================

Critical Issues:
----------------
1. TABLE_NAME Mismatch:
   - Retrieval: 'silver-gdelt-gkg'
   - Update: 'BRONZE-GDELT_GKG'
   - Fix: Standardize to 'silver-gdelt-gkg'

2. Hardcoded Yesterday Date:
   - Current: datetime.now() - timedelta(1)
   - Should: Use task values from control date retrieval
   - Fix: Get date from dbutils.jobs.taskValues

3. Missing Validation:
   - No verification that update succeeded
   - No check if row exists before update
   - Fix: Add verification query after update

Recommended Changes:
--------------------
```python
# Get date from previous task (not hardcoded)
next_date_to_process = dbutils.jobs.taskValues.get(
    "00_get_gkg_control_date",
    "next_date_to_process_gkg"
)

# Update with correct TABLE_NAME
update_query = f\"\"\"
UPDATE BRONZE.TABLE_CONTROL
SET LAST_UPDATE_DATE = '{next_date_to_process}',
    LAST_MODIFIED = CURRENT_TIMESTAMP
WHERE TABLE_NAME = 'silver-gdelt-gkg'
\"\"\"

# Verify update succeeded
verification_query = \"\"\"
SELECT TABLE_NAME, LAST_UPDATE_DATE
FROM BRONZE.TABLE_CONTROL
WHERE TABLE_NAME = 'silver-gdelt-gkg'
\"\"\"
spark.sql(verification_query).show()
```

==============================================================================
"""
