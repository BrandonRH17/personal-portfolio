# Databricks notebook source
"""
==============================================================================
BRONZE LAYER - UPDATE CONTROL DATE FOR GKG
==============================================================================

Purpose:
--------
This notebook is the final step in the Bronze layer workflow for GDELT GKG
data ingestion. It updates the TABLE_CONTROL with the successfully processed
date, enabling the next workflow run to process the following day.

Workflow Position:
------------------
Step 4 of 4 in Bronze Layer GKG Ingestion:
    [1] Get Control Date
    [2] Download and Unzip Data
    [3] Upsert to Delta Table
  → [4] Update Control Date (this script)

Key Difference from Events Workflow:
-------------------------------------
**IMPORTANT INCONSISTENCY (Hackathon Bug):**
- Events workflow: Uses task values from 00_get_events_control_date
- GKG workflow: Uses yesterday's date directly (datetime.now() - timedelta(1))

This inconsistency means:
- Events workflow: Can process historical dates (controlled by TABLE_CONTROL)
- GKG workflow: Always processes yesterday (hardcoded, ignores TABLE_CONTROL)

Why This Happened:
------------------
During the 1-week datathon, the GKG workflow was developed after Events workflow.
The team used a simpler approach (yesterday date) for faster development but
didn't refactor to match Events pattern.

Production Fix Needed:
----------------------
Should be changed to match Events workflow:
```python
next_date_to_process = dbutils.jobs.taskValues.get(
    "00_get_events_control_date",
    "next_date_to_process_gkg"
)
```

Why This Matters:
-----------------
- Enables incremental processing (only new data on next run)
- Prevents reprocessing of already-ingested data
- Maintains data lineage and processing history
- Supports recovery (restart from last successful date)

Dependencies:
-------------
- Previous task: 02_upsert_delta_table_gkg (must complete successfully)
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
# Initialize Spark session for database queries
spark = SparkSession.builder.appName("Actualizar Fecha de Control").getOrCreate()

print("=" * 70)
print("UPDATING CONTROL DATE FOR GDELT GKG")
print("=" * 70)

# ============================================
# CALCULATE YESTERDAY'S DATE
# ============================================
"""
HACKATHON INCONSISTENCY:

This approach is different from Events workflow:
- Events: Uses task values from control date retrieval
- GKG: Calculates yesterday's date directly

Why This Is Problematic:
-------------------------
1. Ignores TABLE_CONTROL state (defeats the purpose of control table)
2. Can't process historical dates (always yesterday)
3. Can't recover from failures (always moves forward)
4. Inconsistent with Events workflow pattern

Why This Was Done:
------------------
- Faster development during datathon time crunch
- "Good enough" for near-real-time processing
- Team focused on winning demo, not production robustness

Production Fix:
---------------
Replace this block with:
```python
next_date_to_process = dbutils.jobs.taskValues.get(
    "00_get_events_control_date",
    "next_date_to_process_gkg"
)
yesterday_date = next_date_to_process
```
"""
yesterday_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

print(f"Date to update in control table: {yesterday_date}")
print("⚠️  WARNING: Using yesterday's date (hardcoded), not task values")
print("   This differs from Events workflow pattern")

# ============================================
# UPDATE CONTROL TABLE
# ============================================
"""
Update the TABLE_CONTROL to reflect successful processing.

The UPDATE statement:
- Sets LAST_UPDATE_DATE to yesterday's date
- Sets LAST_MODIFIED to current timestamp (audit trail)
- Only updates the gdelt_gkg record

This ensures that:
- Next workflow run will process today (yesterday + 1)
- We maintain accurate processing history
- Recovery is possible from any failure point

Note: In production, this should use next_date_to_process from task values
to be consistent with Events workflow.
"""

update_query = f"""
UPDATE BRONZE.TABLE_CONTROL
SET LAST_UPDATE_DATE = '{yesterday_date}',
    LAST_MODIFIED = CURRENT_TIMESTAMP
WHERE TABLE_NAME = 'gdelt_gkg'
"""

try:
    # ============================================
    # EXECUTE UPDATE STATEMENT
    # ============================================
    # Execute UPDATE statement
    spark.sql(update_query)

    print("✓ Control table updated successfully")
    print(f"  Table: BRONZE.TABLE_CONTROL")
    print(f"  Record: gdelt_gkg")
    print(f"  New LAST_UPDATE_DATE: {yesterday_date}")
    print(f"  LAST_MODIFIED: CURRENT_TIMESTAMP")

    # ============================================
    # VERIFICATION QUERY
    # ============================================
    # Verify the update was successful
    verification_query = """
    SELECT TABLE_NAME, LAST_UPDATE_DATE, LAST_MODIFIED
    FROM BRONZE.TABLE_CONTROL
    WHERE TABLE_NAME = 'gdelt_gkg'
    """

    result_df = spark.sql(verification_query)
    result_df.show(truncate=False)

    print("=" * 70)
    print("SUCCESS: Workflow completed successfully")
    print("=" * 70)

    # Calculate next date to process (for informational purposes)
    next_date = (datetime.strptime(yesterday_date, '%Y-%m-%d') + timedelta(1)).strftime('%Y-%m-%d')
    print(f"Next workflow run will process: {next_date}")
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
    print("  3. Verify record exists for 'gdelt_gkg'")
    print("  4. Check if yesterday_date format is correct (YYYY-MM-DD)")

    # Re-raise exception to fail the workflow
    # This ensures we don't proceed with corrupted control state
    raise

# ============================================
# FINALIZATION
# ============================================
# Note: Spark session is not stopped here (unlike Events workflow)
# This is another minor inconsistency from hackathon development

# ============================================
# END OF NOTEBOOK
# ============================================

"""
==============================================================================
PRODUCTION IMPROVEMENTS NEEDED
==============================================================================

Critical Fix:
-------------
Replace yesterday_date calculation with task values:
```python
next_date_to_process = dbutils.jobs.taskValues.get(
    "00_get_events_control_date",
    "next_date_to_process_gkg"
)
```

Additional Improvements:
------------------------
1. Add spark.stop() at the end (resource cleanup)
2. Add more detailed logging (processing metrics, record counts)
3. Add validation (ensure date is within expected range)
4. Add alerting (email/Slack notification on success)
5. Consider using Delta Lake for TABLE_CONTROL (ACID transactions)

Why These Matter:
-----------------
The current approach works for near-real-time processing but breaks down for:
- Historical backfills (can't specify date range)
- Failure recovery (always moves to yesterday, can't restart from failure point)
- Data quality validation (no checks on date validity)
- Production monitoring (no success/failure notifications)

==============================================================================
"""
