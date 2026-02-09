# Databricks notebook source
"""
==============================================================================
BRONZE LAYER - UPDATE CONTROL DATE FOR EVENTS
==============================================================================

Purpose:
--------
This notebook is the final step in the Bronze layer workflow for GDELT Events
data ingestion. It updates the TABLE_CONTROL with the successfully processed
date, enabling the next workflow run to process the following day.

Workflow Position:
------------------
Step 4 of 4 in Bronze Layer Ingestion:
    [1] Get Control Date
    [2] Download and Unzip Data
    [3] Upsert to Delta Table
  → [4] Update Control Date (this script)

Key Functionality:
------------------
1. Retrieve the date that was just processed
2. Update BRONZE.TABLE_CONTROL with new LAST_UPDATE_DATE
3. Log the update for audit trail

Why This Step Matters:
----------------------
- Enables incremental processing (only new data on next run)
- Prevents reprocessing of already-ingested data
- Maintains data lineage and processing history
- Supports recovery (restart from last successful date)

Dependencies:
-------------
- Previous task: 02_upsert_delta_table (must complete successfully)
- BRONZE.TABLE_CONTROL table must exist
- Sufficient permissions to UPDATE table

Author: Neutrino Solutions Team
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
spark = SparkSession.builder \
    .appName("Update GDELT Events Control Date") \
    .getOrCreate()

print("=" * 70)
print("UPDATING CONTROL DATE FOR GDELT EVENTS")
print("=" * 70)

# ============================================
# RETRIEVE PROCESSED DATE
# ============================================
# Get the date that was successfully processed in this workflow run
# This comes from the initial control date retrieval task
next_date_to_process = dbutils.jobs.taskValues.get(
    "00_get_events_control_date",
    "next_date_to_process_events"
)

print(f"Date processed in this run: {next_date_to_process}")

# ============================================
# UPDATE CONTROL TABLE
# ============================================
"""
Update the TABLE_CONTROL to reflect successful processing.

The UPDATE statement:
- Sets LAST_UPDATE_DATE to the date we just processed
- Only updates the gdelt_events record
- Commits immediately (Delta Lake ACID transaction)

This ensures that:
- Next workflow run will process next_date + 1
- We maintain accurate processing history
- Recovery is possible from any failure point
"""

update_query = f"""
UPDATE BRONZE.TABLE_CONTROL
SET LAST_UPDATE_DATE = '{next_date_to_process}'
WHERE TABLE_NAME = 'gdelt_events'
"""

try:
    # Execute UPDATE statement
    spark.sql(update_query)

    print("✓ Control table updated successfully")
    print(f"  Table: BRONZE.TABLE_CONTROL")
    print(f"  Record: gdelt_events")
    print(f"  New LAST_UPDATE_DATE: {next_date_to_process}")

    # ============================================
    # VERIFICATION QUERY
    # ============================================
    # Verify the update was successful
    verification_query = """
    SELECT TABLE_NAME, LAST_UPDATE_DATE
    FROM BRONZE.TABLE_CONTROL
    WHERE TABLE_NAME = 'gdelt_events'
    """

    result_df = spark.sql(verification_query)
    result_df.show(truncate=False)

    print("=" * 70)
    print("SUCCESS: Workflow completed successfully")
    print("=" * 70)
    print(f"Next workflow run will process: {(datetime.strptime(next_date_to_process, '%Y-%m-%d') + timedelta(1)).strftime('%Y-%m-%d')}")

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
    print("  3. Verify record exists for 'gdelt_events'")

    # Re-raise exception to fail the workflow
    # This ensures we don't proceed with corrupted control state
    raise

# ============================================
# FINALIZATION
# ============================================
# Stop Spark session
spark.stop()

# ============================================
# END OF NOTEBOOK
# ============================================
