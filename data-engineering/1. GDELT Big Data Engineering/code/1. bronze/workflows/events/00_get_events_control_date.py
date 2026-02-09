# Databricks notebook source
"""
==============================================================================
BRONZE LAYER - CONTROL DATE RETRIEVAL FOR EVENTS
==============================================================================

Purpose:
--------
This notebook is the first step in the Bronze layer workflow for GDELT Events
and GKG data ingestion. It retrieves the last processed date from the control
table and calculates the next date to process for incremental data loading.

Workflow Position:
------------------
Step 1 of 4 in Bronze Layer Ingestion:
  → [1] Get Control Date (this script)
    [2] Download and Unzip Data
    [3] Upsert to Delta Table
    [4] Update Control Date

Key Functionality:
------------------
1. Query TABLE_CONTROL for last processed dates
2. Calculate next date to process (last_date + 1 day)
3. Set task values for downstream workflows
4. Handle both GDELT Events and GKG tables

Dependencies:
-------------
- BRONZE.TABLE_CONTROL must exist and be initialized
- Databricks job context (dbutils) required
- PySpark environment

Output:
-------
Task values set for downstream notebooks:
  - next_date_to_process_events: Next date for events ingestion
  - next_date_to_process_gkg: Next date for GKG ingestion

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
# Initialize Spark session for database queries
# AppName helps identify this job in Spark UI
spark = SparkSession.builder.appName("Control Fechas GDELT").getOrCreate()

# ============================================
# QUERY CONTROL TABLE
# ============================================
# The TABLE_CONTROL table tracks the last successfully processed date
# for each table in the pipeline. This enables incremental processing
# and prevents reprocessing of already ingested data.
query = """
SELECT TABLE_NAME, LAST_UPDATE_DATE
FROM BRONZE.TABLE_CONTROL
WHERE TABLE_NAME IN ('gdelt_events', 'gdelt_gkg')
"""

# Execute the query to retrieve control dates
df = spark.sql(query)

# ============================================
# PROCESS CONTROL DATES
# ============================================
# Verify that control records exist
if df.count() > 0:
    # Extract last processed date for GDELT Events table
    # collect()[0][0] retrieves the first row, first column value
    gdelt_events_last_modified = df.filter(
        df.TABLE_NAME == 'gdelt_events'
    ).select('LAST_UPDATE_DATE').collect()[0][0]

    # Extract last processed date for GDELT GKG table
    gdelt_gkg_last_modified = df.filter(
        df.TABLE_NAME == 'gdelt_gkg'
    ).select('LAST_UPDATE_DATE').collect()[0][0]

    # Calculate next date to process (increment by 1 day)
    # This ensures we process each day exactly once
    next_date_to_process_events = (
        gdelt_events_last_modified + timedelta(1)
    ).strftime('%Y-%m-%d')

    next_date_to_process_gkg = (
        gdelt_gkg_last_modified + timedelta(1)
    ).strftime('%Y-%m-%d')

    # ============================================
    # SET TASK VALUES FOR DOWNSTREAM WORKFLOWS
    # ============================================
    # Databricks Workflows use task values to pass data between notebooks
    # These values will be consumed by the download/unzip notebooks
    dbutils.jobs.taskValues.set("next_date_to_process_events", next_date_to_process_events)
    dbutils.jobs.taskValues.set("next_date_to_process_gkg", next_date_to_process_gkg)

    # ============================================
    # LOGGING OUTPUT
    # ============================================
    print("=" * 70)
    print("CONTROL DATE RETRIEVAL SUCCESSFUL")
    print("=" * 70)
    print(f"GDELT Events:")
    print(f"  Last Processed Date: {gdelt_events_last_modified}")
    print(f"  Next Date to Process: {next_date_to_process_events}")
    print()
    print(f"GDELT GKG:")
    print(f"  Last Processed Date: {gdelt_gkg_last_modified}")
    print(f"  Next Date to Process: {next_date_to_process_gkg}")
    print("=" * 70)

else:
    # ============================================
    # ERROR HANDLING
    # ============================================
    # If no control records found, the TABLE_CONTROL table may not be initialized
    # or may be missing required records
    print("ERROR: Control dates not found!")
    print("Please ensure BRONZE.TABLE_CONTROL is initialized with:")
    print("  - gdelt_events record")
    print("  - gdelt_gkg record")
    print("\nInitialization SQL:")
    print("""
    INSERT INTO BRONZE.TABLE_CONTROL (TABLE_NAME, LAST_UPDATE_DATE)
    VALUES
        ('gdelt_events', '2022-01-01'),
        ('gdelt_gkg', '2022-01-01');
    """)

    # Raise exception to fail the workflow and alert operators
    raise Exception("TABLE_CONTROL not properly initialized")

# ============================================
# END OF NOTEBOOK
# ============================================
