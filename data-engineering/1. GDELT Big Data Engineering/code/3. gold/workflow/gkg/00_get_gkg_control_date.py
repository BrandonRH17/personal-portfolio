# Databricks notebook source
"""
==============================================================================
GOLD LAYER - GET GKG CONTROL DATE
==============================================================================

Purpose:
--------
This notebook is the first step in the Gold layer workflow for GDELT GKG
processing. It retrieves the last processed date from the control table and
calculates the next date to process for incremental aggregation.

Workflow Position:
------------------
Step 1 of 3 in Gold Layer GKG Processing:
  → [1] Get Control Date (this script)
    [2] Build Weighted News Summary
    [3] Update Control Date

Key Functionality:
------------------
1. Query TABLE_CONTROL for Gold layer's last processed date
2. Calculate next date to process (last_date + 1 day)
3. Set task values for downstream Gold workflows

Why Separate Control from Silver?
----------------------------------
Gold layer has its own control date because:
- Gold and Silver can process at different speeds
- Gold may reprocess Silver data for new aggregations
- Allows independent pipeline execution and recovery
- Enables backfill of Gold layer without touching Silver

Control Table Entry:
--------------------
Gold layer uses 'gold-gdelt-gkg' record in BRONZE.TABLE_CONTROL

Layer Control Records:
- Bronze: 'gdelt_gkg'
- Silver: 'silver-gdelt-gkg'
- Gold: 'gold-gdelt-gkg'

Dependencies:
-------------
- BRONZE.TABLE_CONTROL must exist and be initialized
- Databricks job context (dbutils) required
- PySpark environment

Output:
-------
Task value set for downstream notebooks:
  - next_date_to_process_gkg: Next date for Gold processing

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
spark = SparkSession.builder.appName("Control Fechas GDELT").getOrCreate()

print("=" * 70)
print("GOLD LAYER - RETRIEVING GKG CONTROL DATE")
print("=" * 70)

# ============================================
# QUERY CONTROL TABLE
# ============================================
"""
Query TABLE_CONTROL for Gold layer processing status.

The TABLE_CONTROL table tracks the last successfully processed date
for each layer and table. This enables incremental processing.

Gold Layer Control Entry:
- TABLE_NAME: 'gold-gdelt-gkg'
- LAST_UPDATE_DATE: Last date successfully processed in Gold
"""
query = """
SELECT TABLE_NAME, LAST_UPDATE_DATE
FROM BRONZE.TABLE_CONTROL
"""

# Execute the query to retrieve control date
df = spark.sql(query)

# ============================================
# PROCESS CONTROL DATE
# ============================================
# Verify that control record exists
if df.count() > 0:
    # ============================================
    # EXTRACT GOLD LAYER CONTROL DATE
    # ============================================
    # Filter for Gold GKG record and extract last processed date
    # Note: Uses 'gold-gdelt-gkg' (different from Bronze 'gdelt_gkg' and Silver 'silver-gdelt-gkg')
    gdelt_gkg_last_modified = df.filter(
        df.TABLE_NAME == 'gold-gdelt-gkg'
    ).select('LAST_UPDATE_DATE').collect()[0][0]

    # ============================================
    # CALCULATE NEXT DATE TO PROCESS
    # ============================================
    # Increment by 1 day to get next date for Gold processing
    # Gold processes data that was already transformed by Silver
    next_date_to_process_gkg = (
        gdelt_gkg_last_modified + timedelta(1)
    ).strftime('%Y-%m-%d')

    # ============================================
    # SET TASK VALUE FOR DOWNSTREAM WORKFLOWS
    # ============================================
    # Databricks Workflows use task values to pass data between notebooks
    # This value will be consumed by 01_build_gdelt_gkg_weighted_news_summary.py
    dbutils.jobs.taskValues.set("next_date_to_process_gkg", next_date_to_process_gkg)

    # ============================================
    # LOGGING OUTPUT
    # ============================================
    print("✓ Control date retrieved successfully")
    print(f"  Gold Layer Record: gold-gdelt-gkg")
    print(f"  Last Processed Date: {gdelt_gkg_last_modified}")
    print(f"  Next Date to Process: {next_date_to_process_gkg}")
    print("=" * 70)

    print(f"Última fecha procesada tabla gkg:    {gdelt_gkg_last_modified}")
    print(f"Próxima fecha a procesar gkg:        {next_date_to_process_gkg}")

else:
    # ============================================
    # ERROR HANDLING
    # ============================================
    # If no control records found, the TABLE_CONTROL table may not be initialized
    print("ERROR: Control date not found!")
    print("Please ensure BRONZE.TABLE_CONTROL is initialized with:")
    print("  - gold-gdelt-gkg record")
    print("\nInitialization SQL:")
    print("""
    INSERT INTO BRONZE.TABLE_CONTROL (TABLE_NAME, LAST_UPDATE_DATE)
    VALUES ('gold-gdelt-gkg', '2023-01-01');
    """)
    print("No se encontró la última fecha procesada. Asegúrate de que la tabla de control está correctamente inicializada.")

    # Raise exception to fail the workflow and alert operators
    raise Exception("TABLE_CONTROL not properly initialized for Gold layer")

# ============================================
# END OF NOTEBOOK
# ============================================

"""
==============================================================================
GOLD LAYER PROCESSING FLOW
==============================================================================

Control Date → Weighted Aggregations → Update Control Date

This Gold workflow processes Silver GKG data by:
1. Reading Silver Delta tables (parsed, structured, enriched data)
2. Filtering emotionally-charged news (neutrality + polarity paradox)
3. Applying exponential weighted scoring (5 themes = 500x)
4. Aggregating by date/location/route
5. Writing to Gold Delta tables (analytics-ready metrics)

Next Step:
----------
01_build_gdelt_gkg_weighted_news_summary.py will use next_date_to_process_gkg to:
- Query Silver GKG for that specific date
- Apply data quality filters (emotional charge)
- Calculate weighted news counts
- Write to Gold Delta table for Power BI dashboard

==============================================================================
"""
