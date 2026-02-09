# Databricks notebook source
"""
==============================================================================
SILVER LAYER - GET GKG CONTROL DATE
==============================================================================

Purpose:
--------
This notebook is the first step in the Silver layer workflow for GDELT GKG
processing. It retrieves the last processed date from the control table and
calculates the next date to process for incremental data transformation.

Workflow Position:
------------------
Step 1 of 3 in Silver Layer GKG Processing:
  → [1] Get Control Date (this script)
    [2] Scrape and Parse Data
    [3] Update Control Date

Key Functionality:
------------------
1. Query TABLE_CONTROL for Silver layer's last processed date
2. Calculate next date to process (last_date + 1 day)
3. Set task values for downstream Silver workflows

Why Separate Control from Bronze?
----------------------------------
Silver layer has its own control date because:
- Bronze and Silver can process at different speeds
- Silver may reprocess Bronze data without re-ingesting
- Allows independent pipeline execution and recovery
- Enables backfill of Silver layer without touching Bronze

Control Table Entry:
--------------------
Silver layer uses 'silver-gdelt-gkg' record in BRONZE.TABLE_CONTROL
(Note: Record name is different from Bronze's 'gdelt_gkg')

Dependencies:
-------------
- BRONZE.TABLE_CONTROL must exist and be initialized
- Databricks job context (dbutils) required
- PySpark environment

Output:
-------
Task value set for downstream notebooks:
  - next_date_to_process_gkg: Next date for Silver processing

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
print("SILVER LAYER - RETRIEVING GKG CONTROL DATE")
print("=" * 70)

# ============================================
# QUERY CONTROL TABLE
# ============================================
"""
Query TABLE_CONTROL for Silver layer processing status.

The TABLE_CONTROL table tracks the last successfully processed date
for each layer and table. This enables incremental processing.

Silver Layer Control Entry:
- TABLE_NAME: 'silver-gdelt-gkg'
- LAST_UPDATE_DATE: Last date successfully processed in Silver
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
    # EXTRACT SILVER LAYER CONTROL DATE
    # ============================================
    # Filter for Silver GKG record and extract last processed date
    # Note: Uses 'silver-gdelt-gkg' (not 'gdelt_gkg' like Bronze)
    gdelt_gkg_last_modified = df.filter(
        df.TABLE_NAME == 'silver-gdelt-gkg'
    ).select('LAST_UPDATE_DATE').collect()[0][0]

    # ============================================
    # CALCULATE NEXT DATE TO PROCESS
    # ============================================
    # Increment by 1 day to get next date for Silver processing
    # Silver processes data that was already ingested by Bronze
    next_date_to_process_gkg = (
        gdelt_gkg_last_modified + timedelta(1)
    ).strftime('%Y-%m-%d')

    # ============================================
    # SET TASK VALUE FOR DOWNSTREAM WORKFLOWS
    # ============================================
    # Databricks Workflows use task values to pass data between notebooks
    # This value will be consumed by 01_scrap_data.py
    dbutils.jobs.taskValues.set("next_date_to_process_gkg", next_date_to_process_gkg)

    # ============================================
    # LOGGING OUTPUT
    # ============================================
    print("✓ Control date retrieved successfully")
    print(f"  Silver Layer Record: silver-gdelt-gkg")
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
    print("  - silver-gdelt-gkg record")
    print("\nInitialization SQL:")
    print("""
    INSERT INTO BRONZE.TABLE_CONTROL (TABLE_NAME, LAST_UPDATE_DATE)
    VALUES ('silver-gdelt-gkg', '2023-01-01');
    """)
    print("No se encontró la última fecha procesada. Asegúrate de que la tabla de control está correctamente inicializada.")

    # Raise exception to fail the workflow and alert operators
    raise Exception("TABLE_CONTROL not properly initialized for Silver layer")

# ============================================
# END OF NOTEBOOK
# ============================================

"""
==============================================================================
SILVER LAYER PROCESSING FLOW
==============================================================================

Control Date → Scraping & Parsing → Update Control Date

This Silver workflow processes Bronze GKG data by:
1. Reading Bronze Delta tables (raw semi-structured data)
2. Parsing LOCATIONS, TONE fields into structured columns
3. Filtering for port-related news
4. Web scraping article content from source URLs
5. Writing to Silver Delta tables (cleaned, enriched data)

Next Step:
----------
01_scrap_data.py will use next_date_to_process_gkg to:
- Query Bronze GKG for that specific date
- Parse semi-structured fields
- Scrape article content
- Write to Silver Delta table

==============================================================================
"""
