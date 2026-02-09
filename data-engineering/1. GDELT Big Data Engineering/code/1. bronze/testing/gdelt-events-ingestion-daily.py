# Databricks notebook source
"""
==============================================================================
BRONZE LAYER - GDELT EVENTS DAILY INGESTION TESTING NOTEBOOK
==============================================================================

Purpose:
--------
This notebook was used to test the daily incremental ingestion pattern for
GDELT Events data. It demonstrates processing yesterday's data automatically,
which simulates the production workflow's scheduled execution.

Testing Approach:
-----------------
Unlike the historical backfill notebook (gdelt-events-ingestion.py), this
notebook focuses on testing the daily incremental pattern:
  - Automatically calculates yesterday's date
  - Downloads and processes single day
  - Tests merge/upsert logic with existing data
  - Validates that daily runs don't duplicate data

Use Cases:
----------
- Test daily incremental loading pattern
- Validate automatic date calculation (yesterday)
- Test merge logic with recent data
- Simulate production scheduled workflow
- Verify idempotent operations (safe to re-run)

Key Differences from Production Workflow:
------------------------------------------
- Production: Uses TABLE_CONTROL for date tracking
- Testing: Uses datetime.now() - timedelta(days=1)
- Production: Runs on 15-minute schedule
- Testing: Manual execution for validation
- Production: Has S3 intermediate storage
- Testing: Direct processing to Delta table

How This Maps to Production:
-----------------------------
This testing notebook validates the core logic that's used in:
  - 00_get_events_control_date.py (date calculation)
  - 01_download_and_unzip.py (download logic)
  - 02_upsert_delta_table.py (merge logic)
  - 03_update_events_control_date.py (success tracking)

Author: Neutrino Solutions Team
Date: 2024
==============================================================================
"""

# ============================================
# IMPORTS
# ============================================
import pandas as pd
import requests
import zipfile
import io
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# ============================================
# SPARK SESSION INITIALIZATION
# ============================================
# Initialize Spark for distributed data processing
spark = SparkSession.builder.appName("GKG Data Loader").getOrCreate()

# ============================================
# DELTA TABLE CONFIGURATION
# ============================================
# Delta table path for Bronze layer storage
# Empty string uses Databricks default location
delta_table_path = ""

# Check if Delta table already exists
table_exists = DeltaTable.isDeltaTable(spark, delta_table_path)

# ============================================
# INITIAL TABLE CREATION (IF NEEDED)
# ============================================
# Create table structure if this is the first run
if not table_exists:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze.gdelt_events
        USING DELTA
        LOCATION '{delta_table_path}'
    """)
    print("✓ Delta table structure created")

# ============================================
# CALCULATE YESTERDAY'S DATE
# ============================================
"""
Why Yesterday?
--------------
GDELT publishes daily files with a delay. The most reliable pattern is:
- Process yesterday's data (always available and complete)
- Avoids issues with today's data being incomplete
- Matches production pattern where workflows run on schedule

In production, this logic is replaced by TABLE_CONTROL queries that
track the last successfully processed date.
"""
current_date = datetime.now() - timedelta(days=1)
date_str = current_date.strftime('%Y%m%d')  # Format: YYYYMMDD

print(f"Processing date: {current_date.strftime('%Y-%m-%d')}")
print(f"URL date format: {date_str}")

# ============================================
# GDELT EVENTS SCHEMA DEFINITION
# ============================================
# Column names for GDELT Events 1.0 format (57 columns)
# Schema documentation: http://data.gdeltproject.org/documentation/
column_names = [
    # Event Identification (5 columns)
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",

    # Actor 1 Information (15 columns)
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode",
    "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",

    # Actor 2 Information (15 columns)
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
    "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",

    # Event Action (10 columns)
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",

    # Actor 1 Geography (7 columns)
    "Actor1Geo_Type", "Actor1Geo_Fullname", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",

    # Actor 2 Geography (7 columns)
    "Actor2Geo_Type",
    "Actor2Geo_Fullname", "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code",
    "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",

    # Action Geography (7 columns) - Critical for port analysis
    "ActionGeo_Type",
    "ActionGeo_Fullname", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",

    # Metadata (2 columns)
    "DATEADDED", "SOURCEURL"
]

# ============================================
# SPARK SCHEMA DEFINITION
# ============================================
# Explicit type definitions for schema enforcement
# This prevents type inference issues and ensures data quality
schema = StructType([
    # Event Identification
    StructField("GlobalEventID", LongType(), True),  # Primary key
    StructField("Day", IntegerType(), True),         # YYYYMMDD format
    StructField("MonthYear", IntegerType(), True),   # YYYYMM format
    StructField("Year", IntegerType(), True),
    StructField("FractionDate", FloatType(), True),  # Fractional year

    # Actor 1 Information
    StructField("Actor1Code", StringType(), True),
    StructField("Actor1Name", StringType(), True),
    StructField("Actor1CountryCode", StringType(), True),
    StructField("Actor1KnownGroupCode", StringType(), True),
    StructField("Actor1EthnicCode", StringType(), True),
    StructField("Actor1Religion1Code", StringType(), True),
    StructField("Actor1Religion2Code", StringType(), True),
    StructField("Actor1Type1Code", StringType(), True),
    StructField("Actor1Type2Code", StringType(), True),
    StructField("Actor1Type3Code", StringType(), True),

    # Actor 2 Information
    StructField("Actor2Code", StringType(), True),
    StructField("Actor2Name", StringType(), True),
    StructField("Actor2CountryCode", StringType(), True),
    StructField("Actor2KnownGroupCode", StringType(), True),
    StructField("Actor2EthnicCode", StringType(), True),
    StructField("Actor2Religion1Code", StringType(), True),
    StructField("Actor2Religion2Code", StringType(), True),
    StructField("Actor2Type1Code", StringType(), True),
    StructField("Actor2Type2Code", StringType(), True),
    StructField("Actor2Type3Code", StringType(), True),

    # Event Action
    StructField("IsRootEvent", IntegerType(), True),
    StructField("EventCode", IntegerType(), True),         # CAMEO event code
    StructField("EventBaseCode", IntegerType(), True),
    StructField("EventRootCode", IntegerType(), True),
    StructField("QuadClass", IntegerType(), True),         # Event classification
    StructField("GoldsteinScale", FloatType(), True),      # Impact score (-10 to +10)
    StructField("NumMentions", IntegerType(), True),       # Media coverage
    StructField("NumSources", IntegerType(), True),
    StructField("NumArticles", IntegerType(), True),
    StructField("AvgTone", FloatType(), True),             # Sentiment (-100 to +100)

    # Actor 1 Geography
    StructField("Actor1Geo_Type", IntegerType(), True),
    StructField("Actor1Geo_Fullname", StringType(), True),
    StructField("Actor1Geo_CountryCode", StringType(), True),
    StructField("Actor1Geo_ADM1Code", StringType(), True),
    StructField("Actor1Geo_Lat", FloatType(), True),
    StructField("Actor1Geo_Long", FloatType(), True),
    StructField("Actor1Geo_FeatureID", StringType(), True),

    # Actor 2 Geography
    StructField("Actor2Geo_Type", IntegerType(), True),
    StructField("Actor2Geo_Fullname", StringType(), True),
    StructField("Actor2Geo_CountryCode", StringType(), True),
    StructField("Actor2Geo_ADM1Code", StringType(), True),
    StructField("Actor2Geo_Lat", FloatType(), True),
    StructField("Actor2Geo_Long", FloatType(), True),
    StructField("Actor2Geo_FeatureID", StringType(), True),

    # Action Geography (Critical for port analysis)
    StructField("ActionGeo_Type", IntegerType(), True),
    StructField("ActionGeo_Fullname", StringType(), True),
    StructField("ActionGeo_CountryCode", StringType(), True),
    StructField("ActionGeo_ADM1Code", StringType(), True),
    StructField("ActionGeo_Lat", FloatType(), True),       # Latitude for geospatial filtering
    StructField("ActionGeo_Long", FloatType(), True),      # Longitude for geospatial filtering
    StructField("ActionGeo_FeatureID", StringType(), True),

    # Metadata
    StructField("DATEADDED", LongType(), True),            # Date article was added to GDELT
    StructField("SOURCEURL", StringType(), True),          # Source article URL
    StructField("extraction_date", DateType(), True)       # Pipeline extraction date
])

print(f"✓ Schema defined: {len(schema.fields)} columns")

# ============================================
# DOWNLOAD AND PROCESS YESTERDAY'S DATA
# ============================================
# Construct GDELT download URL
# URL pattern: http://data.gdeltproject.org/events/YYYYMMDD.export.CSV.zip
url = f"http://data.gdeltproject.org/events/{date_str}.export.CSV.zip"

try:
    # ============================================
    # STEP 1: DOWNLOAD ZIP FILE
    # ============================================
    print(f"Downloading from: {url}")
    response = requests.get(url)
    response.raise_for_status()  # Raise exception for HTTP errors (4xx, 5xx)
    print(f"✓ Downloaded {len(response.content) / 1024 / 1024:.2f} MB")

    # ============================================
    # STEP 2: EXTRACT CSV FROM ZIP
    # ============================================
    # Process ZIP in-memory (no disk I/O)
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    csv_file_name = zip_file.namelist()[0]  # Should be YYYYMMDD.export.CSV
    csv_file = zip_file.open(csv_file_name)
    print(f"✓ Extracted: {csv_file_name}")

    # ============================================
    # STEP 3: LOAD CSV TO PANDAS DATAFRAME
    # ============================================
    # Parse tab-delimited CSV with explicit column names
    df = pd.read_csv(csv_file, sep='\t', header=None, names=column_names)
    print(f"✓ Loaded {len(df):,} events")

    # ============================================
    # STEP 4: TYPE CONVERSION - NUMERIC COLUMNS
    # ============================================
    # Convert numeric columns with error handling (coerce invalid → NaN)
    numeric_columns = [
        'GlobalEventID', 'Day', 'MonthYear', 'Year', 'FractionDate', 'IsRootEvent',
        'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass', 'GoldsteinScale',
        'NumMentions', 'NumSources', 'NumArticles', 'AvgTone',
        'Actor1Geo_Type', 'Actor1Geo_Lat', 'Actor1Geo_Long',
        'Actor2Geo_Type', 'Actor2Geo_Lat', 'Actor2Geo_Long',
        'ActionGeo_Type', 'ActionGeo_Lat', 'ActionGeo_Long',
        'DATEADDED'
    ]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    # ============================================
    # STEP 5: TYPE CONVERSION - STRING COLUMNS
    # ============================================
    # Convert code/identifier columns to strings
    string_columns = [
        'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode', 'Actor1EthnicCode',
        'Actor1Religion1Code', 'Actor1Religion2Code', 'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code',
        'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode', 'Actor2EthnicCode',
        'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code', 'Actor1Geo_Fullname', 'Actor1Geo_CountryCode',
        'Actor1Geo_ADM1Code', 'Actor1Geo_FeatureID', 'Actor2Geo_Fullname',
        'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code', 'Actor2Geo_FeatureID',
        'ActionGeo_Fullname', 'ActionGeo_CountryCode', 'ActionGeo_ADM1Code',
        'ActionGeo_FeatureID', 'SOURCEURL'
    ]
    df[string_columns] = df[string_columns].astype(str)
    print("✓ Type conversions completed")

    # ============================================
    # STEP 6: ADD METADATA COLUMN
    # ============================================
    # Track when this data was extracted for data lineage
    df['extraction_date'] = current_date.date()

    # ============================================
    # STEP 7: CONVERT TO SPARK DATAFRAME
    # ============================================
    # Convert pandas to Spark with schema enforcement
    spark_df = spark.createDataFrame(df, schema=schema)
    print(f"✓ Converted to Spark DataFrame")

    # ============================================
    # STEP 8: UPSERT TO DELTA TABLE
    # ============================================
    """
    Delta Lake Merge Operation:
    - Match condition: GlobalEventID (primary key)
    - If match found: UPDATE all columns (handles late-arriving data)
    - If no match: INSERT new record

    This idempotent operation ensures:
    - No duplicates
    - Safe to re-run
    - Late-arriving data updates are captured
    """
    if table_exists:
        # Perform merge for incremental updates
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        delta_table.alias("tgt").merge(
                source=spark_df.alias("src"),
                condition="tgt.GlobalEventID = src.GlobalEventID"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"✓ Data merged successfully for {date_str}")
    else:
        # Create table on first run
        spark_df.write.format("delta").mode("overwrite").save(delta_table_path)
        print(f"✓ Table created with data for {date_str}")
        table_exists = True

    # ============================================
    # SUCCESS MESSAGE
    # ============================================
    print("=" * 70)
    print("SUCCESS: Daily ingestion test completed")
    print("=" * 70)
    print(f"  Date processed: {current_date.strftime('%Y-%m-%d')}")
    print(f"  Records: {len(df):,}")
    print(f"  Table: bronze.gdelt_events")

except requests.exceptions.RequestException as e:
    # ============================================
    # ERROR HANDLING: DOWNLOAD/NETWORK ERRORS
    # ============================================
    print("=" * 70)
    print("ERROR: Download or extraction failed")
    print("=" * 70)
    print(f"  Date: {date_str}")
    print(f"  URL: {url}")
    print(f"  Exception: {e}")
    print("\nTroubleshooting:")
    print("  1. Check internet connectivity")
    print("  2. Verify GDELT servers are accessible")
    print("  3. Confirm date format is correct (YYYYMMDD)")

except Exception as e:
    # ============================================
    # ERROR HANDLING: PROCESSING ERRORS
    # ============================================
    print("=" * 70)
    print("ERROR: Data processing failed")
    print("=" * 70)
    print(f"  Date: {date_str}")
    print(f"  Exception: {e}")
    print("\nTroubleshooting:")
    print("  1. Check schema compatibility")
    print("  2. Verify Delta table permissions")
    print("  3. Review data quality issues")

# ============================================
# FINALIZATION
# ============================================
# Stop Spark session to release cluster resources
spark.stop()
print("✓ Spark session stopped")


# COMMAND ----------


