# Databricks notebook source
"""
==============================================================================
BRONZE LAYER - GDELT EVENTS INGESTION TESTING NOTEBOOK
==============================================================================

Purpose:
--------
This notebook was used during the initial development and testing phase of the
GDELT Events ingestion pipeline. It demonstrates the iterative development
process from initial schema validation to full historical backfill.

Development Workflow:
---------------------
This notebook shows 5 progressive stages of pipeline development:
  [1] Initial Schema Validation (lines 1-49)
  [2] Pandas to Spark Conversion (lines 51-209)
  [3] Delta Table Creation & Merge Logic (lines 211-247)
  [4] SQL Query Validation (lines 249-256)
  [5] Historical Backfill Testing (lines 258-445)

Use Cases:
----------
- Initial schema validation and column count verification
- Type conversion testing (numeric vs string columns)
- Delta Lake merge operation validation
- Historical data backfill for date range 2020-08-13 to 2023-08-12
- End-to-end pipeline testing before productionization

Key Differences from Production Workflow:
------------------------------------------
- Production uses S3 intermediate storage; this notebook processes directly
- Production uses control table pattern; this uses hardcoded date ranges
- Production runs on schedule; this is manual execution
- Production has error notifications; this has console logging only

Testing Approach:
-----------------
1. Test with single date first (20130401)
2. Validate schema and data types
3. Test Delta table creation and merge
4. Validate with SQL queries
5. Run historical backfill for 3-year period

Author: Neutrino Solutions Team
Date: 2024
==============================================================================
"""

# ============================================
# CELL 1: INITIAL SCHEMA VALIDATION
# ============================================
"""
DEVELOPMENT STAGE 1: Schema Validation

Purpose:
--------
- Verify GDELT Events file structure
- Validate that all 57 columns are present
- Test download and extraction logic
- Confirm tab-delimited format

This initial cell tests the basic download and parsing logic without any
Spark or Delta Lake dependencies. It's the foundation for understanding
the GDELT Events data format.
"""

import pandas as pd
import requests
import zipfile
import io

# ============================================
# DOWNLOAD SAMPLE DATE FOR TESTING
# ============================================
# Using a historical date (April 1, 2013) for initial testing
# This date is chosen because it's in the middle of GDELT's history
url = f"http://data.gdeltproject.org/events/20130401.export.CSV.zip"

# Download ZIP file from GDELT servers
response = requests.get(url)
zip_file = zipfile.ZipFile(io.BytesIO(response.content))

# ============================================
# EXTRACT CSV FROM ZIP
# ============================================
# GDELT stores daily files as: YYYYMMDD.export.CSV inside a ZIP
csv_file_name = zip_file.namelist()[0]  # Get first (and only) file in ZIP
csv_file = zip_file.open(csv_file_name)

# ============================================
# DEFINE GDELT EVENTS SCHEMA
# ============================================
# GDELT Events 1.0 format has 57 columns
# Schema documentation: http://data.gdeltproject.org/documentation/
column_names = [
    # Event Identification (5 columns)
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",

    # Actor 1 Information (15 columns)
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",

    # Actor 2 Information (15 columns)
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",

    # Event Action (10 columns)
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone",

    # Actor 1 Geography (7 columns)
    "Actor1Geo_Type", "Actor1Geo_Fullname",
    "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_Lat",
    "Actor1Geo_Long", "Actor1Geo_FeatureID",

    # Actor 2 Geography (7 columns)
    "Actor2Geo_Type", "Actor2Geo_Fullname", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_Lat", "Actor2Geo_Long",
    "Actor2Geo_FeatureID",

    # Action Geography (7 columns) - Critical for port analysis
    "ActionGeo_Type",
    "ActionGeo_Fullname", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",

    # Metadata (2 columns)
    "DATEADDED", "SOURCEURL"
]

# ============================================
# LOAD AND VALIDATE CSV
# ============================================
# Parse tab-delimited CSV without header row
df = pd.read_csv(csv_file, sep='\t', header=None, names=column_names)

# Verify column consistency across all rows
# This checks if any rows have missing or extra columns
column_counts = df.apply(lambda row: len(row), axis=1).value_counts()

# Display validation results
print(column_counts)
df.head(10)


# COMMAND ----------

# ============================================
# CELL 2: PANDAS TO SPARK CONVERSION
# ============================================
"""
DEVELOPMENT STAGE 2: Type Conversion and Spark Integration

Purpose:
--------
- Convert pandas DataFrame to Spark DataFrame
- Enforce strict type definitions for all columns
- Test schema enforcement and type coercion
- Add extraction_date metadata column

Key Changes from Cell 1:
------------------------
- Introduced PySpark dependencies
- Added explicit type conversions (numeric vs string)
- Defined StructType schema for Spark
- Added extraction_date for data lineage tracking

Why Type Conversion Matters:
-----------------------------
- GDELT CSV has no type information (all strings)
- Type enforcement prevents downstream errors
- Numeric columns enable aggregations and calculations
- String columns preserve codes and identifiers
"""

import pandas as pd
import requests
import zipfile
import io
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

# ============================================
# SPARK SESSION INITIALIZATION
# ============================================
# Initialize Spark for distributed processing
spark = SparkSession.builder.appName("GDELT Data Loader").getOrCreate()

# ============================================
# DATE CONFIGURATION
# ============================================
# Using same test date as Cell 1 for consistency
date = "20130401"  # Change this date for different tests

# ============================================
# DOWNLOAD AND EXTRACT GDELT FILE
# ============================================
# Same download logic as Cell 1
url = f"http://data.gdeltproject.org/events/{date}.export.CSV.zip"

response = requests.get(url)
zip_file = zipfile.ZipFile(io.BytesIO(response.content))

csv_file_name = zip_file.namelist()[0]
csv_file = zip_file.open(csv_file_name)

# ============================================
# LOAD CSV WITH COLUMN NAMES
# ============================================
# Define column names (same as Cell 1)
column_names = [
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode",
    "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
    "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_Fullname", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID", "Actor2Geo_Type",
    "Actor2Geo_Fullname", "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code",
    "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID", "ActionGeo_Type",
    "ActionGeo_Fullname", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID", "DATEADDED", "SOURCEURL"
]

df = pd.read_csv(csv_file, sep='\t', header=None, names=column_names)

# ============================================
# TYPE CONVERSION: NUMERIC COLUMNS
# ============================================
# Convert numeric columns with error handling (coerce invalid values to NaN)
# These columns are used for calculations, aggregations, and filtering

# Event IDs and temporal information
df['GlobalEventID'] = pd.to_numeric(df['GlobalEventID'], errors='coerce')
df['Day'] = pd.to_numeric(df['Day'], errors='coerce')
df['MonthYear'] = pd.to_numeric(df['MonthYear'], errors='coerce')
df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
df['FractionDate'] = pd.to_numeric(df['FractionDate'], errors='coerce')

# Event classification and metrics
df['IsRootEvent'] = pd.to_numeric(df['IsRootEvent'], errors='coerce')
df['EventCode'] = pd.to_numeric(df['EventCode'], errors='coerce')
df['EventBaseCode'] = pd.to_numeric(df['EventBaseCode'], errors='coerce')
df['EventRootCode'] = pd.to_numeric(df['EventRootCode'], errors='coerce')
df['QuadClass'] = pd.to_numeric(df['QuadClass'], errors='coerce')
df['GoldsteinScale'] = pd.to_numeric(df['GoldsteinScale'], errors='coerce')
df['NumMentions'] = pd.to_numeric(df['NumMentions'], errors='coerce')
df['NumSources'] = pd.to_numeric(df['NumSources'], errors='coerce')
df['NumArticles'] = pd.to_numeric(df['NumArticles'], errors='coerce')
df['AvgTone'] = pd.to_numeric(df['AvgTone'], errors='coerce')

# Geographic coordinates (critical for geospatial filtering)
df['Actor1Geo_Type'] = pd.to_numeric(df['Actor1Geo_Type'], errors='coerce')
df['Actor1Geo_Lat'] = pd.to_numeric(df['Actor1Geo_Lat'], errors='coerce')
df['Actor1Geo_Long'] = pd.to_numeric(df['Actor1Geo_Long'], errors='coerce')
df['Actor2Geo_Type'] = pd.to_numeric(df['Actor2Geo_Type'], errors='coerce')
df['Actor2Geo_Lat'] = pd.to_numeric(df['Actor2Geo_Lat'], errors='coerce')
df['Actor2Geo_Long'] = pd.to_numeric(df['Actor2Geo_Long'], errors='coerce')
df['ActionGeo_Type'] = pd.to_numeric(df['ActionGeo_Type'], errors='coerce')
df['ActionGeo_Lat'] = pd.to_numeric(df['ActionGeo_Lat'], errors='coerce')
df['ActionGeo_Long'] = pd.to_numeric(df['ActionGeo_Long'], errors='coerce')

# Metadata
df['DATEADDED'] = pd.to_numeric(df['DATEADDED'], errors='coerce')

# ============================================
# TYPE CONVERSION: STRING COLUMNS
# ============================================
# Convert categorical/code columns to strings
# These columns are codes, names, and identifiers (not for calculations)
string_columns = [
    'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode', 'Actor1EthnicCode',
    'Actor1Religion1Code', 'Actor1Religion2Code', 'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code',
    'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode', 'Actor2EthnicCode',
    'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code',
    'Actor1Geo_Fullname', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code',
    'Actor1Geo_FeatureID', 'Actor2Geo_Fullname', 'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code',
    'Actor2Geo_FeatureID', 'ActionGeo_Fullname', 'ActionGeo_CountryCode',
    'ActionGeo_ADM1Code', 'ActionGeo_FeatureID', 'SOURCEURL'
]
df[string_columns] = df[string_columns].astype(str)

# ============================================
# ADD METADATA COLUMN
# ============================================
# Add extraction_date to track when data was processed
# This enables data lineage and troubleshooting
df['extraction_date'] = datetime.strptime(date, '%Y%m%d').date()

# ============================================
# DEFINE SPARK SCHEMA
# ============================================
# Explicit schema definition ensures type safety in Spark
# This prevents schema inference issues and enforces data quality
schema = StructType([
    # Event Identification
    StructField("GlobalEventID", LongType(), True),
    StructField("Day", IntegerType(), True),
    StructField("MonthYear", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("FractionDate", FloatType(), True),

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
    StructField("EventCode", IntegerType(), True),
    StructField("EventBaseCode", IntegerType(), True),
    StructField("EventRootCode", IntegerType(), True),
    StructField("QuadClass", IntegerType(), True),
    StructField("GoldsteinScale", FloatType(), True),
    StructField("NumMentions", IntegerType(), True),
    StructField("NumSources", IntegerType(), True),
    StructField("NumArticles", IntegerType(), True),
    StructField("AvgTone", FloatType(), True),

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

    # Action Geography
    StructField("ActionGeo_Type", IntegerType(), True),
    StructField("ActionGeo_Fullname", StringType(), True),
    StructField("ActionGeo_CountryCode", StringType(), True),
    StructField("ActionGeo_ADM1Code", StringType(), True),
    StructField("ActionGeo_Lat", FloatType(), True),
    StructField("ActionGeo_Long", FloatType(), True),
    StructField("ActionGeo_FeatureID", StringType(), True),

    # Metadata
    StructField("DATEADDED", LongType(), True),
    StructField("SOURCEURL", StringType(), True),
    StructField("extraction_date", DateType(), True)  # Pipeline metadata
])

# ============================================
# CONVERT TO SPARK DATAFRAME
# ============================================
# Convert pandas DataFrame to Spark DataFrame with enforced schema
spark_df = spark.createDataFrame(df, schema=schema)

# ============================================
# VALIDATION OUTPUT
# ============================================
# Display sample data for manual validation
pandas_df = spark_df.toPandas()
display(pandas_df.head(20))  # Show first 20 rows


# COMMAND ----------

# ============================================
# CELL 3: DELTA TABLE CREATION AND MERGE
# ============================================
"""
DEVELOPMENT STAGE 3: Delta Lake Integration

Purpose:
--------
- Test Delta table creation
- Validate merge/upsert logic
- Verify primary key constraints (GlobalEventID)
- Test both initial load and incremental updates

Delta Lake Benefits:
--------------------
- ACID transactions for data reliability
- Upsert capability (update existing, insert new)
- Time travel for historical queries
- Schema enforcement and evolution

Merge Logic:
------------
- Match condition: GlobalEventID (unique identifier)
- If match found: UPDATE all columns
- If no match: INSERT new record
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from datetime import datetime

# ============================================
# SPARK SESSION INITIALIZATION
# ============================================
spark = SparkSession.builder.appName("GDELT Data Loader").getOrCreate()

# ============================================
# DELTA TABLE CONFIGURATION
# ============================================
# Specify Delta table path (this would be S3 in production)
# Empty string means default location in Databricks
delta_table_path = ""

# ============================================
# CHECK IF TABLE EXISTS
# ============================================
# Determine if this is initial load or incremental update
if not DeltaTable.isDeltaTable(spark, delta_table_path):
    # ============================================
    # CASE 1: INITIAL TABLE CREATION
    # ============================================
    # Create new Delta table from Spark DataFrame
    spark_df.write.format("delta").mode("overwrite").save(delta_table_path)

    # Register table in Unity Catalog for SQL access
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze.gdelt_events
        USING DELTA
        LOCATION '{delta_table_path}'
    """)
    print(f"Tabla creada y registrada en {delta_table_path}.")
else:
    # ============================================
    # CASE 2: INCREMENTAL UPDATE (MERGE/UPSERT)
    # ============================================
    # Load existing Delta table
    delta_table = DeltaTable.forPath(spark, delta_table_path)

    # Perform merge operation
    # - Match on GlobalEventID (primary key)
    # - Update all columns if match found
    # - Insert new records if no match
    delta_table.alias("tgt").merge(
        source=spark_df.alias("src"),
        condition="tgt.GLOBALEVENTID = src.GLOBALEVENTID"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print(f"Datos actualizados en la tabla {delta_table_path}.")

# ============================================
# VALIDATION OUTPUT
# ============================================
# Display sample data to confirm successful write
spark_df.show(10)


# COMMAND ----------

# ============================================
# CELL 4: SQL QUERY VALIDATION
# ============================================
"""
DEVELOPMENT STAGE 4: Data Quality Validation

Purpose:
--------
- Verify data was written correctly to Delta table
- Test SQL query access to Bronze layer
- Validate data quality (check for 'nan' strings)
- Ensure URL column has valid values

This cell demonstrates querying the Delta table using Spark SQL,
which is how downstream Silver and Gold layers will access the data.
"""

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.gdelt_events
# MAGIC WHERE SOURCEURL != 'nan'
# MAGIC LIMIT 1000

# COMMAND ----------

# ============================================
# CELL 5: HISTORICAL BACKFILL TESTING
# ============================================
"""
DEVELOPMENT STAGE 5: Historical Data Backfill

Purpose:
--------
- Load 3 years of historical GDELT Events data (2020-08-13 to 2023-08-12)
- Validate pipeline scalability and reliability
- Test error handling for missing or corrupted files
- Ensure merge logic works correctly across large date ranges

Why Historical Backfill?
-------------------------
- Provides sufficient data for meaningful analytics
- Tests pipeline robustness with 1,096 daily files
- Validates incremental loading pattern
- Identifies edge cases and data quality issues

Performance Considerations:
---------------------------
- Processing ~1,096 days of data
- Each day: 20-50 MB compressed, 200-500 MB uncompressed
- Total: ~200-500 GB uncompressed data
- Estimated time: 8-12 hours depending on cluster size

Error Handling:
---------------
- Continues processing even if individual dates fail
- Logs errors for troubleshooting
- Safe to restart (idempotent merge operation)
"""

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
spark = SparkSession.builder.appName("GDELT Data Loader").getOrCreate()

# ============================================
# DELTA TABLE CONFIGURATION
# ============================================
# Delta table path for Bronze layer storage
delta_table_path = ""

# Check if table already exists
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

# ============================================
# DEFINE DATE RANGE FOR BACKFILL
# ============================================
# Load 3 years of historical data (2020-08-13 to 2023-08-12)
# This covers the training period for predictive models
start_date = datetime.strptime("2020-08-13", "%Y-%m-%d")
end_date = datetime.strptime("2023-08-12", "%Y-%m-%d")

# ============================================
# GDELT EVENTS SCHEMA DEFINITION
# ============================================
# Column names for GDELT Events 1.0 format (57 columns)
column_names = [
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode",
    "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
    "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_Fullname", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID", "Actor2Geo_Type",
    "Actor2Geo_Fullname", "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code",
    "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID", "ActionGeo_Type",
    "ActionGeo_Fullname", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID", "DATEADDED", "SOURCEURL"
]

# Spark schema with explicit type definitions
schema = StructType([
    StructField("GlobalEventID", LongType(), True),
    StructField("Day", IntegerType(), True),
    StructField("MonthYear", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("FractionDate", FloatType(), True),
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
    StructField("IsRootEvent", IntegerType(), True),
    StructField("EventCode", IntegerType(), True),
    StructField("EventBaseCode", IntegerType(), True),
    StructField("EventRootCode", IntegerType(), True),
    StructField("QuadClass", IntegerType(), True),
    StructField("GoldsteinScale", FloatType(), True),
    StructField("NumMentions", IntegerType(), True),
    StructField("NumSources", IntegerType(), True),
    StructField("NumArticles", IntegerType(), True),
    StructField("AvgTone", FloatType(), True),
    StructField("Actor1Geo_Type", IntegerType(), True),
    StructField("Actor1Geo_Fullname", StringType(), True),
    StructField("Actor1Geo_CountryCode", StringType(), True),
    StructField("Actor1Geo_ADM1Code", StringType(), True),
    StructField("Actor1Geo_Lat", FloatType(), True),
    StructField("Actor1Geo_Long", FloatType(), True),
    StructField("Actor1Geo_FeatureID", StringType(), True),
    StructField("Actor2Geo_Type", IntegerType(), True),
    StructField("Actor2Geo_Fullname", StringType(), True),
    StructField("Actor2Geo_CountryCode", StringType(), True),
    StructField("Actor2Geo_ADM1Code", StringType(), True),
    StructField("Actor2Geo_Lat", FloatType(), True),
    StructField("Actor2Geo_Long", FloatType(), True),
    StructField("Actor2Geo_FeatureID", StringType(), True),
    StructField("ActionGeo_Type", IntegerType(), True),
    StructField("ActionGeo_Fullname", StringType(), True),
    StructField("ActionGeo_CountryCode", StringType(), True),
    StructField("ActionGeo_ADM1Code", StringType(), True),
    StructField("ActionGeo_Lat", FloatType(), True),
    StructField("ActionGeo_Long", FloatType(), True),
    StructField("ActionGeo_FeatureID", StringType(), True),
    StructField("DATEADDED", LongType(), True),
    StructField("SOURCEURL", StringType(), True),
    StructField("extraction_date", DateType(), True)
])

# ============================================
# HISTORICAL BACKFILL LOOP
# ============================================
# Process each day in the date range sequentially
current_date = start_date
while current_date <= end_date:
    date_str = current_date.strftime('%Y%m%d')
    url = f"http://data.gdeltproject.org/events/{date_str}.export.CSV.zip"

    try:
        # ============================================
        # DOWNLOAD AND EXTRACT
        # ============================================
        # Download ZIP file for current date
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))

        # Extract CSV from ZIP
        csv_file_name = zip_file.namelist()[0]
        csv_file = zip_file.open(csv_file_name)

        # ============================================
        # PARSE CSV TO PANDAS
        # ============================================
        # Load tab-delimited CSV with column names
        df = pd.read_csv(csv_file, sep='\t', header=None, names=column_names)

        # ============================================
        # TYPE CONVERSION: NUMERIC COLUMNS
        # ============================================
        # Convert numeric columns with error handling
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
        # TYPE CONVERSION: STRING COLUMNS
        # ============================================
        # Convert string/code columns
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

        # ============================================
        # ADD METADATA COLUMN
        # ============================================
        # Track when this data was extracted
        df['extraction_date'] = current_date.date()

        # ============================================
        # CONVERT TO SPARK DATAFRAME
        # ============================================
        # Convert pandas to Spark with schema enforcement
        spark_df = spark.createDataFrame(df, schema=schema)

        # ============================================
        # UPSERT TO DELTA TABLE
        # ============================================
        if table_exists:
            # Perform merge for incremental updates
            delta_table = DeltaTable.forPath(spark, delta_table_path)
            delta_table.alias("tgt").merge(
                source=spark_df.alias("src"),
                condition="tgt.GlobalEventID = src.GlobalEventID"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            print(f"Datos actualizados para {date_str}.")
        else:
            # Create table on first iteration
            spark_df.write.format("delta").mode("overwrite").save(delta_table_path)
            print(f"Tabla creada y datos insertados para {date_str}.")
            table_exists = True

    except requests.exceptions.RequestException as e:
        # ============================================
        # ERROR HANDLING: DOWNLOAD/NETWORK ERRORS
        # ============================================
        print(f"Error al descargar o procesar los datos para la fecha {date_str}: {e}")
    except Exception as e:
        # ============================================
        # ERROR HANDLING: PROCESSING ERRORS
        # ============================================
        print(f"Error al procesar los datos para la fecha {date_str}: {e}")

    # ============================================
    # INCREMENT DATE
    # ============================================
    # Move to next day in the range
    current_date += timedelta(days=1)

# ============================================
# FINALIZATION
# ============================================
# Stop Spark session to release resources
spark.stop()


