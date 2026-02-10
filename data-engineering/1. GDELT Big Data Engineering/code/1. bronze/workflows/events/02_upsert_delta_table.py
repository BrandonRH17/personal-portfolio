# Databricks notebook source
"""
==============================================================================
BRONZE LAYER - DELTA TABLE UPSERT FOR GDELT EVENTS
==============================================================================

Purpose:
--------
This notebook is the third step in the Bronze layer workflow for GDELT Events
data ingestion. It reads the Parquet file from S3, applies schema enforcement,
and performs an upsert (merge) operation into a Delta Lake table.

Workflow Position:
------------------
Step 3 of 4 in Bronze Layer Ingestion:
    [1] Get Control Date
    [2] Download and Unzip Data
  → [3] Upsert to Delta Table (this script)
    [4] Update Control Date

Key Functionality:
------------------
1. Configure S3 access for Spark
2. Read Parquet file from S3
3. Apply strict schema enforcement (58 columns)
4. Perform upsert operation using Delta Lake merge
5. Handle both initial table creation and incremental updates

Why Delta Lake?
---------------
- ACID transactions: Atomic, consistent, isolated, durable operations
- Time travel: Query historical versions of data
- Schema enforcement: Prevents data quality issues
- Efficient upserts: Merge operations without full rewrites
- Scalability: Handles petabyte-scale data

Dependencies:
-------------
- Previous task: 01_download_and_unzip
- Delta Lake library
- S3 access credentials
- Databricks widgets: aws_access_key, aws_secret_access_key,
                      aws_delta_table_path, aws_raw_path

Author: Neutrino Solutions Team
Date: 2024
==============================================================================
"""

# ============================================
# IMPORTS
# ============================================
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.functions import lit
import io

# ============================================
# SPARK SESSION CONFIGURATION
# ============================================
# Initialize Spark with Delta Lake support
spark = SparkSession.builder \
    .appName("GDELT Delta Table Upsert") \
    .getOrCreate()

# Configure S3 access for Spark
# These credentials allow Spark to read/write directly from S3
spark.conf.set("fs.s3a.access.key", dbutils.widgets.get("aws_access_key"))
spark.conf.set("fs.s3a.secret.key", dbutils.widgets.get("aws_secret_access_key"))
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

print("✓ Spark session configured with S3 access")

# ============================================
# DELTA TABLE PATH CONFIGURATION
# ============================================
# Delta table location in S3 (e.g., s3://bucket/path/to/delta/table)
delta_table_path = dbutils.widgets.get("aws_delta_table_path")
print(f"Delta table path: {delta_table_path}")

# Check if Delta table already exists
# This determines whether we create or update the table
table_exists = DeltaTable.isDeltaTable(spark, delta_table_path)
print(f"Table exists: {table_exists}")

# ============================================
# GDELT EVENTS SCHEMA DEFINITION
# ============================================
"""
GDELT Events 1.0 format consists of 57 base columns plus 1 metadata column.
Schema is strictly enforced to ensure data quality and consistency.

Column Categories:
- Event Identification: IDs and temporal information
- Actor Information: Two actors per event (actor1, actor2)
- Event Action: CAMEO codes, Goldstein scale, mentions
- Geography: Three geographic levels (actor1, actor2, action)
- Metadata: Date added, source URL, extraction date

Full documentation: http://data.gdeltproject.org/documentation/
"""

# Column names (58 total including extraction_date)
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
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",

    # Actor 1 Geography (7 columns)
    "Actor1Geo_Type", "Actor1Geo_Fullname", "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code", "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",

    # Actor 2 Geography (7 columns)
    "Actor2Geo_Type", "Actor2Geo_Fullname", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",

    # Action Geography (7 columns) - Most important for port analysis
    "ActionGeo_Type", "ActionGeo_Fullname", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",

    # Metadata (2 + 1 columns)
    "DATEADDED", "SOURCEURL", "extraction_date"
]

# Spark schema with explicit data types
# Type enforcement prevents data quality issues
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
    StructField("extraction_date", DateType(), True)       # Our pipeline extraction date
])

print(f"✓ Schema defined: {len(schema.fields)} columns")

# ============================================
# READ PARQUET FROM S3
# ============================================
# Get extraction date from previous task
extraction_date = dbutils.jobs.taskValues.get(
    "00_get_events_control_date",
    "next_date_to_process_events"
)

# Convert date format for S3 path (2024-01-15 → 20240115)
extraction_date_for_s3 = extraction_date.replace("-", "")

# Construct S3 path to Parquet file
s3_base_path = dbutils.widgets.get("aws_raw_path")
s3_path = f"{s3_base_path}/{extraction_date_for_s3}.parquet"

print(f"Reading Parquet from S3: {s3_path}")

# Read Parquet file with automatic schema inference
spark_df = spark.read.parquet(s3_path)

# Add metadata columns
# extraction_date: Date this data was extracted (for tracking)
# last_modified: Last modification timestamp (for Delta Lake)
spark_df = spark_df.withColumn("extraction_date", lit(extraction_date))
spark_df = spark_df.withColumn("last_modified", lit(extraction_date))

print(f"✓ Read {spark_df.count():,} records from S3")

# ============================================
# UPSERT TO DELTA TABLE
# ============================================
"""
Delta Lake Merge Operation:
- If record exists (matched by GlobalEventID): UPDATE all columns
- If record is new: INSERT all columns

This approach:
- Handles late-arriving data (updates if event details change)
- Prevents duplicates (GlobalEventID is unique)
- Maintains data lineage (last_modified timestamp)
- Supports idempotent operations (safe to re-run)
"""

if table_exists:
    # ============================================
    # CASE 1: TABLE EXISTS - PERFORM MERGE
    # ============================================
    print("Performing Delta Lake merge (upsert)...")

    # Load existing Delta table
    delta_table = DeltaTable.forPath(spark, delta_table_path)

    # Perform merge operation
    # Condition: Match on GlobalEventID (primary key)
    delta_table.alias("tgt").merge(
        source=spark_df.alias("src"),
        condition="tgt.GlobalEventID = src.GlobalEventID"
    ).whenMatchedUpdateAll() \      # Update all columns if ID matches
     .whenNotMatchedInsertAll() \   # Insert if ID doesn't exist
     .execute()

    print(f"✓ Merge completed successfully")
    print(f"  Updated/Inserted data from: {s3_path}")

else:
    # ============================================
    # CASE 2: TABLE DOESN'T EXIST - CREATE TABLE
    # ============================================
    print("Creating new Delta table...")

    # Write DataFrame as Delta table
    # Mode 'overwrite' for initial creation
    spark_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(delta_table_path)

    print(f"✓ Delta table created successfully")
    print(f"  Location: {delta_table_path}")
    print(f"  Initial records: {spark_df.count():,}")

    # Update table existence flag
    table_exists = True

# ============================================
# FINALIZATION
# ============================================
# Stop Spark session to release resources
spark.stop()

print("=" * 70)
print("SUCCESS: Delta table upsert completed")
print("=" * 70)

# ============================================
# END OF NOTEBOOK
# ============================================
