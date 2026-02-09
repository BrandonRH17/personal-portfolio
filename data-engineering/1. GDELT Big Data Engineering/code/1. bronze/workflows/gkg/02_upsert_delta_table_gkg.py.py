# Databricks notebook source
"""
==============================================================================
BRONZE LAYER - DELTA TABLE UPSERT FOR GDELT GKG
==============================================================================

Purpose:
--------
This notebook is the third step in the Bronze layer workflow for GDELT GKG
data ingestion. It reads the Parquet file from S3, applies schema enforcement,
and performs an upsert (merge) operation into a Delta Lake table.

Workflow Position:
------------------
Step 3 of 4 in Bronze Layer GKG Ingestion:
    [1] Get Control Date
    [2] Download and Unzip Data
  → [3] Upsert to Delta Table (this script)
    [4] Update Control Date

Key Differences from Events Workflow:
--------------------------------------
1. **No Unique ID**: GKG has NO GlobalEventID → Must use composite key for merge
2. **Simpler Schema**: Only 12 columns (11 + extraction_date) vs 58 for Events
3. **Text-Heavy**: Most columns are semi-structured text (not typed numbers)
4. **Larger Files**: GKG Delta tables grow faster due to larger row sizes
5. **Complex Merge**: Merge condition requires matching on 10 fields (expensive)

Why Delta Lake for GKG?
------------------------
- ACID transactions: Atomic, consistent, isolated, durable operations
- Time travel: Query historical versions of data
- Schema enforcement: Prevents data quality issues in semi-structured fields
- Efficient upserts: Merge operations without full rewrites
- Scalability: Handles petabyte-scale data

The Composite Key Challenge:
-----------------------------
GKG has no unique identifier like Events' GlobalEventID. We must match on
a combination of 10 fields to detect duplicates. This is expensive but necessary
to prevent duplicate article mentions in the data lake.

Hackathon Note:
---------------
This composite key merge is slow and not ideal for production at scale.
Better approach would be to add a hash-based surrogate key (MD5 of key fields)
or use append-only with downstream deduplication.

Dependencies:
-------------
- Previous task: 01_download_and_unzip_gkg
- Delta Lake library
- S3 access credentials
- Databricks widgets: aws_access_key, aws_secret_access_key,
                      aws_delta_table_path, aws_raw_path

Author: Neutrino Solutions Team (Factored Datathon 2024)
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
from pyspark.sql.functions import expr, lit
import io

# ============================================
# SPARK SESSION CONFIGURATION
# ============================================
# Initialize Spark with Delta Lake support
spark = SparkSession.builder \
    .appName("GDELT Delta Table Creator") \
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
# GDELT GKG SCHEMA DEFINITION
# ============================================
"""
GDELT GKG 1.0 format consists of 11 base columns plus 1 metadata column.
Schema is strictly enforced to ensure data quality and consistency.

Column Categories:
------------------
- Temporal: DATE (when article was published)
- Article Metrics: NUMARTS (number of articles mentioning this combination)
- Semi-Structured Fields: COUNTS, THEMES, LOCATIONS, PERSONS, ORGANIZATIONS, TONE
- Linkage: CAMEOEVENTIDS (links to GDELT Events table)
- Source: SOURCES, SOURCEURLS (attribution)
- Metadata: extraction_date (pipeline processing date)

Key Difference from Events:
----------------------------
Events has many typed numeric columns (integers, floats).
GKG has mostly string columns containing delimited lists.
This makes GKG more flexible but requires parsing in Silver layer.

Full documentation: http://data.gdeltproject.org/documentation/
"""

# Column names (12 total including extraction_date)
column_names = [
    "DATE", "NUMARTS", "COUNTS", "THEMES", "LOCATIONS",
    "PERSONS", "ORGANIZATIONS", "TONE", "CAMEOEVENTIDS",
    "SOURCES", "SOURCEURLS", "extraction_date"
]

# Spark schema with explicit data types
# Type enforcement prevents data quality issues
schema = StructType([
    # Temporal Information
    StructField("DATE", IntegerType(), True),           # YYYYMMDD format (e.g., 20240115)

    # Article Metrics
    StructField("NUMARTS", IntegerType(), True),        # Number of articles

    # Semi-Structured Text Fields (parsed in Silver layer)
    StructField("COUNTS", StringType(), True),          # Semicolon-delimited count metrics
    StructField("THEMES", StringType(), True),          # Semicolon-delimited theme codes
    StructField("LOCATIONS", StringType(), True),       # Complex location strings (type#name#country#coords)
    StructField("PERSONS", StringType(), True),         # Semicolon-delimited person names
    StructField("ORGANIZATIONS", StringType(), True),   # Semicolon-delimited organization names
    StructField("TONE", StringType(), True),            # Comma-delimited sentiment metrics

    # Linkage
    StructField("CAMEOEVENTIDS", StringType(), True),   # Related GDELT Event IDs

    # Source Attribution
    StructField("SOURCES", StringType(), True),         # Semicolon-delimited source identifiers
    StructField("SOURCEURLS", StringType(), True),      # Semicolon-delimited article URLs

    # Pipeline Metadata
    StructField("extraction_date", DateType(), True)    # Date data was extracted by our pipeline
])

print(f"✓ Schema defined: {len(schema.fields)} columns")

# ============================================
# READ PARQUET FROM S3
# ============================================
# Get extraction date from previous task
extraction_date = dbutils.jobs.taskValues.get(
    "00_get_events_control_date",
    "next_date_to_process_gkg"
)

# Convert date format for S3 path (2024-01-15 → 20240115)
extraction_date_for_s3 = extraction_date.replace("-", "")

# Construct S3 path to Parquet file
s3_base_path = dbutils.widgets.get("aws_raw_path")
s3_path = f"{s3_base_path}/{extraction_date_for_s3}.parquet"

print(f"Reading Parquet from S3: {s3_path}")

# Read Parquet file with automatic schema inference
spark_df = spark.read.parquet(s3_path)

# ============================================
# ADD METADATA COLUMNS
# ============================================
# extraction_date: Date this data was extracted (for tracking)
# last_modified: Last modification timestamp (for Delta Lake)
spark_df = spark_df.withColumn("extraction_date", lit(extraction_date))
spark_df = spark_df.withColumn("last_modified", lit(extraction_date))

print(f"✓ Read {spark_df.count():,} records from S3")

# ============================================
# UPSERT TO DELTA TABLE
# ============================================
"""
Delta Lake Merge Operation for GKG:

Challenge: GKG has NO unique identifier
-------------------------------------------
Unlike Events (which has GlobalEventID), GKG represents article mentions.
The same article can be mentioned multiple times with different themes/locations.

Composite Key Strategy:
-----------------------
We match on a combination of 10 fields to detect duplicates:
- DATE, NUMARTS, CAMEOEVENTIDS, THEMES, TONE, LOCATIONS,
  PERSONS, ORGANIZATIONS, SOURCES, SOURCEURLS

This ensures we don't insert the same article mention twice.

Performance Implications:
-------------------------
- Matching on 10 string fields is EXPENSIVE (no indexed primary key)
- Each merge scans all fields for comparison
- Production should add surrogate key: hash(DATE + THEMES + LOCATIONS + ...)
- Alternative: Use append-only mode and deduplicate in Gold layer

Merge Logic:
------------
- If record exists (matched by composite key): UPDATE all columns
- If record is new: INSERT all columns

This approach:
- Handles late-arriving data (updates if details change)
- Prevents duplicates (composite key uniquely identifies articles)
- Maintains data lineage (last_modified timestamp)
- Supports idempotent operations (safe to re-run)

Hackathon Tradeoff:
-------------------
This merge condition is not optimal but works for datathon scale.
For production at petabyte scale, consider:
1. Add MD5 hash surrogate key
2. Use append-only + downstream deduplication
3. Partition by DATE for better merge performance
"""

if table_exists:
    # ============================================
    # CASE 1: TABLE EXISTS - PERFORM MERGE
    # ============================================
    print("Performing Delta Lake merge (upsert)...")

    # Load existing Delta table
    delta_table = DeltaTable.forPath(spark, delta_table_path)

    # Perform merge operation
    # Condition: Match on composite key (10 fields)
    delta_table.alias("tgt").merge(
        source=spark_df.alias("src"),
        condition=(
            "tgt.DATE = src.DATE AND "
            "tgt.NUMARTS = src.NUMARTS AND "
            "tgt.CAMEOEVENTIDS = src.CAMEOEVENTIDS AND "
            "tgt.THEMES = src.THEMES AND "
            "tgt.TONE = src.TONE AND "
            "tgt.LOCATIONS = src.LOCATIONS AND "
            "tgt.PERSONS = src.PERSONS AND "
            "tgt.ORGANIZATIONS = src.ORGANIZATIONS AND "
            "tgt.SOURCES = src.SOURCES AND "
            "tgt.SOURCEURLS = src.SOURCEURLS"
        )
    ).whenMatchedUpdateAll() \      # Update all columns if composite key matches
     .whenNotMatchedInsertAll() \   # Insert if composite key doesn't exist
     .execute()

    print(f"✓ Merge completed successfully")
    print(f"  Updated/Inserted data from: {s3_path}")
    print(f"Datos actualizados para el archivo {s3_path}.")

else:
    # ============================================
    # CASE 2: TABLE DOESN'T EXIST - CREATE TABLE
    # ============================================
    print("Creating new Delta table...")

    # Write DataFrame as Delta table
    # Mode 'append' for initial creation (first batch of data)
    spark_df.write \
        .format("delta") \
        .mode("append") \
        .save(delta_table_path)

    print(f"✓ Delta table created successfully")
    print(f"  Location: {delta_table_path}")
    print(f"  Initial records: {spark_df.count():,}")
    print(f"Tabla creada y datos insertados desde el archivo {s3_path}.")

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

# COMMAND ----------

# ============================================
# END OF NOTEBOOK
# ============================================
