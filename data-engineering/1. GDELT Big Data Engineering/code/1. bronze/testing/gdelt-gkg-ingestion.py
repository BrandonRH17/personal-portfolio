# Databricks notebook source
"""
==============================================================================
BRONZE LAYER - GDELT GKG INGESTION TESTING NOTEBOOK
==============================================================================

Purpose:
--------
This notebook was used during the initial development and testing phase of the
GDELT Global Knowledge Graph (GKG) ingestion pipeline. It demonstrates the
two-phase development process: initial testing and production-ready backfill.

What is GDELT GKG?
------------------
The GKG is GDELT's knowledge graph that connects:
- Themes: What the article is about (conflict, economics, health, etc.)
- Locations: Where events are happening (with coordinates)
- Persons: Who is mentioned in the article
- Organizations: Which organizations are involved
- Tone: Sentiment and emotional content
- Sources: Original news article URLs

GKG vs Events:
--------------
- Events: 57 columns, structured event records
- GKG: 11 columns, semi-structured text fields (themes, locations, etc.)
- Events: ~200-500 MB/day uncompressed
- GKG: ~100-500 MB/day compressed (much larger files)
- Events: Used for event-level analysis
- GKG: Used for theme extraction, sentiment analysis, entity recognition

Development Workflow:
---------------------
This notebook shows 2 progressive stages:
  [1] Testing Phase (lines 1-31): Single-date validation with pandas
  [2] Production Phase (lines 33-132): Historical backfill with Spark + Delta

Use Cases:
----------
- Validate GKG file format and column structure
- Test schema with semi-structured text fields
- Historical data backfill (2023-03-27 to 2024-08-18)
- Validate append-only pattern (GKG doesn't have unique IDs)
- End-to-end pipeline testing before productionization

Key Differences from Events Pipeline:
--------------------------------------
- GKG has NO unique identifier → uses append mode instead of merge
- GKG files are larger → requires more memory
- GKG has semi-structured fields → requires parsing in Silver layer
- GKG schema is simpler → only 11 columns vs 57

Author: Neutrino Solutions Team
Date: 2024
==============================================================================
"""

# COMMAND ----------

# ============================================
# CELL 1: TESTING PHASE - SCHEMA VALIDATION
# ============================================
# MAGIC %md
# MAGIC ## TESTING PHASE

# COMMAND ----------

"""
DEVELOPMENT STAGE 1: GKG Schema Validation

Purpose:
--------
- Verify GDELT GKG file structure and format
- Validate that all 11 columns are present
- Test download and extraction logic for larger files
- Understand tab-delimited format with header row

Key Differences from Events Testing:
-------------------------------------
- GKG files HAVE a header row (Events don't)
- GKG uses tab-delimited format with complex nested data
- GKG files are much larger (100-500 MB compressed)
- GKG has no unique ID column
"""

import pandas as pd
import requests
import zipfile
import io

# ============================================
# DOWNLOAD SAMPLE GKG FILE
# ============================================
# Using recent date (August 13, 2024) for testing
# GKG files are published daily like Events files
url = f"http://data.gdeltproject.org/gkg/20240813.gkg.csv.zip"

print(f"Downloading GKG file from: {url}")

# ============================================
# DOWNLOAD AND EXTRACT
# ============================================
# Download ZIP file from GDELT servers
response = requests.get(url)
zip_file = zipfile.ZipFile(io.BytesIO(response.content))

# Extract CSV from ZIP (GKG files: YYYYMMDD.gkg.csv)
csv_file_name = zip_file.namelist()[0]
csv_file = zip_file.open(csv_file_name)

print(f"✓ Downloaded and extracted: {csv_file_name}")

# ============================================
# LOAD CSV TO PANDAS
# ============================================
# GKG files HAVE a header row (unlike Events files)
# Tab-delimited format with 11 columns
df = pd.read_csv(csv_file, sep='\t')

print(f"✓ Loaded {len(df):,} GKG records")
print(f"✓ Columns: {list(df.columns)}")

# ============================================
# VALIDATE COLUMN STRUCTURE
# ============================================
# Check that all rows have the same number of columns
# This validates file integrity
column_counts = df.apply(lambda row: len(row), axis=1).value_counts()

print("\nColumn count validation:")
print(column_counts)

# Display sample data for manual inspection
df.head(5)


# COMMAND ----------

# ============================================
# CELL 2: PRODUCTION PHASE - HISTORICAL BACKFILL
# ============================================
# MAGIC %md
# MAGIC ## LOAD DATA FROM A PERIOD - PROD PHASE

# COMMAND ----------

"""
DEVELOPMENT STAGE 2: Historical Backfill with Delta Lake

Purpose:
--------
- Load 1.5 years of historical GKG data (2023-03-27 to 2024-08-18)
- Test Spark processing with large GKG files
- Validate Delta Lake append pattern (no merge needed)
- Test error handling and resilience
- Prepare data for Silver layer parsing

Why Append Instead of Merge?
-----------------------------
- GKG records have NO unique identifier
- Each row represents an article mention (not an event)
- Duplicates are unlikely (each article is unique)
- Append mode is faster and simpler for GKG
- Downstream Silver layer handles deduplication if needed

Performance Considerations:
---------------------------
- Processing ~510 days of GKG data
- Each day: 100-500 MB compressed, 500 MB-2 GB uncompressed
- Total: ~250-1000 GB uncompressed data
- GKG files are larger than Events files
- Estimated time: 12-18 hours depending on cluster size
"""

import pandas as pd
import requests
import zipfile
import io
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timedelta
from pyspark.sql.functions import col

# ============================================
# SPARK SESSION INITIALIZATION
# ============================================
# Initialize Spark for distributed processing
spark = SparkSession.builder.appName("GKG Data Loader").getOrCreate()
print("✓ Spark session initialized")

# ============================================
# DELTA TABLE CONFIGURATION
# ============================================
# Delta table path for GKG Bronze layer
# This is a real S3 path (unlike Events testing which used default)
delta_table_path = "s3://databricks-workspace-stack-e63e7-bucket/unity-catalog/2600119076103476/bronze/gdelt/gkg/"
print(f"Delta table path: {delta_table_path}")

# ============================================
# DEFINE DATE RANGE FOR BACKFILL
# ============================================
# Load 1.5 years of historical GKG data for comprehensive analysis
start_date = datetime.strptime("2023-03-27", "%Y-%m-%d")
end_date = datetime.strptime("2024-08-18", "%Y-%m-%d")

print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
print(f"Total days to process: {(end_date - start_date).days + 1}")

# ============================================
# GDELT GKG SCHEMA DEFINITION
# ============================================
"""
GDELT GKG 1.0 Format: 11 Columns

Column Descriptions:
--------------------
1. DATE: Publication date (YYYYMMDD format)
2. NUMARTS: Number of source articles mentioning this combination
3. COUNTS: Count-based metrics (type#count pairs)
4. THEMES: Semicolon-delimited theme codes (TAX_FNCACT, WB_632_RURAL_DEVELOPMENT, etc.)
5. LOCATIONS: Complex location strings with coordinates (type#name#country#lat#long)
6. PERSONS: Semicolon-delimited person names mentioned
7. ORGANIZATIONS: Semicolon-delimited organization names mentioned
8. TONE: Comma-delimited sentiment metrics (tone, positive, negative, polarity, etc.)
9. CAMEOEVENTIDS: Related GDELT Event IDs
10. SOURCES: Semicolon-delimited source identifiers
11. SOURCEURLS: Semicolon-delimited source article URLs

Key Fields for Port Analysis:
------------------------------
- THEMES: Identifies disruption-related themes (conflict, weather, supply chain)
- LOCATIONS: Geographic filtering around ports
- TONE: Sentiment analysis for risk assessment
- SOURCEURLS: Source attribution for validation
"""

# Column names for GKG format
column_names = [
    "DATE", "NUMARTS", "COUNTS", "THEMES", "LOCATIONS",
    "PERSONS", "ORGANIZATIONS", "TONE", "CAMEOEVENTIDS",
    "SOURCES", "SOURCEURLS"
]

# ============================================
# SPARK SCHEMA DEFINITION
# ============================================
# Most GKG columns are strings because they contain:
# - Semicolon-delimited lists
# - Comma-delimited metrics
# - Complex nested structures
# These are parsed in the Silver layer
schema = StructType([
    StructField("DATE", IntegerType(), True),          # YYYYMMDD format
    StructField("NUMARTS", IntegerType(), True),       # Article count
    StructField("COUNTS", StringType(), True),         # Count metrics (to be parsed)
    StructField("THEMES", StringType(), True),         # Semicolon-delimited themes
    StructField("LOCATIONS", StringType(), True),      # Complex location strings
    StructField("PERSONS", StringType(), True),        # Semicolon-delimited names
    StructField("ORGANIZATIONS", StringType(), True),  # Semicolon-delimited orgs
    StructField("TONE", StringType(), True),           # Comma-delimited sentiment
    StructField("CAMEOEVENTIDS", StringType(), True),  # Related event IDs
    StructField("SOURCES", StringType(), True),        # Source identifiers
    StructField("SOURCEURLS", StringType(), True),     # Source URLs
    StructField("extraction_date", DateType(), True)   # Pipeline metadata
])

print(f"✓ Schema defined: {len(schema.fields)} columns")

# ============================================
# HISTORICAL BACKFILL LOOP
# ============================================
# Process each day in the date range sequentially
current_date = start_date
processed_count = 0
error_count = 0

print("\nStarting historical backfill...")
print("=" * 70)

while current_date <= end_date:
    date_str = current_date.strftime('%Y%m%d')
    url = f"http://data.gdeltproject.org/gkg/{date_str}.gkg.csv.zip"

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
        # LOAD DATA TO PANDAS
        # ============================================
        # GKG files have headers, but we override with our column names
        # low_memory=False handles large files with mixed types
        df = pd.read_csv(csv_file, sep='\t', header=None, names=column_names, low_memory=False)

        # ============================================
        # TYPE CONVERSION: NUMERIC COLUMNS
        # ============================================
        # Convert DATE and NUMARTS to integers
        # Other columns remain as strings (contain complex data)
        numeric_columns = ['DATE', 'NUMARTS']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

        # ============================================
        # TYPE CONVERSION: STRING COLUMNS
        # ============================================
        # Ensure all text fields are strings (not objects)
        # This prevents schema issues in Spark
        string_columns = [
            'COUNTS', 'THEMES', 'LOCATIONS', 'PERSONS', 'ORGANIZATIONS',
            'TONE', 'CAMEOEVENTIDS', 'SOURCES', 'SOURCEURLS'
        ]
        df[string_columns] = df[string_columns].astype(str)

        # ============================================
        # ADD METADATA COLUMN
        # ============================================
        # Track extraction date for data lineage
        df['extraction_date'] = current_date.date()

        # ============================================
        # CONVERT TO SPARK DATAFRAME
        # ============================================
        # Convert pandas to Spark with schema enforcement
        spark_df = spark.createDataFrame(df, schema=schema)

        # ============================================
        # APPEND TO DELTA TABLE
        # ============================================
        """
        Append Mode for GKG:
        - No merge needed (no unique identifier)
        - Faster than merge operations
        - Each article is unique by nature
        - Downstream deduplication handled in Silver layer if needed
        """
        spark_df.write.format("delta").mode("append").save(delta_table_path)

        processed_count += 1
        print(f"✓ [{processed_count}] {date_str}: {len(df):,} records appended")

    except requests.exceptions.RequestException as e:
        # ============================================
        # ERROR HANDLING: DOWNLOAD/NETWORK ERRORS
        # ============================================
        error_count += 1
        print(f"✗ [{error_count}] {date_str}: Download error - {e}")

    except Exception as e:
        # ============================================
        # ERROR HANDLING: PROCESSING ERRORS
        # ============================================
        error_count += 1
        print(f"✗ [{error_count}] {date_str}: Processing error - {e}")

    # ============================================
    # INCREMENT DATE
    # ============================================
    # Move to next day in the range
    current_date += timedelta(days=1)

# ============================================
# COMPLETION SUMMARY
# ============================================
print("=" * 70)
print("HISTORICAL BACKFILL COMPLETED")
print("=" * 70)
print(f"  Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
print(f"  Successfully processed: {processed_count} days")
print(f"  Errors encountered: {error_count} days")
print(f"  Success rate: {processed_count / (processed_count + error_count) * 100:.1f}%")
print(f"  Delta table: {delta_table_path}")

# ============================================
# FINALIZATION
# ============================================
# Stop Spark session to release resources
spark.stop()
print("\n✓ Spark session stopped")

