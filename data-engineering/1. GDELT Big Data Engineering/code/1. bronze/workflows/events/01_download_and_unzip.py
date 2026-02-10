# Databricks notebook source
"""
==============================================================================
BRONZE LAYER - GDELT EVENTS DOWNLOAD AND S3 UPLOAD
==============================================================================

Purpose:
--------
This notebook is the second step in the Bronze layer workflow for GDELT Events
data ingestion. It downloads daily GDELT Events ZIP files, extracts the CSV,
converts it to Parquet format, and uploads to S3 for further processing.

Workflow Position:
------------------
Step 2 of 4 in Bronze Layer Ingestion:
    [1] Get Control Date
  → [2] Download and Unzip Data (this script)
    [3] Upsert to Delta Table
    [4] Update Control Date

Key Functionality:
------------------
1. Retrieve next date to process from previous task
2. Download GDELT Events ZIP file from data.gdeltproject.org
3. Extract and parse tab-delimited CSV
4. Convert to Parquet format (columnar storage for better performance)
5. Upload to S3 bucket for persistent storage

Data Format:
------------
- Source: GDELT Events 1.0 format (57 columns)
- Input: Tab-delimited CSV in ZIP archive
- Output: Parquet file in S3

Dependencies:
-------------
- Previous task: 00_get_events_control_date
- AWS S3 bucket: factored-datalake-raw
- Databricks widgets: aws_access_key, aws_secret_access_key

Author: Neutrino Solutions Team
Date: 2024
==============================================================================
"""

# ============================================
# IMPORTS
# ============================================
import requests          # HTTP requests for downloading GDELT files
import zipfile           # ZIP file extraction
import io                # In-memory file handling
import pandas as pd      # DataFrame operations
import boto3             # AWS S3 client
import pyarrow as pa     # Arrow format for efficient data structures
import pyarrow.parquet as pq  # Parquet file writing
from botocore.exceptions import NoCredentialsError
from datetime import datetime, timedelta


# ============================================
# FUNCTION: DOWNLOAD AND PREPARE DATA
# ============================================
def download_and_prepare_data():
    """
    Download GDELT Events ZIP file, extract CSV, and load into pandas DataFrame.

    This function:
    1. Retrieves the next date to process from Databricks task values
    2. Constructs the GDELT download URL
    3. Downloads and extracts the ZIP file in-memory (no disk I/O)
    4. Parses the tab-delimited CSV with proper column names
    5. Returns DataFrame ready for S3 upload

    Returns:
    --------
    tuple: (DataFrame, date_string)
        - df: pandas DataFrame containing GDELT events
        - date_str: Date string in YYYYMMDD format

    Raises:
    -------
    requests.exceptions.RequestException: If download or HTTP request fails

    Notes:
    ------
    - GDELT Events uses 57 columns (Events 1.0 format)
    - Files are typically 20-50 MB compressed, 200-500 MB uncompressed
    - In-memory processing avoids Databricks DBFS storage overhead
    """

    # ============================================
    # STEP 1: GET NEXT DATE TO PROCESS
    # ============================================
    # Retrieve date from previous task (00_get_events_control_date)
    # Task values are set by the control date notebook
    next_day_to_process = dbutils.jobs.taskValues.get(
        "00_get_events_control_date",
        "next_date_to_process_events"
    )

    # Convert date format: 2024-01-15 → 20240115 (GDELT URL format)
    date_str = next_day_to_process.replace("-", "")

    # ============================================
    # STEP 2: CONSTRUCT GDELT DOWNLOAD URL
    # ============================================
    # GDELT Events URL pattern: http://data.gdeltproject.org/events/YYYYMMDD.export.CSV.zip
    url = f"http://data.gdeltproject.org/events/{date_str}.export.CSV.zip"

    print(f"Downloading GDELT Events for date: {next_day_to_process}")
    print(f"URL: {url}")

    try:
        # ============================================
        # STEP 3: DOWNLOAD ZIP FILE
        # ============================================
        # Stream download to handle large files efficiently
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors (4xx, 5xx)

        print(f"✓ Downloaded {len(response.content) / 1024 / 1024:.2f} MB")

        # ============================================
        # STEP 4: EXTRACT ZIP IN-MEMORY
        # ============================================
        # Process ZIP without writing to disk (faster and cleaner)
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        csv_file_name = zip_file.namelist()[0]  # Should be YYYYMMDD.export.CSV
        csv_file = zip_file.open(csv_file_name)

        print(f"✓ Extracted CSV file: {csv_file_name}")

        # ============================================
        # STEP 5: PARSE CSV WITH SCHEMA
        # ============================================
        # GDELT Events 1.0 format has 57 columns, tab-delimited, no header row
        # Column names defined according to GDELT documentation:
        # http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
        df = pd.read_csv(
            csv_file,
            sep='\t',           # Tab-delimited format
            header=None,        # No header row in source file
            names=[
                # Event Identification
                "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",

                # Actor 1 Information
                "Actor1Code", "Actor1Name", "Actor1CountryCode",
                "Actor1KnownGroupCode", "Actor1EthnicCode",
                "Actor1Religion1Code", "Actor1Religion2Code",
                "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",

                # Actor 2 Information
                "Actor2Code", "Actor2Name", "Actor2CountryCode",
                "Actor2KnownGroupCode", "Actor2EthnicCode",
                "Actor2Religion1Code", "Actor2Religion2Code",
                "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",

                # Event Action
                "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
                "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
                "NumArticles", "AvgTone",

                # Actor 1 Geography
                "Actor1Geo_Type", "Actor1Geo_Fullname", "Actor1Geo_CountryCode",
                "Actor1Geo_ADM1Code", "Actor1Geo_Lat", "Actor1Geo_Long",
                "Actor1Geo_FeatureID",

                # Actor 2 Geography
                "Actor2Geo_Type", "Actor2Geo_Fullname", "Actor2Geo_CountryCode",
                "Actor2Geo_ADM1Code", "Actor2Geo_Lat", "Actor2Geo_Long",
                "Actor2Geo_FeatureID",

                # Action Geography (most important for port analysis)
                "ActionGeo_Type", "ActionGeo_Fullname", "ActionGeo_CountryCode",
                "ActionGeo_ADM1Code", "ActionGeo_Lat", "ActionGeo_Long",
                "ActionGeo_FeatureID",

                # Metadata
                "DATEADDED", "SOURCEURL"
            ]
        )

        print(f"✓ Parsed {len(df):,} events")
        print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

        return df, date_str

    except requests.exceptions.RequestException as e:
        # ============================================
        # ERROR HANDLING
        # ============================================
        print(f"ERROR: Failed to download or extract data for {date_str}")
        print(f"  Exception: {e}")
        print(f"  URL: {url}")
        raise


# ============================================
# FUNCTION: UPLOAD TO S3 AS PARQUET
# ============================================
def upload_to_s3_parquet(df, file_name, bucket_name, s3_prefix=""):
    """
    Convert DataFrame to Parquet format and upload to AWS S3.

    Parquet is chosen over CSV because:
    - Columnar storage (10-100x faster queries)
    - Built-in compression (50-80% smaller files)
    - Schema preservation (types, nullability)
    - Compatible with Spark, Databricks, and analytics tools

    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing GDELT events data
    file_name : str
        Base filename (date string YYYYMMDD)
    bucket_name : str
        S3 bucket name (e.g., 'factored-datalake-raw')
    s3_prefix : str, optional
        S3 key prefix/folder path (e.g., 'events/')

    Raises:
    -------
    NoCredentialsError: If AWS credentials are invalid or missing
    Exception: For other S3 upload errors

    Notes:
    ------
    - Credentials are retrieved from Databricks widgets (set by job parameters)
    - Parquet is written to memory buffer (no local disk usage)
    - S3 key format: {s3_prefix}gdelt/{file_name}.parquet
    """

    # ============================================
    # STEP 1: RETRIEVE AWS CREDENTIALS
    # ============================================
    # Credentials are passed as Databricks job parameters
    # These should be stored securely in Databricks Secrets
    aws_access_key_id = dbutils.widgets.get("aws_access_key")
    aws_secret_access_key = dbutils.widgets.get("aws_secret_access_key")

    # ============================================
    # STEP 2: INITIALIZE S3 CLIENT
    # ============================================
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # ============================================
    # STEP 3: CONVERT TO PARQUET IN-MEMORY
    # ============================================
    # Convert pandas DataFrame to Apache Arrow Table
    # Arrow provides efficient in-memory columnar format
    table = pa.Table.from_pandas(df)

    # Write Parquet to memory buffer (no disk I/O)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)  # Reset buffer position for reading

    print(f"✓ Converted to Parquet format")
    print(f"  Buffer size: {parquet_buffer.getbuffer().nbytes / 1024 / 1024:.2f} MB")

    try:
        # ============================================
        # STEP 4: UPLOAD TO S3
        # ============================================
        # Construct S3 key (path within bucket)
        # Example: events/gdelt/20240115.parquet
        s3_key = f"{s3_prefix}gdelt/{file_name}.parquet"

        # Upload file object to S3
        s3.upload_fileobj(parquet_buffer, bucket_name, s3_key)

        print(f"✓ Uploaded to S3")
        print(f"  Bucket: {bucket_name}")
        print(f"  Key: {s3_key}")

    except NoCredentialsError:
        # ============================================
        # ERROR: INVALID CREDENTIALS
        # ============================================
        print("ERROR: AWS credentials not found or invalid")
        print("  Check Databricks job parameters:")
        print("    - aws_access_key")
        print("    - aws_secret_access_key")
        raise

    except Exception as e:
        # ============================================
        # ERROR: UPLOAD FAILED
        # ============================================
        print(f"ERROR: Failed to upload to S3")
        print(f"  Bucket: {bucket_name}")
        print(f"  Key: {s3_key}")
        print(f"  Exception: {e}")
        raise


# ============================================
# MAIN EXECUTION
# ============================================
if __name__ == "__main__":
    """
    Main execution flow for GDELT Events download and S3 upload.

    This is orchestrated by Databricks Workflows as part of the Bronze layer pipeline.
    """

    print("=" * 70)
    print("GDELT EVENTS DOWNLOAD AND S3 UPLOAD")
    print("=" * 70)

    # STEP 1: Download and prepare GDELT Events data
    df, date_str = download_and_prepare_data()

    # STEP 2: Upload to S3 as Parquet for further processing
    upload_to_s3_parquet(
        df,
        date_str,
        'factored-datalake-raw',  # S3 bucket name
        'events/'                  # S3 prefix (folder)
    )

    print("=" * 70)
    print("SUCCESS: GDELT Events processing complete")
    print("=" * 70)

# ============================================
# END OF NOTEBOOK
# ============================================
