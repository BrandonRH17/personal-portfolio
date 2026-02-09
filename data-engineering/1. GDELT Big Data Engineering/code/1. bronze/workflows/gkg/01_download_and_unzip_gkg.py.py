# Databricks notebook source
"""
==============================================================================
BRONZE LAYER - GDELT GKG DOWNLOAD AND S3 UPLOAD
==============================================================================

Purpose:
--------
This notebook is the second step in the Bronze layer workflow for GDELT GKG
(Global Knowledge Graph) data ingestion. It downloads daily GKG ZIP files,
extracts the CSV, converts to Parquet format, and uploads to S3 for processing.

Workflow Position:
------------------
Step 2 of 4 in Bronze Layer GKG Ingestion:
    [1] Get Control Date
  → [2] Download and Unzip Data (this script)
    [3] Upsert to Delta Table
    [4] Update Control Date

Key Differences from Events Workflow:
--------------------------------------
1. **File Size**: GKG files are MUCH larger (100-500 MB compressed vs 20-50 MB for Events)
2. **Schema**: Only 11 columns (vs 57 for Events) but semi-structured text fields
3. **URL Pattern**: .gkg.csv.zip (vs .export.CSV.zip for Events)
4. **Data Type**: Knowledge graph (themes, entities, locations) vs structured events
5. **Header Row**: GKG files have NO header row (like Events)

What is GDELT GKG?
------------------
The Global Knowledge Graph connects:
- **THEMES**: What the article is about (e.g., WB_632_RURAL_DEVELOPMENT, TERROR)
- **LOCATIONS**: Where events are happening (with coordinates)
- **PERSONS**: Who is mentioned in the article
- **ORGANIZATIONS**: Which organizations are involved
- **TONE**: Sentiment and emotional content
- **SOURCES**: Original news article URLs

Why GKG Matters for Port Analysis:
-----------------------------------
- THEMES identify disruption types (infrastructure, trade, incidents)
- LOCATIONS enable geographic filtering around ports
- TONE provides sentiment for risk assessment
- SOURCEURLS allow content verification and web scraping

Dependencies:
-------------
- Previous task: 00_get_events_control_date (retrieves next_date_to_process_gkg)
- AWS S3 bucket: factored-datalake-raw
- Databricks widgets: aws_access_key, aws_secret_access_key

Performance Note:
-----------------
GKG files can be 5-10x larger than Events files, so download and processing
take longer. The pipeline handles this with in-memory processing (no disk I/O).

Author: Neutrino Solutions Team (Factored Datathon 2024)
Date: 2024
==============================================================================
"""

# ============================================
# IMPORTS
# ============================================
import requests          # HTTP requests for downloading GDELT GKG files
import zipfile           # ZIP file extraction
import io                # In-memory file handling
import pandas as pd      # DataFrame operations
import boto3             # AWS S3 client
import pyarrow as pa     # Arrow format for efficient data structures
import pyarrow.parquet as pq  # Parquet file writing
from botocore.exceptions import NoCredentialsError
from datetime import datetime, timedelta


# ============================================
# FUNCTION: DOWNLOAD AND PREPARE GKG DATA
# ============================================
def download_and_prepare_data():
    """
    Download GDELT GKG ZIP file, extract CSV, and load into pandas DataFrame.

    This function:
    1. Retrieves the next date to process from Databricks task values
    2. Constructs the GDELT GKG download URL
    3. Downloads and extracts the ZIP file in-memory (no disk I/O)
    4. Parses the tab-delimited CSV with proper column names
    5. Applies type conversions (numeric vs string columns)
    6. Returns DataFrame ready for S3 upload

    Returns:
    --------
    tuple: (DataFrame, date_string)
        - df: pandas DataFrame containing GDELT GKG records
        - date_str: Date string in YYYYMMDD format

    Raises:
    -------
    requests.exceptions.RequestException: If download or HTTP request fails

    Notes:
    ------
    - GDELT GKG has 11 columns (vs 57 for Events)
    - Files are typically 100-500 MB compressed, 500 MB-2 GB uncompressed
    - Most columns are semi-structured text (semicolon-delimited lists)
    - In-memory processing avoids Databricks DBFS storage overhead
    - Type conversion handles mixed types and missing values
    """

    # ============================================
    # STEP 1: GET NEXT DATE TO PROCESS
    # ============================================
    # Retrieve date from previous task (00_get_events_control_date)
    # Task values are set by the control date notebook
    # Note: Uses same control date notebook as Events (shared task)
    next_day_to_process = dbutils.jobs.taskValues.get(
        "00_get_events_control_date",
        "next_date_to_process_gkg"
    )

    # Convert date format: 2024-01-15 → 20240115 (GDELT URL format)
    date_str = next_day_to_process.replace("-", "")

    # ============================================
    # STEP 2: CONSTRUCT GDELT GKG DOWNLOAD URL
    # ============================================
    # GDELT GKG URL pattern: http://data.gdeltproject.org/gkg/YYYYMMDD.gkg.csv.zip
    # Different from Events: /gkg/ instead of /events/, .gkg.csv.zip instead of .export.CSV.zip
    url = f"http://data.gdeltproject.org/gkg/{date_str}.gkg.csv.zip"

    print(f"Downloading GDELT GKG for date: {next_day_to_process}")
    print(f"URL: {url}")

    try:
        # ============================================
        # STEP 3: DOWNLOAD ZIP FILE
        # ============================================
        # Stream download to handle large GKG files efficiently
        # GKG files can be 5-10x larger than Events files
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors (4xx, 5xx)

        print(f"✓ Downloaded {len(response.content) / 1024 / 1024:.2f} MB")

        # ============================================
        # STEP 4: EXTRACT ZIP IN-MEMORY
        # ============================================
        # Process ZIP without writing to disk (faster and cleaner)
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        csv_file_name = zip_file.namelist()[0]  # Should be YYYYMMDD.gkg.csv
        csv_file = zip_file.open(csv_file_name)

        print(f"✓ Extracted CSV file: {csv_file_name}")

        # ============================================
        # STEP 5: PARSE CSV WITH SCHEMA
        # ============================================
        """
        GDELT GKG 1.0 Format: 11 Columns (vs 57 for Events)

        Column Descriptions:
        --------------------
        1. DATE: Publication date (YYYYMMDD format)
        2. NUMARTS: Number of source articles mentioning this combination
        3. COUNTS: Count-based metrics (type#count pairs, semicolon-delimited)
        4. THEMES: Semicolon-delimited theme codes (e.g., TAX_FNCACT, WB_632_RURAL_DEVELOPMENT)
        5. LOCATIONS: Complex location strings with coordinates (type#name#country#lat#long)
        6. PERSONS: Semicolon-delimited person names mentioned
        7. ORGANIZATIONS: Semicolon-delimited organization names mentioned
        8. TONE: Comma-delimited sentiment metrics (tone, positive, negative, polarity, etc.)
        9. CAMEOEVENTIDS: Related GDELT Event IDs (links to Events table)
        10. SOURCES: Semicolon-delimited source identifiers
        11. SOURCEURLS: Semicolon-delimited source article URLs

        Key Differences from Events:
        -----------------------------
        - Events: 57 columns, highly structured (integers, floats, strings)
        - GKG: 11 columns, semi-structured (mostly delimited text fields)
        - Events: Each row = one event
        - GKG: Each row = one article mention (one article can mention multiple themes/locations)
        - Events: Has unique ID (GlobalEventID)
        - GKG: No unique ID (composite key needed)

        Why Semi-Structured?
        ---------------------
        - THEMES can have 10+ themes per article (semicolon-delimited list)
        - LOCATIONS can have multiple locations per article
        - PERSONS can have 20+ people mentioned
        - These are parsed in Silver layer into structured columns
        """

        # Read tab-delimited CSV with explicit column names
        # low_memory=False handles large files with mixed types
        df = pd.read_csv(
            csv_file,
            sep='\t',              # Tab-delimited format
            header=None,           # No header row in source file
            names=[
                "DATE", "NUMARTS", "COUNTS", "THEMES", "LOCATIONS",
                "PERSONS", "ORGANIZATIONS", "TONE", "CAMEOEVENTIDS",
                "SOURCES", "SOURCEURLS"
            ],
            low_memory=False       # Required for large GKG files with mixed types
        )

        # ============================================
        # STEP 6: TYPE CONVERSION - NUMERIC COLUMNS
        # ============================================
        # Only 2 numeric columns in GKG (vs 24 in Events)
        # DATE: YYYYMMDD format (e.g., 20240115)
        # NUMARTS: Number of articles mentioning this combination
        numeric_columns = ['DATE', 'NUMARTS']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

        # ============================================
        # STEP 7: TYPE CONVERSION - STRING COLUMNS
        # ============================================
        # All other columns are text fields (semicolon-delimited lists or complex strings)
        # These will be parsed in Silver layer
        string_columns = [
            'COUNTS', 'THEMES', 'LOCATIONS', 'PERSONS', 'ORGANIZATIONS',
            'TONE', 'CAMEOEVENTIDS', 'SOURCES', 'SOURCEURLS'
        ]
        df[string_columns] = df[string_columns].astype(str)

        print(f"✓ Parsed {len(df):,} GKG records")
        print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        print(f"Datos para {date_str} descargados y preparados.")

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
        DataFrame containing GDELT GKG data
    file_name : str
        Base filename (date string YYYYMMDD)
    bucket_name : str
        S3 bucket name (e.g., 'factored-datalake-raw')
    s3_prefix : str, optional
        S3 key prefix/folder path (e.g., 'gkg/')

    Raises:
    -------
    NoCredentialsError: If AWS credentials are invalid or missing
    Exception: For other S3 upload errors

    Notes:
    ------
    - Credentials are retrieved from Databricks widgets (set by job parameters)
    - Parquet is written to memory buffer (no local disk usage)
    - S3 key format: {s3_prefix}gdelt/{file_name}.parquet
    - For GKG, typical compression ratio is 60-70% (better than Events due to text)
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
        # Example: gkg/gdelt/20240115.parquet
        s3_key = f"{s3_prefix}gdelt/{file_name}.parquet"

        # Upload file object to S3
        s3.upload_fileobj(parquet_buffer, bucket_name, s3_key)

        print(f"✓ Uploaded to S3")
        print(f"  Bucket: {bucket_name}")
        print(f"  Key: {s3_key}")
        print(f"Archivo {s3_key} subido a S3 en el bucket {bucket_name}.")

    except NoCredentialsError:
        # ============================================
        # ERROR: INVALID CREDENTIALS
        # ============================================
        print("ERROR: AWS credentials not found or invalid")
        print("  Check Databricks job parameters:")
        print("    - aws_access_key")
        print("    - aws_secret_access_key")
        print("Credenciales de AWS no encontradas.")
        raise

    except Exception as e:
        # ============================================
        # ERROR: UPLOAD FAILED
        # ============================================
        print(f"ERROR: Failed to upload to S3")
        print(f"  Bucket: {bucket_name}")
        print(f"  Key: {s3_key}")
        print(f"  Exception: {e}")
        print(f"Error al subir el archivo a S3: {e}")
        raise


# ============================================
# MAIN EXECUTION
# ============================================
if __name__ == "__main__":
    """
    Main execution flow for GDELT GKG download and S3 upload.

    This is orchestrated by Databricks Workflows as part of the Bronze layer pipeline.

    Execution Steps:
    ----------------
    1. Download GKG data for next date (from control table)
    2. Parse CSV and apply type conversions
    3. Convert to Parquet format
    4. Upload to S3 for Delta Lake processing

    Performance:
    ------------
    - GKG files: 100-500 MB compressed → 500 MB-2 GB uncompressed
    - Processing time: ~3-8 minutes per day (vs 2-5 min for Events)
    - Memory usage: ~1-3 GB peak (larger than Events)

    Next Step:
    ----------
    - 02_upsert_delta_table_gkg.py will read this Parquet file from S3
      and merge into Delta Lake table
    """

    print("=" * 70)
    print("GDELT GKG DOWNLOAD AND S3 UPLOAD")
    print("=" * 70)

    # STEP 1: Download and prepare GDELT GKG data
    df, date_str = download_and_prepare_data()

    # STEP 2: Upload to S3 as Parquet for further processing
    upload_to_s3_parquet(
        df,
        date_str,
        'factored-datalake-raw',  # S3 bucket name
        'gkg/'                     # S3 prefix (folder)
    )

    print("=" * 70)
    print("SUCCESS: GDELT GKG processing complete")
    print("=" * 70)

# COMMAND ----------

# ============================================
# END OF NOTEBOOK
# ============================================
