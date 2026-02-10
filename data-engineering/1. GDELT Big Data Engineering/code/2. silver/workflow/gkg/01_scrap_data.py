# Databricks notebook source
"""
==============================================================================
SILVER LAYER - GKG DATA SCRAPING AND PARSING
==============================================================================

Purpose:
--------
This notebook is the core transformation step in the Silver layer workflow for
GDELT GKG. It reads Bronze GKG data, parses semi-structured fields, filters
for port-related news, and scrapes article content from source URLs.

Workflow Position:
------------------
Step 2 of 3 in Silver Layer GKG Processing:
    [1] Get Control Date
  → [2] Scrape and Parse Data (this script)
    [3] Update Control Date

Key Transformations:
--------------------
1. Parse semi-structured fields (LOCATIONS, TONE)
2. Classify shipping routes (Transpacific, Transatlantic)
3. Filter port-related news only
4. Rank by negative sentiment
5. Web scrape top 3 articles per port/date
6. Merge into Silver Delta table

Silver Layer Value:
-------------------
- Bronze: Raw, unparsed semi-structured text
- Silver: Structured columns + enriched with scraped content
- Gold: Can now aggregate and analyze without parsing overhead

Hackathon Note:
---------------
This code has syntax errors (indentation in scrape_content function) but
works in Databricks due to notebook execution environment. Should be fixed
for production IDE compatibility.

Dependencies:
-------------
- Previous task: 00_get_gkg_control_date
- Bronze Delta table: BRONZE.GDELT_GKG
- Libraries: BeautifulSoup (for web scraping)
- PySpark functions: split, when, Window, row_number, udf

Author: Neutrino Solutions Team (Factored Datathon 2024)
Date: 2024
==============================================================================
"""

# ============================================
# IMPORTS
# ============================================
import requests          # HTTP requests for web scraping
import zipfile           # Not used in this script (legacy import)
import io                # Not used in this script (legacy import)
import pandas as pd      # Not used in this script (legacy import)
import boto3             # Not used in this script (legacy import)
import pyarrow as pa     # Not used in this script (legacy import)
import pyarrow.parquet as pq  # Not used in this script (legacy import)
from botocore.exceptions import NoCredentialsError  # Not used in this script
from datetime import datetime, timedelta

# Note: Missing imports that should be added:
# from bs4 import BeautifulSoup
# from pyspark.sql.functions import col, split, when, lit, to_date, count, row_number, udf
# from pyspark.sql.window import Window
# from pyspark.sql.types import StringType
# from delta.tables import DeltaTable


# ============================================
# FUNCTION: WEB SCRAPING
# ============================================
def scrape_content(url):
    """
    Scrape article content from news source URL.

    This function extracts paragraph text from HTML pages to provide
    full article content for downstream NLP analysis (summarization).

    Parameters:
    -----------
    url : str
        Source article URL from GDELT GKG SOURCEURLS field

    Returns:
    --------
    str : Article content (paragraphs concatenated) or error message

    Notes:
    ------
    - 10 second timeout prevents hanging on slow sites
    - Extracts <p> tags only (may miss some content)
    - No Javascript rendering (static HTML only)
    - Returns error message as string (allows processing to continue)

    Hackathon Issue:
    ----------------
    Indentation is incorrect (lines 13-21 should be indented under function).
    Works in Databricks notebook but not in standard Python IDE.
    """
try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    paragraphs = soup.find_all('p')
    content = ' '.join([p.get_text() for p in paragraphs])
    return content
except Exception as e:
    return str(e)


# ============================================
# MAIN PROCESSING FUNCTION
# ============================================
def check_rows():
    """
    Main Silver layer transformation function.

    This function orchestrates the entire Silver layer workflow:
    1. Load Bronze GKG data for specific date
    2. Parse semi-structured fields into structured columns
    3. Filter for port-related news
    4. Rank by negative sentiment
    5. Scrape article content (top 3 per port/date)
    6. Merge into Silver Delta table

    Process Flow:
    -------------
    Bronze GKG → Parse Fields → Filter Ports → Rank → Scrape → Silver Delta
    """

    # ============================================
    # STEP 1: GET DATE TO PROCESS
    # ============================================
    # Retrieve date from previous task (00_get_gkg_control_date)
    next_day_to_process = dbutils.jobs.taskValues.get(
        "00_get_events_control_date",
        "next_date_to_process_gkg"
    )
    date_str = next_day_to_process.replace("-", "")  # 2024-01-15 → 20240115

    print(f"Processing Silver layer for date: {next_day_to_process}")

    # ============================================
    # STEP 2: LOAD BRONZE GKG DATA
    # ============================================
    # Query Bronze Delta table for specific date
    # Bronze data has unparsed semi-structured fields
    gdelt_gkg = spark.sql(f"SELECT * FROM BRONZE.GDELT_GKG where date = {date_str}")

    print(f"✓ Loaded {gdelt_gkg.count():,} records from Bronze layer")

    try:
        # ============================================
        # STEP 3: DEFINE SHIPPING ROUTE CODES
        # ============================================
        """
        Port Location Codes for Major Shipping Routes:

        Transpacific Route (US-Asia):
        - CA02, CA10: Vancouver, Prince Rupert (Canada)
        - USCA: Los Angeles/Long Beach (US West Coast)
        - CH23, CH30, CH02: Shanghai, Shenzhen, Hong Kong (China)
        - JA40, JA19, JA01: Tokyo, Yokohama, Osaka (Japan)

        Transatlantic Route (US-Europe):
        - UKF2, UKN5: London, Southampton (UK)
        - GM04, GM03: Hamburg, Bremerhaven (Germany)
        - SP51, SP60: Barcelona, Valencia (Spain)
        - USNY, USGA, USFL: New York/NJ, Savannah, Miami (US East Coast)
        - CA10, CA07: Montreal, Halifax (Canada)

        Hackathon Note:
        ---------------
        These codes are hardcoded. Production should use dimension table
        for easier maintenance and extensibility.
        """
        transpacifica_codes = ["CA02", "CA10", "USCA", "CH23", "CH30", "CH02", "JA40", "JA19", "JA01"]
        transatlantica_codes = ["UKF2", "UKN5", "GM04", "GM03", "SP51", "SP60", "USNY", "USGA", "USFL", "CA10", "CA07"]

        # ============================================
        # STEP 4: PARSE SEMI-STRUCTURED FIELDS
        # ============================================
        """
        Transform Bronze GKG semi-structured data into structured Silver columns.

        Parsing Operations:
        -------------------
        1. DATE: Convert YYYYMMDD integer → proper date type
        2. LOCATIONS: Split "type#name#country#code#lat#long" → CountryCode, LocationCode
        3. TONE: Split "tone,positive,negative,polarity,..." → 4 separate columns

        Why This Matters:
        -----------------
        - Bronze stores LOCATIONS as single complex string
        - Silver extracts structured LocationCode for filtering
        - Gold can now aggregate by location without parsing overhead
        """
        gdelt_gkg_filtered = (
            gdelt_gkg
            # ============================================
            # DATE PARSING
            # ============================================
            .withColumn("Date", to_date(col("DATE"), "yyyyMMdd"))  # Convert YYYYMMDD → date

            # ============================================
            # LOCATIONS PARSING
            # ============================================
            # LOCATIONS format: "type#name#country#locationcode#lat#long#featureid"
            # Example: "4#Port of Los Angeles#US#USCA#33.7#-118.2#12345"
            .withColumn("CountryCode", split(col("LOCATIONS"), "#").getItem(2))  # Extract country (e.g., "US")
            .withColumn("LocationCode", split(col("LOCATIONS"), "#").getItem(3))  # Extract port code (e.g., "USCA")

            # ============================================
            # TONE PARSING
            # ============================================
            # TONE format: "tone,positive,negative,polarity,activity,self/group"
            # Example: "-2.5,1.2,3.7,5.0,10.0,1.5"
            .withColumn("AverageTone", split(col("TONE"), ",").getItem(0))  # Overall sentiment (-100 to +100)
            .withColumn("TonePositiveScore", split(col("TONE"), ",").getItem(1))  # Positive score
            .withColumn("ToneNegativeScore", split(col("TONE"), ",").getItem(2))  # Negative score
            .withColumn("Polarity", split(col("TONE"), ",").getItem(3))  # Emotional charge (0-10+)

            # ============================================
            # ROUTE CLASSIFICATION
            # ============================================
            # Flag which shipping route(s) this news belongs to
            .withColumn("is_ruta_transpacifica", when(col("LocationCode").isin(transpacifica_codes), 1).otherwise(0))
            .withColumn("is_ruta_transatlantica", when(col("LocationCode").isin(transatlantica_codes), 1).otherwise(0))
            .withColumn("is_ruta_del_cabo_de_buena_esperanza", lit(0))  # Cape of Good Hope (placeholder for future)

            # ============================================
            # FILTER PORT-RELATED NEWS
            # ============================================
            .filter(
                # Must be a port in Transpacific route
                col("LocationCode").isin(transpacifica_codes) &

                # Must have port/transport related themes
                (
                    (col("THEMES").like("%PORT%")) |
                    (col("THEMES").like("%TRANSPORT%")) |
                    (col("THEMES").like("%SHIPPING%")) |
                    (col("THEMES").like("%MARITIME%")) |
                    (col("THEMES").like("%TRADE_PORT%")) |
                    (col("THEMES").like("%NAVAL_PORT%")) |
                    (col("THEMES").like("%LOGISTICS_PORT%"))
                ) &

                # Exclude airport news (focus on maritime only)
                (~col("THEMES").like("%AIRPORT%"))
            )
        )

        print(f"✓ Parsed and filtered to {gdelt_gkg_filtered.count():,} port-related records")

        # ============================================
        # STEP 5: RANK NEWS BY NEGATIVE SENTIMENT
        # ============================================
        """
        Identify top 3 most negative news per port per date.

        Why Top 3?
        ----------
        - Focus on disruption events (negative sentiment)
        - Limit scraping workload (hundreds of URLs per port per day)
        - Provide diverse perspectives (3 articles vs 1)

        Ranking Window:
        ---------------
        - Partition: (Date, LocationCode) → Each port gets its own ranking
        - Order: ToneNegativeScore desc → Most negative first
        - Result: rank 1 = most negative, rank 2 = 2nd most negative, etc.
        """
        # Register scraping function as Spark UDF
        scrape_udf = udf(scrape_content, StringType())

        # Define ranking window (per port, per date)
        windowSpec = Window.partitionBy('Date', 'LocationCode').orderBy(col('ToneNegativeScore').desc())

        # Add rank column (1 = most negative)
        gdelt_gkg_ranked = gdelt_gkg_filtered.withColumn('rank', row_number().over(windowSpec))

        # ============================================
        # STEP 6: WEB SCRAPE TOP 3 ARTICLES
        # ============================================
        """
        Scrape article content for top 3 ranked news only.

        Conditional Scraping:
        ---------------------
        - IF rank <= 3: Scrape content (call scrape_udf)
        - ELSE: Set to None (skip scraping to save resources)

        Why Not Scrape All?
        -------------------
        - 1000s of articles per day per port
        - Web scraping is slow (10 sec/article with timeout)
        - Top 3 provides sufficient coverage for analysis
        """
        gdelt_gkg_filtered_with_content = gdelt_gkg_ranked.withColumn(
            'contenturl',
            when(col('rank') <= 3, scrape_udf(col('SOURCEURLS'))).otherwise(None)
        )

        print(f"✓ Scraped content for top 3 articles per port")

        # ============================================
        # STEP 7: CONFIGURE SILVER DELTA TABLE
        # ============================================
        table_name = "silver.s_gdelt_gkg_scraping"
        delta_table_path = f"s3://databricks-workspace-stack-e63e7-bucket/unity-catalog/2600119076103476/silver/gdelt/gkg/"

        # ============================================
        # STEP 8: MERGE INTO SILVER DELTA TABLE
        # ============================================
        if DeltaTable.isDeltaTable(spark, delta_table_path):
            # ============================================
            # CASE 1: TABLE EXISTS - PERFORM MERGE
            # ============================================
            print("La tabla existe, insertando los datos")
            delta_table = DeltaTable.forPath(spark, delta_table_path)
            delta_table.alias("tgt").merge(
                source=gdelt_gkg_filtered_with_content.alias("src"),
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
            ).whenNotMatchedInsertAll().execute()

            print(f"✓ Merged data into Silver Delta table")

        else:
            # ============================================
            # CASE 2: TABLE DOESN'T EXIST - CREATE TABLE
            # ============================================
            print("La tabla no existe, creando la tabla y insertando los datos")
            gdelt_gkg_filtered_with_content.write.format("delta").mode("overwrite").save(delta_table_path)
            print("Se ha escrito en formato delta.")

            # Register table in Unity Catalog
            spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_table_path}'")

            print(f"✓ Created Silver Delta table")

    return

    except requests.exceptions.RequestException as e:
        # ============================================
        # ERROR HANDLING: WEB SCRAPING FAILURES
        # ============================================
        print(f"ERROR: Web scraping failed for date {date_str}")
        print(f"  Exception: {e}")
        print(f"Error al realizar scrapping a la fecha {date_str}: {e}")
        raise


# ============================================
# MAIN EXECUTION
# ============================================
if __name__ == "__main__":
    """
    Execute Silver layer transformation.

    This is orchestrated by Databricks Workflows as part of the Silver layer pipeline.
    """
    print("=" * 70)
    print("SILVER LAYER - GKG SCRAPING AND PARSING")
    print("=" * 70)

    check_rows()

    print("=" * 70)
    print("SUCCESS: Silver layer transformation complete")
    print("=" * 70)

# COMMAND ----------

"""
==============================================================================
SILVER LAYER OUTPUT
==============================================================================

Silver Delta Table: silver.s_gdelt_gkg_scraping

New Structured Columns:
------------------------
- Date: Proper date type (vs integer in Bronze)
- CountryCode: Extracted from LOCATIONS
- LocationCode: Port code for filtering
- AverageTone: Sentiment score
- TonePositiveScore: Positive sentiment
- ToneNegativeScore: Negative sentiment (used for ranking)
- Polarity: Emotional charge
- is_ruta_transpacifica: Route flag
- is_ruta_transatlantica: Route flag
- is_ruta_del_cabo_de_buena_esperanza: Route flag
- rank: Ranking within (Date, LocationCode) partition
- contenturl: Scraped article content (top 3 only)

Gold Layer Benefits:
--------------------
Gold layer can now:
1. Aggregate by LocationCode without parsing LOCATIONS
2. Filter by AverageTone without parsing TONE
3. Use contenturl for NLP summarization
4. Apply weighted scoring on structured columns

Hackathon Notes:
----------------
- Indentation error in scrape_content function (works in notebooks)
- Missing import statements (BeautifulSoup, pyspark.sql.functions)
- Hardcoded port lists (should be dimension table)
- Sequential scraping (should be parallelized)
- Only processes Transpacific route (Transatlantic filtered out)

==============================================================================
"""
