# Databricks notebook source
"""
==============================================================================
SILVER LAYER - GKG SCRAPING AND PARSING TESTING NOTEBOOK
==============================================================================

Purpose:
--------
This notebook was used during the development phase to test the Silver layer
transformation logic for GDELT GKG data. It demonstrates parsing semi-structured
fields, filtering port-related news, and web scraping article content.

What This Notebook Does:
-------------------------
1. Parses semi-structured GKG fields (LOCATIONS, TONE) into separate columns
2. Filters news related to maritime ports on two major shipping routes:
   - Transpacific Route (US-Asia): 9 ports
   - Transatlantic Route (US-Europe): 11 ports
3. Web scrapes article content from top 3 most negative news per location/date
4. Merges enriched data into Silver Delta table

Development Context (Hackathon):
--------------------------------
This code was developed during a 1-week datathon, so some practices reflect
rapid prototyping rather than production standards:
- Hardcoded port location codes (should be dimension table)
- Sequential web scraping (should be parallelized)
- Manual route flags (should be data-driven)
- Limited error handling for web scraping failures

Key Transformations:
--------------------
1. **LOCATIONS Parsing**: Split "type#name#country#coords" → CountryCode, LocationCode
2. **TONE Parsing**: Split comma-delimited → AverageTone, PositiveScore, NegativeScore, Polarity
3. **Route Classification**: Flag news by shipping route (Transpacific, Transatlantic)
4. **Theme Filtering**: Port-related themes (PORT, TRANSPORT, SHIPPING, MARITIME)
5. **Content Enrichment**: Scrape full article text from source URLs

Why This Matters:
-----------------
- Bronze layer has raw, unparsed text fields
- Silver layer needs structured columns for analytics
- Article content enables NLP analysis (sentiment, summarization)
- Route classification enables targeted risk assessment

Author: Neutrino Solutions Team (Factored Datathon 2024)
Date: 2024
==============================================================================
"""

# ============================================
# CELL 1: SETUP AND DEPENDENCIES
# ============================================
"""
Install BeautifulSoup for web scraping HTML content.
This allows extracting article text from news source URLs.
"""
!pip install bs4

# ============================================
# IMPORTS
# ============================================
# Loading libraries for data processing and web scraping
import datetime
import requests
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, split, to_date


# COMMAND ----------

# ============================================
# CELL 2: LOAD BRONZE GKG DATA
# ============================================
"""
Load GDELT GKG data from Bronze layer for 2023.

Why 2023?
---------
- Development/testing period for the datathon
- Provides sufficient historical data for validation
- Production workflow would use control table for date range
"""
# Query Bronze layer for 2023 GKG data
gdelt_gkg = spark.sql("SELECT * FROM BRONZE.GDELT_GKG where date >= 20230101 and date <= 20231231")


# COMMAND ----------

# ============================================
# CELL 3: PARSE SEMI-STRUCTURED FIELDS & FILTER
# ============================================
"""
Transform raw GKG data into structured Silver layer format.

Key Transformations:
--------------------
1. Parse DATE → proper date column
2. Parse LOCATIONS → CountryCode, LocationCode
3. Parse TONE → AverageTone, PositiveScore, NegativeScore, Polarity
4. Classify routes (Transpacific, Transatlantic)
5. Filter port-related news only

LOCATIONS Field Format:
-----------------------
GDELT LOCATIONS field structure: "type#name#country#locationcode#lat#long#featureid"
Example: "4#Port of Los Angeles#US#USCA#33.7#-118.2#12345"
We extract:
- Element [2]: CountryCode (e.g., "US")
- Element [3]: LocationCode (e.g., "USCA")

TONE Field Format:
------------------
GDELT TONE field structure: "tone,positive,negative,polarity,activity,self/group"
Example: "-2.5,1.2,3.7,5.0,10.0,1.5"
We extract:
- Element [0]: AverageTone (-100 to +100, negative = bad news)
- Element [1]: TonePositiveScore
- Element [2]: ToneNegativeScore
- Element [3]: Polarity (emotional charge, 0-10+)
"""

# ============================================
# SHIPPING ROUTE CONFIGURATION
# ============================================
# Transpacific Route: US West Coast ↔ East Asia
# Critical ports: LA, Long Beach, Oakland (US); Shanghai, Shenzhen, Hong Kong (China); Tokyo, Yokohama (Japan)
transpacifica_codes = ["CA02", "CA10", "USCA", "CH23", "CH30", "CH02", "JA40", "JA19", "JA01"]

# Transatlantic Route: US East Coast ↔ Europe
# Critical ports: NY/NJ, Savannah, Miami (US); London, Southampton (UK); Hamburg, Bremerhaven (Germany); Barcelona (Spain)
transatlantica_codes = ["UKF2", "UKN5", "GM04", "GM03", "SP51", "SP60", "USNY", "USGA", "USFL", "CA10", "CA07"]

# ============================================
# PARSE AND FILTER GKG DATA
# ============================================
gdelt_gkg_filtered = (
    gdelt_gkg
    # ============================================
    # PARSE DATE AND LOCATION FIELDS
    # ============================================
    .withColumn("Date", to_date(col("DATE"), "yyyyMMdd"))  # Convert YYYYMMDD → date type
    .withColumn("CountryCode", split(col("LOCATIONS"), "#").getItem(2))  # Extract country code from LOCATIONS
    .withColumn("LocationCode", split(col("LOCATIONS"), "#").getItem(3))  # Extract port location code from LOCATIONS

    # ============================================
    # PARSE TONE METRICS
    # ============================================
    .withColumn("AverageTone", split(col("TONE"), ",").getItem(0))  # Extract overall sentiment
    .withColumn("TonePositiveScore", split(col("TONE"), ",").getItem(1))  # Extract positive sentiment score
    .withColumn("ToneNegativeScore", split(col("TONE"), ",").getItem(2))  # Extract negative sentiment score
    .withColumn("Polarity", split(col("TONE"), ",").getItem(3))  # Extract emotional polarity

    # ============================================
    # CLASSIFY SHIPPING ROUTES
    # ============================================
    # Flag news by shipping route for targeted analysis
    .withColumn("is_ruta_transpacifica", when(col("LocationCode").isin(transpacifica_codes), 1).otherwise(0))  # Transpacific Route
    .withColumn("is_ruta_transatlantica", when(col("LocationCode").isin(transatlantica_codes), 1).otherwise(0))  # Transatlantic Route
    .withColumn("is_ruta_del_cabo_de_buena_esperanza", lit(0))  # Cape of Good Hope Route (placeholder for future)

    # ============================================
    # FILTER PORT-RELATED NEWS
    # ============================================
    .filter(
        # Filter 1: Must be a port in Transpacific route
        col("LocationCode").isin(transpacifica_codes) &

        # Filter 2: Must have port/transport related themes
        (
            (col("THEMES").like("%PORT%")) |
            (col("THEMES").like("%TRANSPORT%")) |
            (col("THEMES").like("%SHIPPING%")) |
            (col("THEMES").like("%MARITIME%")) |
            (col("THEMES").like("%TRADE_PORT%")) |
            (col("THEMES").like("%NAVAL_PORT%")) |
            (col("THEMES").like("%LOGISTICS_PORT%"))
        ) &

        # Filter 3: Exclude airport-related news (focus on maritime only)
        (~col("THEMES").like("%AIRPORT%"))
    )
)


# COMMAND ----------

# ============================================
# CELL 4: WEB SCRAPING FUNCTION DEFINITION
# ============================================
"""
Scrape article content from source URLs.

Purpose:
--------
GDELT GKG provides URLs but not full article text. We need article content for:
- NLP summarization (Gold layer)
- Sentiment analysis validation
- Theme verification

Why Top 3 Most Negative News?
------------------------------
- Focus on disruption events (negative sentiment)
- Limit scraping to most relevant articles per location/date
- Avoid overwhelming web scraping workload (hackathon time constraint)

Error Handling:
---------------
- Timeout after 10 seconds (avoid hanging on slow sites)
- Return error message as string (allows processing to continue)
- Production should use retry logic and proxy rotation
"""

def scrape_content(url):
    """
    Scrape article content from news source URL.

    This function:
    1. Fetches HTML content from URL
    2. Parses with BeautifulSoup
    3. Extracts all paragraph text
    4. Returns concatenated content

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
    - Production should use Selenium for dynamic content
    """
    try:
        # Fetch HTML with timeout
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract all paragraph tags
        paragraphs = soup.find_all('p')

        # Concatenate paragraph text
        content = ' '.join([p.get_text() for p in paragraphs])

        return content
    except Exception as e:
        # Return error message (allows processing to continue)
        return str(e)

# ============================================
# REGISTER UDF FOR SPARK
# ============================================
# Register scraping function as Spark UDF for distributed execution
scrape_udf = udf(scrape_content, StringType())

# COMMAND ----------

# ============================================
# CELL 5: SCRAPE TOP 3 NEWS PER LOCATION/DATE
# ============================================
"""
Scrape article content for top 3 most negative news per location and date.

Strategy:
---------
1. Rank news by ToneNegativeScore (higher = more negative)
2. Partition by (Date, LocationCode) to get top 3 per group
3. Scrape content only for top 3 (optimize scraping workload)
4. Others get NULL content (not needed for analysis)

Why This Approach?
------------------
- Focuses on disruption events (negative sentiment)
- Limits scraping to ~3 URLs per port per day
- Avoids overwhelming web scraping infrastructure
- Production should use job queue and rate limiting
"""

# ============================================
# DEFINE RANKING WINDOW
# ============================================
# Partition by Date and LocationCode, order by most negative tone
windowSpec = Window.partitionBy('Date', 'LocationCode').orderBy(col('ToneNegativeScore').desc())

# ============================================
# RANK AND SCRAPE
# ============================================
# Rank news by negative tone within each (Date, LocationCode) group
gdelt_gkg_ranked = gdelt_gkg_filtered.withColumn('rank', row_number().over(windowSpec))

# Scrape content only for top 3 ranked news (others get NULL)
gdelt_gkg_filtered_with_content = gdelt_gkg_ranked.withColumn(
    'contenturl',
    when(col('rank') <= 3, scrape_udf(col('SOURCEURLS'))).otherwise(None)
)

# Display results for validation
display(gdelt_gkg_filtered_with_content.select('Date', 'LocationCode', 'ToneNegativeScore', 'contenturl'))


# COMMAND ----------

# ============================================
# CELL 6: DELTA TABLE CONFIGURATION
# ============================================
"""
Configure Silver layer Delta table for scraped GKG data.

Silver Layer Purpose:
---------------------
- Store cleaned, parsed, and enriched GKG data
- Add scraped article content for downstream NLP
- Enable Gold layer to perform weighted scoring
"""

# Silver table name in Unity Catalog
table_name = "silver.s_gdelt_gkg_scraping"

# S3 path for Delta table storage
delta_table_path = f"s3://databricks-workspace-stack-e63e7-bucket/unity-catalog/2600119076103476/silver/gdelt/gkg/"


# COMMAND ----------

# ============================================
# CELL 7: UPSERT TO SILVER DELTA TABLE
# ============================================
"""
Merge enriched GKG data into Silver Delta table.

Merge Strategy:
---------------
- Match on all fields except contenturl (complex composite key)
- Insert only if no match found (whenNotMatchedInsertAll)
- No updates (content doesn't change after scraping)

Why Complex Merge Condition?
-----------------------------
- GKG has no unique identifier
- Must match on combination of fields
- Prevents duplicate scraping of same articles

Production Considerations:
---------------------------
- This merge condition is expensive (no indexed primary key)
- Production should add surrogate key (hash of key fields)
- Consider CDC pattern instead of full merge
"""

# Check if table exists
if DeltaTable.isDeltaTable(spark, delta_table_path):
    # ============================================
    # CASE 1: TABLE EXISTS - PERFORM MERGE
    # ============================================
    print("La tabla existe, insertando los datos")

    # Load existing Delta table
    delta_table = DeltaTable.forPath(spark, delta_table_path)

    # Merge on composite key (all fields except contenturl)
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

    print("✓ Merge completed successfully")

else:
    # ============================================
    # CASE 2: TABLE DOESN'T EXIST - CREATE TABLE
    # ============================================
    print("La tabla no existe, creando la tabla y insertando los datos")

    # Write DataFrame as Delta table
    gdelt_gkg_filtered_with_content.write.format("delta").mode("overwrite").save(delta_table_path)
    print("Se ha escrito en formato delta.")

    # Register table in Unity Catalog
    spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_table_path}'")

    print("✓ Table created and registered in Unity Catalog")


# COMMAND ----------

"""
==============================================================================
TESTING NOTEBOOK COMPLETION
==============================================================================

What Was Tested:
----------------
✓ Semi-structured field parsing (LOCATIONS, TONE)
✓ Route classification logic (Transpacific, Transatlantic)
✓ Theme-based filtering for port-related news
✓ Web scraping UDF with error handling
✓ Top-N ranking pattern (top 3 per location/date)
✓ Delta table merge with complex composite key

Next Steps:
-----------
1. Production workflow: Move to workflow/gkg/01_scrap_data.py
2. Add error handling for failed scrapes
3. Implement retry logic for timeouts
4. Consider using Selenium for dynamic content
5. Add data quality checks (verify parsed fields)

Hackathon Notes:
----------------
This was developed in a 1-week datathon, so shortcuts were taken:
- Hardcoded port lists (should be dimension table)
- Sequential scraping (should be parallelized)
- Limited error handling
- No rate limiting for web scraping

==============================================================================
"""
