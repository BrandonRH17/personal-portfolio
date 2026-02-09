# Databricks notebook source
"""
==============================================================================
GOLD LAYER - NEWS SUMMARIZATION WORKFLOW TESTING NOTEBOOK
==============================================================================

Purpose:
--------
This notebook was used during the development phase to test AI-powered news
summarization for the Gold layer. It demonstrates filtering disruption-related
news, using BART-large-CNN for text summarization, and creating dashboard-ready
summary tables.

What This Notebook Does:
-------------------------
1. Filters Silver GKG data for port-related disruption news (negative tone)
2. Ranks news by theme relevance (5 key disruption themes)
3. Extracts top 3 most relevant news per route per day
4. Uses Facebook's BART-large-CNN to generate news summaries
5. Creates dashboard-ready dataset with summaries and metadata

Development Context (Hackathon):
--------------------------------
This was developed during a 1-week datathon for the Power BI dashboard feature
that shows "Top Disruption News" with AI-generated summaries. Time constraints
meant some shortcuts were taken:
- Hardcoded theme weights (should be configurable)
- Sequential summarization (should be batched)
- CPU-only BART inference (should use GPU)
- Manual pandas conversion (should stay in Spark)

Key Innovation:
---------------
This notebook combines traditional data engineering with modern NLP:
- GDELT provides raw news data
- Our pipeline filters relevant disruption events
- BART generates human-readable summaries
- Dashboard presents actionable intelligence

AI Model Used:
--------------
Model: facebook/bart-large-cnn
Purpose: Extractive + abstractive summarization
Why BART?: State-of-art summarization, handles long text, no fine-tuning needed
Input: 200-3500 characters of article text
Output: 30+ character summary

Author: Neutrino Solutions Team (Factored Datathon 2024 - Overall Grand Winner)
Date: 2024
==============================================================================
"""

# ============================================
# CELL 1: IMPORTS
# ============================================
"""
Load dependencies for NLP summarization and data processing.

Key Libraries:
--------------
- transformers: Hugging Face library for BART model
- pyspark.sql.functions: Data transformations
- delta.tables: Delta Lake operations
"""
from transformers import pipeline
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import datetime
from delta.tables import DeltaTable

# COMMAND ----------

# ============================================
# CELL 2: DATE CONFIGURATION
# ============================================
"""
Calculate date range for processing.

In Production:
--------------
- Would use TABLE_CONTROL for date tracking
- Workflow triggered by Silver layer completion
- Processes incremental data only

For Testing:
------------
- Uses yesterday's date for demo purposes
- Can be manually adjusted for historical testing
"""
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

print(f"Processing date: {yesterday}")

# COMMAND ----------

# ============================================
# CELL 3: LOAD SILVER DATA
# ============================================
"""
Load scraped and parsed GKG data from Silver layer.

Silver Layer Contains:
----------------------
- Parsed LOCATIONS and TONE fields
- Scraped article content
- Route classifications
- Filtered port-related news only
"""
gkg_scraped = spark.sql("SELECT * FROM SILVER.S_GDELT_GKG_SCRAPING")

print(f"✓ Loaded {gkg_scraped.count():,} records from Silver layer")

# COMMAND ----------

# ============================================
# CELL 4: FILTER AND RANK NEWS - TRANSPACIFIC ROUTE
# ============================================
"""
Filter and rank disruption-related news for Transpacific Route.

5 Key Disruption Themes:
------------------------
1. TRANSPORT_INFRASTRUCTURE: Port capacity, equipment failures
2. TRADE: Trade disputes, tariffs, policy changes
3. MACROECONOMIC: Economic downturns affecting shipping
4. PUBLIC_SECTOR: Government regulations, port closures
5. MARITIME_INCIDENT: Accidents, spills, collisions

Ranking Logic:
--------------
- Each theme presence = +1 point
- Higher total = more themes mentioned = more comprehensive coverage
- Top 3 news per day provide diverse perspective on disruptions

Why Negative Tone Only?
------------------------
- Focus on disruption events (positive news less relevant for risk)
- Negative sentiment correlates with supply chain issues
- Dashboard users want early warning signals
"""

NEWS_RELATED_TO_PORT_PROBLEMS_TPR = (
    gkg_scraped
    # ============================================
    # FILTER BY DATE AND SENTIMENT
    # ============================================
    .filter(col("Date") >= yesterday)  # Recent news only
    .filter(col("AverageTone") < 0)     # Negative sentiment (disruption)

    # ============================================
    # THEME FLAGS (5 KEY DISRUPTION CATEGORIES)
    # ============================================
    .withColumn("NewsWithTINFA", when(col("THEMES").like("%TRANSPORT_INFRASTRUCTURE%"), 1))  # Infrastructure issues
    .withColumn("NewsWithTRADE", when(col("THEMES").like("%TRADE%"), 1))                    # Trade disruptions
    .withColumn("NewsWithME", when(col("THEMES").like("%MACROECONOMIC%"), 1))                # Economic issues
    .withColumn("NewsWithPS", when(col("THEMES").like("%PUBLIC_SECTOR%"), 1))                # Regulatory issues
    .withColumn("NewsWithMI", when(col("THEMES").like("%MARITIME_INCIDENT%"), 1))            # Accidents/incidents

    # Fill NULL flags with 0
    .fillna(0)

    # ============================================
    # FILTER OUT SCRAPING ERRORS
    # ============================================
    .filter(~col("contenturl").like('%Client Error%'))  # Failed HTTP requests
    .filter(~col("contenturl").like('%OK XID%'))        # Malformed responses

    # ============================================
    # FILTER BY ROUTE
    # ============================================
    .filter("is_ruta_transpacifica == 1")  # Transpacific Route only

    # ============================================
    # CALCULATE THEME SCORE AND RANK
    # ============================================
    .withColumn("Total", col("NewsWithTINFA") + col("NewsWithTRADE") + col("NewsWithME") + col("NewsWithPS") + col("NewsWithMI"))
    .withColumn('Rank', row_number().over(Window.partitionBy("Date").orderBy(col("Total").desc())))
    .filter("Rank <= 3")  # Top 3 most comprehensive news per day
)

# COMMAND ----------

# ============================================
# CELL 5: FILTER AND RANK NEWS - TRANSATLANTIC ROUTE
# ============================================
"""
Same logic as Transpacific Route, but for Transatlantic shipping lane.

Note: Uses hardcoded date '2024-08-15' for testing.
Production would use control table or yesterday's date.
"""

NEWS_RELATED_TO_PORT_PROBLEMS_TAR = (
    gkg_scraped
    .filter(col("Date") >= '2024-08-15')  # Testing date
    .filter(col("AverageTone") < 0)

    # Theme flags (same as Transpacific)
    .withColumn("NewsWithTINFA", when(col("THEMES").like("%TRANSPORT_INFRASTRUCTURE%"), 1))
    .withColumn("NewsWithTRADE", when(col("THEMES").like("%TRADE%"), 1))
    .withColumn("NewsWithME", when(col("THEMES").like("%MACROECONOMIC%"), 1))
    .withColumn("NewsWithPS", when(col("THEMES").like("%PUBLIC_SECTOR%"), 1))
    .withColumn("NewsWithMI", when(col("THEMES").like("%MARITIME_INCIDENT%"), 1))

    .fillna(0)

    # Filter scraping errors
    .filter(~col("contenturl").like('%Client Error%'))
    .filter(~col("contenturl").like('%OK XID%'))

    # Filter by route
    .filter("is_ruta_transatlantica == 1")  # Transatlantic Route

    # Calculate score and rank
    .withColumn("Total", col("NewsWithTINFA") + col("NewsWithTRADE") + col("NewsWithME") + col("NewsWithPS") + col("NewsWithMI"))
    .withColumn('Rank', row_number().over(Window.partitionBy("Date").orderBy(col("Total").desc())))
    .filter("Rank <= 3")
)

# COMMAND ----------

# ============================================
# CELL 6: LOAD BART SUMMARIZATION MODEL
# ============================================
"""
Initialize Facebook's BART-large-CNN model for text summarization.

Model Details:
--------------
- Model: facebook/bart-large-cnn
- Task: Text summarization (extractive + abstractive)
- Size: ~1.6 GB
- Performance: State-of-art on CNN/DailyMail benchmark

Why BART?
---------
- Pre-trained on news articles (perfect for GDELT)
- Handles long documents (up to 1024 tokens)
- Generates fluent, coherent summaries
- No fine-tuning needed (zero-shot)

Hackathon Note:
---------------
- Running on CPU (slow but works)
- Production should use GPU cluster
- Consider caching model for repeated runs
"""
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

print("✓ BART model loaded successfully")

# COMMAND ----------

# ============================================
# CELL 7: CONVERT TO PANDAS FOR NLP PROCESSING
# ============================================
"""
Convert Spark DataFrames to pandas for BART processing.

Why Pandas?
-----------
- Transformers library works with Python lists/strings
- Easier to iterate for sequential summarization
- Hackathon time constraint (no time for distributed NLP)

Production Improvement:
-----------------------
- Use spark-nlp for distributed inference
- Batch processing with UDFs
- GPU acceleration
"""
GKG_2024_MOST_IMPORTANT_NEWS_PER_DAY_P_TPR = NEWS_RELATED_TO_PORT_PROBLEMS_TPR.select('Date', 'Rank', 'contenturl').toPandas()
GKG_2024_MOST_IMPORTANT_NEWS_PER_DAY_P_TAR = NEWS_RELATED_TO_PORT_PROBLEMS_TAR.select('Date', 'Rank', 'contenturl').toPandas()

print(f"✓ Transpacific: {len(GKG_2024_MOST_IMPORTANT_NEWS_PER_DAY_P_TPR)} articles to summarize")
print(f"✓ Transatlantic: {len(GKG_2024_MOST_IMPORTANT_NEWS_PER_DAY_P_TAR)} articles to summarize")

# COMMAND ----------

# ============================================
# CELL 8: EXTRACT CONTENT FOR SUMMARIZATION
# ============================================
"""
Extract article content from pandas DataFrames.
"""
content_list_TPR = GKG_2024_MOST_IMPORTANT_NEWS_PER_DAY_P_TPR['contenturl'].to_list()
content_list_TAR = GKG_2024_MOST_IMPORTANT_NEWS_PER_DAY_P_TAR['contenturl'].to_list()

# COMMAND ----------

# ============================================
# CELL 9: CLEAN CONTENT FOR BART INPUT
# ============================================
"""
Truncate article content to optimal length for BART.

Why 200-3500 characters?
------------------------
- Skip first 200 chars (often ads, headers, metadata)
- Limit to 3500 chars (BART max context ~1024 tokens ≈ 4000 chars)
- Balances context vs computation time

Production Improvement:
-----------------------
- Use smart truncation (sentence boundaries)
- Extract main content only (remove boilerplate)
- Handle non-English content
"""
content_list_cleaned_TPR = []

for i in range(0, len(content_list_TPR)):
    content_list_cleaned_TPR.append(content_list_TPR[i][200:3500])

content_list_cleaned_TAR = []

for i in range(0, len(content_list_TAR)):
    content_list_cleaned_TAR.append(content_list_TAR[i][200:3500])

print(f"✓ Content cleaned for {len(content_list_cleaned_TPR) + len(content_list_cleaned_TAR)} articles")

# COMMAND ----------

# ============================================
# CELL 10: GENERATE SUMMARIES - TRANSPACIFIC
# ============================================
"""
Generate AI summaries for Transpacific Route news.

BART Parameters:
----------------
- min_length=30: Minimum summary length (avoid too-short summaries)
- do_sample=False: Use greedy decoding (deterministic, faster)

Processing Time:
----------------
- ~10-20 seconds per article on CPU
- ~100 articles = 15-30 minutes total
- GPU would be 10-50x faster

Hackathon Note:
---------------
- Sequential processing (slow but simple)
- Production should batch and parallelize
"""
resume_content_TPR = []

for i in range(0, len(content_list_cleaned_TPR)):
    # Generate summary
    text = summarizer(content_list_cleaned_TPR[i], min_length=30, do_sample=False)[0]['summary_text']

    print(text)
    resume_content_TPR.append(text)
    print(f"✓ Summarized article {i + 1}/{len(content_list_cleaned_TPR)}")

# COMMAND ----------

# ============================================
# CELL 11: VERIFY SUMMARY COUNT
# ============================================
print(f"✓ Generated {len(resume_content_TPR)} summaries for Transpacific Route")

# COMMAND ----------

# ============================================
# CELL 12: GENERATE SUMMARIES - TRANSATLANTIC
# ============================================
"""
Generate AI summaries for Transatlantic Route news.
Same logic as Transpacific Route.
"""
resume_content_TAR = []

for i in range(0, len(content_list_cleaned_TAR)):
    text = summarizer(content_list_cleaned_TAR[i], min_length=30, do_sample=False)[0]['summary_text']

    print(text)
    resume_content_TAR.append(text)
    print(f"✓ Summarized article {i + 1}/{len(content_list_cleaned_TAR)}")

# COMMAND ----------

# ============================================
# CELL 13: ADD SUMMARIES TO DATAFRAMES
# ============================================
"""
Append generated summaries back to pandas DataFrames.
"""
GKG_2024_MOST_IMPORTANT_NEWS_PER_DAY_P_TPR['Summary'] = resume_content_TPR
GKG_2024_MOST_IMPORTANT_NEWS_PER_DAY_P_TAR['Summary'] = resume_content_TAR

print("✓ Summaries added to DataFrames")

# COMMAND ----------

# ============================================
# CELL 14: ADD ROUTE FLAGS
# ============================================
"""
Add route identifier flags for dashboard filtering.
"""
GKG_2024_MOST_IMPORTANT_NEWS_PER_DAY_P_TPR['is_ruta_transpacifica'] = 1
GKG_2024_MOST_IMPORTANT_NEWS_PER_DAY_P_TAR['is_ruta_transatlantica'] = 1

# COMMAND ----------

# ============================================
# CELL 15: CONVERT BACK TO SPARK DATAFRAMES
# ============================================
"""
Convert pandas DataFrames back to Spark for Delta Lake storage.
"""
SPARK_GKG_NEWS_TPR = spark.createDataFrame(GKG_2024_MOST_IMPORTANT_NEWS_PER_DAY_P_TPR)
SPARK_GKG_NEWS_TAR = spark.createDataFrame(GKG_2024_MOST_IMPORTANT_NEWS_PER_DAY_P_TAR)

print("✓ Converted back to Spark DataFrames")

# COMMAND ----------

# ============================================
# CELL 16: JOIN SUMMARIES WITH FULL DATA - TRANSPACIFIC
# ============================================
"""
Join summarized news back with full GKG metadata.

This creates the final Gold layer dataset with:
- Original GKG metadata (date, location, tone)
- Route classifications
- Source URLs
- AI-generated summaries
"""
NEWS_RELATED_TO_PORT_PROBLEMS_TPR_SUMMARIZED = (
    gkg_scraped
    .filter(col("Date") >= '2024-08-15')
    .filter(col("AverageTone") < 0)

    # Add theme flags
    .withColumn("NewsWithTINFA", when(col("THEMES").like("%TRANSPORT_INFRASTRUCTURE%"), 1))
    .withColumn("NewsWithTRADE", when(col("THEMES").like("%TRADE%"), 1))
    .withColumn("NewsWithME", when(col("THEMES").like("%MACROECONOMIC%"), 1))
    .withColumn("NewsWithPS", when(col("THEMES").like("%PUBLIC_SECTOR%"), 1))
    .withColumn("NewsWithMI", when(col("THEMES").like("%MARITIME_INCIDENT%"), 1))

    .fillna(0)

    # Filter errors
    .filter(~col("contenturl").like('%Client Error%'))
    .filter(~col("contenturl").like('%OK XID%'))

    # Filter route
    .filter("is_ruta_transpacifica == 1")

    # Calculate score and rank
    .withColumn("Total", col("NewsWithTINFA") + col("NewsWithTRADE") + col("NewsWithME") + col("NewsWithPS") + col("NewsWithMI"))
    .withColumn('Rank', row_number().over(Window.partitionBy("Date").orderBy(col("Total").desc())))

    # Join with summaries
    .join(SPARK_GKG_NEWS_TPR.drop('contenturl'), ['Date', 'Rank', 'is_ruta_transpacifica'], 'left')

    # Select final columns for Gold layer
    .select('DATE', 'EXTRACTION_DATE', 'COUNTRYCODE', 'LOCATIONCODE', 'AVERAGETONE',
            'IS_RUTA_TRANSPACIFICA', 'IS_RUTA_TRANSATLANTICA', 'IS_RUTA_DEL_CABO_DE_BUENA_ESPERANZA',
            'SOURCEURLS', 'SUMMARY')
)

# COMMAND ----------

# ============================================
# CELL 17: JOIN SUMMARIES WITH FULL DATA - TRANSATLANTIC
# ============================================
"""
Same join logic for Transatlantic Route.
"""
NEWS_RELATED_TO_PORT_PROBLEMS_TAR_SUMMARIZED = (
    gkg_scraped
    .filter(col("Date") >= '2024-08-15')
    .filter(col("AverageTone") < 0)

    .withColumn("NewsWithTINFA", when(col("THEMES").like("%TRANSPORT_INFRASTRUCTURE%"), 1))
    .withColumn("NewsWithTRADE", when(col("THEMES").like("%TRADE%"), 1))
    .withColumn("NewsWithME", when(col("THEMES").like("%MACROECONOMIC%"), 1))
    .withColumn("NewsWithPS", when(col("THEMES").like("%PUBLIC_SECTOR%"), 1))
    .withColumn("NewsWithMI", when(col("THEMES").like("%MARITIME_INCIDENT%"), 1))

    .fillna(0)

    .filter(~col("contenturl").like('%Client Error%'))
    .filter(~col("contenturl").like('%OK XID%'))

    .filter("is_ruta_transatlantica == 1")

    .withColumn("Total", col("NewsWithTINFA") + col("NewsWithTRADE") + col("NewsWithME") + col("NewsWithPS") + col("NewsWithMI"))
    .withColumn('Rank', row_number().over(Window.partitionBy("Date").orderBy(col("Total").desc())))

    .join(SPARK_GKG_NEWS_TAR.drop('contenturl'), ['Date', 'Rank', 'is_ruta_transatlantica'], 'left')

    .select('DATE', 'EXTRACTION_DATE', 'COUNTRYCODE', 'LOCATIONCODE', 'AVERAGETONE',
            'IS_RUTA_TRANSPACIFICA', 'IS_RUTA_TRANSATLANTICA', 'IS_RUTA_DEL_CABO_DE_BUENA_ESPERANZA',
            'SOURCEURLS', 'SUMMARY')
)

# ============================================
# UNION BOTH ROUTES
# ============================================
"""
Combine Transpacific and Transatlantic datasets into single Gold table.
"""
FINAL_DATASET_NEWS = (
    NEWS_RELATED_TO_PORT_PROBLEMS_TPR_SUMMARIZED.union(NEWS_RELATED_TO_PORT_PROBLEMS_TAR_SUMMARIZED)
)

print(f"✓ Final dataset created: {FINAL_DATASET_NEWS.count():,} summarized news articles")

# COMMAND ----------

# ============================================
# CELL 18: DELTA TABLE CONFIGURATION
# ============================================
"""
Configure Gold layer Delta table for summarized news.

This table powers the "Top Disruption News" dashboard feature.
"""
table_name = "gold.g_gdelt_gkg_weights_detail_report"
delta_table_path = f"s3://databricks-workspace-stack-e63e7-bucket/unity-catalog/2600119076103476/gold/gdelt/g_gdelt_gkg_weights_detail_report/"

# COMMAND ----------

# ============================================
# CELL 19: UPSERT TO GOLD DELTA TABLE
# ============================================
"""
Merge summarized news into Gold Delta table.

Note: Merge condition has issues (references non-existent columns).
This was a hackathon bug that should be fixed in production.
"""
if DeltaTable.isDeltaTable(spark, delta_table_path):
    print("La tabla existe, insertando los datos")

    delta_table = DeltaTable.forPath(spark, delta_table_path)
    delta_table.alias("tgt").merge(
        source=FINAL_DATASET_NEWS.alias("src"),
        condition=(
            "tgt.DATE = src.DATE AND "
            "tgt.Total = src.Total AND "
            "tgt.LocationCode = src.LocationCode AND "
            "tgt.NumberOfNews = src.NumberOfNews"
        )
    ).whenNotMatchedInsertAll().execute()

    print("✓ Merge completed")

else:
    print("La tabla no existe, creando la tabla y insertando los datos")

    FINAL_DATASET_NEWS.write.format("delta").mode("overwrite").save(delta_table_path)
    print("Se ha escrito en formato delta.")

    spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_table_path}'")

    print("✓ Table created and registered")

# COMMAND ----------

"""
==============================================================================
TESTING NOTEBOOK COMPLETION
==============================================================================

What Was Tested:
----------------
✓ Theme-based news filtering and ranking
✓ BART-large-CNN model integration
✓ AI-powered text summarization (30+ char summaries)
✓ Pandas ↔ Spark conversions for NLP processing
✓ Multi-route processing (Transpacific, Transatlantic)
✓ Delta Lake merge for Gold layer

Dashboard Impact:
-----------------
This Gold table powers the "Top Disruption News" dashboard card that shows:
- Top 3 most relevant news per route per day
- AI-generated summaries for quick reading
- Links to source articles for deep dives
- Route-specific filtering

Hackathon Achievements:
-----------------------
- Integrated cutting-edge NLP into data pipeline
- Delivered actionable intelligence, not just metrics
- Won Overall Grand Winner at Factored Datathon 2024

Production Improvements Needed:
--------------------------------
1. GPU acceleration for BART inference
2. Batch processing (not sequential)
3. Distributed NLP with spark-nlp
4. Better content extraction (not just <p> tags)
5. Fix merge condition bug (references wrong columns)
6. Add summary quality validation
7. Handle non-English content

==============================================================================
"""
