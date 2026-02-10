# Databricks notebook source
"""
==============================================================================
GOLD LAYER - BUILD GKG WEIGHTED NEWS SUMMARY
==============================================================================

Purpose:
--------
This notebook is the core transformation step in the Gold layer workflow for
GDELT GKG. It reads Silver GKG data, filters emotionally-charged news, applies
exponential weighted scoring based on disruption themes, and aggregates metrics
for Power BI dashboard consumption.

Workflow Position:
------------------
Step 2 of 3 in Gold Layer GKG Processing:
    [1] Get Control Date
  → [2] Build Weighted News Summary (this script)
    [3] Update Control Date

Key Transformations:
--------------------
1. Filter emotionally-charged news (neutrality + polarity paradox)
2. Identify 5 key disruption themes in port news
3. Apply exponential weighted scoring (5 themes = 500x multiplier)
4. Aggregate by date/location/route
5. Write to Gold Delta table for analytics

Gold Layer Value:
-----------------
This is the winning formula from Factored Datathon 2024!

The exponential weighting system amplifies news that mention multiple
disruption themes simultaneously:

  Themes | Weight | Impact Example
  -------|--------|------------------------------------------
    5    |  500x  | Port + Transport + Trade + Macro + Incident = MAJOR CRISIS
    4    |  250x  | 4 themes = Significant disruption event
    3    |  100x  | 3 themes = Notable supply chain issue
    2    |   5x   | 2 themes = Minor port-related news
    1    |   0x   | 1 theme = Filtered out (too generic)

Why This Matters:
-----------------
A single news article mentioning all 5 themes (e.g., "Port strike disrupts
trade, impacts economy, maritime incident reported") carries 500x more weight
than generic port news. This mathematical approach identifies supply chain
disruptions that genuinely impact shipping routes.

Emotional Charge Filter:
-------------------------
The "neutrality + polarity paradox" filter removes sensationalist news:

  - Neutrality: AverageTone between -0.5 and 0.5 (neither positive nor negative)
  - Polarity: Emotional intensity score (0-10+)
  - Paradox: Neutral tone + high polarity = Emotionally charged but neutral framing
  - Filter: Exclude articles with (Neutral=1 AND Polarity≥9)

This removes clickbait headlines like "SHOCKING port news leaves economists
STUNNED" that have neutral sentiment but high emotional language.

5 Disruption Themes:
--------------------
1. TRANSPORT_INFRASTRUCTURE (TINFA): Physical port/route damage
2. TRADE: Commercial impact, tariffs, restrictions
3. MACROECONOMIC (ME): Economic indicators, GDP impact
4. PUBLIC_SECTOR (PS): Government policy, regulations
5. MARITIME_INCIDENT (MI): Accidents, collisions, groundings

Dependencies:
-------------
- Previous task: 00_get_gkg_control_date
- Silver Delta table: SILVER.S_GDELT_GKG_SCRAPING
- PySpark functions: col, when, count
- Delta Lake for ACID transactions

Author: Neutrino Solutions Team (Factored Datathon 2024)
Date: 2024
==============================================================================
"""

# ============================================
# IMPORTS
# ============================================
import requests          # Not used in this script (legacy import)
import zipfile           # Not used in this script (legacy import)
import io                # Not used in this script (legacy import)
import pandas as pd      # Not used in this script (legacy import)
import boto3             # Not used in this script (legacy import)
import pyarrow as pa     # Not used in this script (legacy import)
import pyarrow.parquet as pq  # Not used in this script (legacy import)
from botocore.exceptions import NoCredentialsError  # Not used in this script
from datetime import datetime, timedelta

# Note: Missing imports that should be added:
# from pyspark.sql.functions import col, when, count
# from delta.tables import DeltaTable


# ============================================
# MAIN PROCESSING FUNCTION
# ============================================
def check_rows():
    """
    Main Gold layer transformation function.

    This function orchestrates the entire Gold layer workflow:
    1. Load Silver GKG data for specific date
    2. Filter emotionally-charged news (neutrality + polarity paradox)
    3. Identify 5 key disruption themes
    4. Apply exponential weighted scoring (5 themes = 500x)
    5. Aggregate by date/location/route
    6. Merge into Gold Delta table

    Process Flow:
    -------------
    Silver GKG → Filter Emotional Charge → Theme Detection → Weighted Scoring → Gold Delta
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

    print(f"Processing Gold layer for date: {next_day_to_process}")

    # ============================================
    # STEP 2: LOAD SILVER GKG DATA
    # ============================================
    # Query Silver Delta table for specific date
    # Silver data has parsed fields and scraped content
    gkg_scraped = spark.sql(
        f"SELECT * FROM SILVER.S_GDELT_GKG_SCRAPING where date = {date_str}"
    )

    print(f"✓ Loaded {gkg_scraped.count():,} records from Silver layer")

    try:
        # ============================================
        # STEP 3: FILTER EMOTIONALLY-CHARGED NEWS
        # ============================================
        """
        Apply "Neutrality + Polarity Paradox" filter.

        This removes sensationalist/clickbait news that has:
        - Neutral sentiment (AverageTone ≈ 0)
        - High emotional intensity (Polarity ≥ 9)

        Logic:
        ------
        1. Neutrality Flag: 1 if AverageTone in [-0.5, 0.5], else 0
        2. Emotional Charge (EC): 1 if (Neutral=1 AND Polarity≥9), else 0
        3. Filter: Keep only EC=0 (not emotionally charged)

        Example Filtered Out:
        ---------------------
        "SHOCKING port strike leaves economists STUNNED but trade continues"
        - AverageTone = 0.2 (neutral) → Neutrality=1
        - Polarity = 9.5 (high emotional language) → EC=1
        - Result: Filtered out (EC=1)

        Why This Matters:
        -----------------
        Clickbait headlines distort supply chain risk analysis. This filter
        ensures only substantive disruption news is included in Gold metrics.
        """
        gkg_scraped_without_emotional_charge = (
            gkg_scraped
            # ============================================
            # NEUTRALITY FLAG
            # ============================================
            # Flag articles with neutral sentiment (neither positive nor negative)
            .withColumn(
                "Neutrality",
                when(
                    (col("AverageTone") >= -0.5) & (col("AverageTone") <= 0.5),
                    1
                ).otherwise(0)
            )  # 1=NEUTRAL, 0=NOT NEUTRAL

            # ============================================
            # EMOTIONAL CHARGE FLAG
            # ============================================
            # Flag articles with neutral tone but high emotional intensity (paradox)
            .withColumn(
                "EC",
                when(
                    (col("Neutrality") == 1) & (col('Polarity') >= 9),
                    1
                ).otherwise(0)
            )  # 1=EMOTIONAL CHARGE, 0=NOT EMOTIONAL CHARGED

            # ============================================
            # FILTER OUT EMOTIONAL CHARGE
            # ============================================
            # Keep only news that is NOT emotionally charged
            .filter("EC == 0")
        )

        print(f"✓ Filtered to {gkg_scraped_without_emotional_charge.count():,} non-emotional news")

        # ============================================
        # STEP 4: APPLY EXPONENTIAL WEIGHTED SCORING
        # ============================================
        """
        Identify 5 key disruption themes and apply exponential weighting.

        This is the winning formula from Factored Datathon 2024!

        5 Key Disruption Themes:
        ------------------------
        1. TRANSPORT_INFRASTRUCTURE: Physical port/route damage
        2. TRADE: Commercial impact, tariffs, restrictions
        3. MACROECONOMIC: Economic indicators, GDP impact
        4. PUBLIC_SECTOR: Government policy, regulations
        5. MARITIME_INCIDENT: Accidents, collisions, groundings

        Exponential Weighting System:
        ------------------------------
        Total Themes | Weight Multiplier | Rationale
        -------------|-------------------|----------------------------------
            5        |       500x        | CRITICAL: All disruption factors
            4        |       250x        | MAJOR: Most disruption factors
            3        |       100x        | SIGNIFICANT: Multiple factors
            2        |        5x         | MINOR: Limited scope
            1        |        0x         | EXCLUDED: Too generic

        Example Calculation:
        --------------------
        Scenario: 3 articles mention Port+Transport+Trade+Macro+Incident (5 themes)

        Without weighting:
          3 articles = 3 news count

        With exponential weighting:
          3 articles × 500 = 1,500 weighted news count

        This amplification correctly signals a major supply chain disruption
        event that should appear prominently in the Power BI dashboard.

        Why This Works:
        ----------------
        Real disruptions (port strikes, maritime accidents) trigger news
        articles that naturally mention multiple themes. Generic port news
        mentions only 1-2 themes. The exponential scaling mathematically
        separates signal from noise.
        """
        gkg_with_weights = (
            gkg_scraped_without_emotional_charge

            # ============================================
            # BASE NEWS FILTER
            # ============================================
            # Establish baseline: Must be port-related transport news (not airport)
            .withColumn(
                "BaseNews",
                when(
                    (col("THEMES").like("%PORT%")) &
                    (col("THEMES").like("%TRANSPORT%")) &
                    (~col("THEMES").like("%AIRPORT%")),
                    1
                )
            )
            .filter("BaseNews == 1")

            # ============================================
            # THEME 1: TRANSPORT INFRASTRUCTURE
            # ============================================
            # Flag news mentioning physical infrastructure (docks, cranes, roads)
            .withColumn(
                "NewsWithTINFA",
                when(col("THEMES").like("%TRANSPORT_INFRASTRUCTURE%"), 1)
            )

            # ============================================
            # THEME 2: TRADE
            # ============================================
            # Flag news mentioning trade impact (tariffs, volume, restrictions)
            .withColumn(
                "NewsWithTRADE",
                when(col("THEMES").like("%TRADE%"), 1)
            )

            # ============================================
            # THEME 3: MACROECONOMIC
            # ============================================
            # Flag news mentioning economic impact (GDP, inflation, supply chain)
            .withColumn(
                "NewsWithME",
                when(col("THEMES").like("%MACROECONOMIC%"), 1)
            )

            # ============================================
            # THEME 4: PUBLIC SECTOR
            # ============================================
            # Flag news mentioning government policy (regulations, permits)
            .withColumn(
                "NewsWithPS",
                when(col("THEMES").like("%PUBLIC_SECTOR%"), 1)
            )

            # ============================================
            # THEME 5: MARITIME INCIDENT
            # ============================================
            # Flag news mentioning accidents (collisions, groundings, spills)
            .withColumn(
                "NewsWithMI",
                when(col("THEMES").like("%MARITIME_INCIDENT%"), 1)
            )

            # ============================================
            # FILL NULLS AND CALCULATE TOTAL THEMES
            # ============================================
            .fillna(0)  # Set missing theme flags to 0
            .withColumn(
                "Total",
                col("NewsWithTINFA") +
                col("NewsWithTRADE") +
                col("NewsWithME") +
                col("NewsWithPS") +
                col("NewsWithMI")
            )  # Sum of all 5 theme flags (0-5)

            # ============================================
            # FILTER NEGATIVE SENTIMENT ONLY
            # ============================================
            # Focus on disruptions (negative news impacts supply chains)
            .filter("AverageTone < 0")

            # ============================================
            # AGGREGATE BY DIMENSIONS
            # ============================================
            # Group by: Date, Theme Count, Location, Shipping Routes
            .groupby(
                "Date",
                "Total",  # Number of themes (1-5)
                "LocationCode",  # Port code (e.g., USCA, CH23)
                "is_ruta_transpacifica",  # Transpacific route flag
                "is_ruta_transatlantica",  # Transatlantic route flag
                "is_ruta_del_cabo_de_buena_esperanza"  # Cape of Good Hope flag
            )
            .agg(count("Total").alias("NumberOfNews"))  # Count articles per group

            # ============================================
            # APPLY EXPONENTIAL WEIGHTING
            # ============================================
            # This is the winning formula! Multiply news count by theme-based weight
            .withColumn(
                'WeightedCountOfNews',
                when(col("Total") == 5, col("NumberOfNews") * 500)  # 5 themes = 500x
                .when(col('Total') == 4, col("NumberOfNews") * 250)  # 4 themes = 250x
                .when(col("NumberOfNews") == 3, col("NumberOfNews") * 100)  # 3 themes = 100x (Note: Bug - should check Total, not NumberOfNews)
                .when(col("Total") == 2, col("NumberOfNews") * 5)  # 2 themes = 5x
                .otherwise(0)  # 1 theme = 0x (filtered out)
            )
        )

        print(f"✓ Applied exponential weighted scoring")

        # ============================================
        # STEP 5: CONFIGURE GOLD DELTA TABLE
        # ============================================
        table_name = "gold.g_gdelt_gkg_weights_report"
        delta_table_path = (
            f"s3://databricks-workspace-stack-e63e7-bucket/"
            f"unity-catalog/2600119076103476/gold/gdelt/gkg_weights_report/"
        )

        # ============================================
        # STEP 6: MERGE INTO GOLD DELTA TABLE
        # ============================================
        if DeltaTable.isDeltaTable(spark, delta_table_path):
            # ============================================
            # CASE 1: TABLE EXISTS - PERFORM MERGE
            # ============================================
            print("La tabla existe, insertando los datos")
            delta_table = DeltaTable.forPath(spark, delta_table_path)

            """
            Merge Condition:
            ----------------
            Match on composite key of aggregation dimensions:
            - DATE: Processing date
            - Total: Number of themes (1-5)
            - LocationCode: Port code
            - NumberOfNews: News count

            Note: This assumes no duplicates within same date/themes/location.
            Production should add route flags to merge condition for robustness.
            """
            delta_table.alias("tgt").merge(
                source=gkg_with_weights.alias("src"),
                condition=(
                    "tgt.DATE = src.DATE AND "
                    "tgt.Total = src.Total AND "
                    "tgt.LocationCode = src.LocationCode AND "
                    "tgt.NumberOfNews = src.NumberOfNews"
                )
            ).whenNotMatchedInsertAll().execute()

            print(f"✓ Merged data into Gold Delta table")

        else:
            # ============================================
            # CASE 2: TABLE DOESN'T EXIST - CREATE TABLE
            # ============================================
            print("La tabla no existe, creando la tabla y insertando los datos")
            gkg_with_weights.write.format("delta").mode("overwrite").save(delta_table_path)
            print("Se ha escrito en formato delta.")

            # Register table in Unity Catalog
            spark.sql(
                f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_table_path}'"
            )

            print(f"✓ Created Gold Delta table")

    return

    except requests.exceptions.RequestException as e:
        # ============================================
        # ERROR HANDLING
        # ============================================
        print(f"ERROR: Gold layer processing failed for date {date_str}")
        print(f"  Exception: {e}")
        print(f"Error al ejecutar el proceso en la fecha {date_str}: {e}")
        raise


# ============================================
# MAIN EXECUTION
# ============================================
if __name__ == "__main__":
    """
    Execute Gold layer transformation.

    This is orchestrated by Databricks Workflows as part of the Gold layer pipeline.
    """
    print("=" * 70)
    print("GOLD LAYER - BUILDING GKG WEIGHTED NEWS SUMMARY")
    print("=" * 70)

    check_rows()

    print("=" * 70)
    print("SUCCESS: Gold layer transformation complete")
    print("=" * 70)

# COMMAND ----------


"""
==============================================================================
GOLD LAYER OUTPUT
==============================================================================

Gold Delta Table: gold.g_gdelt_gkg_weights_report

Schema:
-------
- Date: Processing date (date type)
- Total: Number of disruption themes (1-5)
- LocationCode: Port code (e.g., USCA, CH23)
- is_ruta_transpacifica: Transpacific route flag
- is_ruta_transatlantica: Transatlantic route flag
- is_ruta_del_cabo_de_buena_esperanza: Cape of Good Hope route flag
- NumberOfNews: Raw article count per group
- WeightedCountOfNews: Exponentially weighted news count (CRITICAL METRIC)

Power BI Dashboard Queries:
----------------------------
SELECT Date, LocationCode, SUM(WeightedCountOfNews) as DisruptionScore
FROM gold.g_gdelt_gkg_weights_report
WHERE is_ruta_transpacifica = 1
GROUP BY Date, LocationCode
ORDER BY DisruptionScore DESC

This returns supply chain disruption risk scores by port and date.

Hackathon Notes:
----------------
1. Bug in Line 40: Checks NumberOfNews instead of Total for 3-theme case
   Should be: .when(col("Total") == 3, col("NumberOfNews") * 100)

2. Missing Imports: col, when, count, DeltaTable not imported

3. Merge Condition: Should include route flags for uniqueness
   Current condition can create duplicates if same location appears in
   multiple routes with same news count

4. Sequential Processing: Processes one date at a time
   Production should batch process multiple dates in parallel

5. No Data Quality Checks: Should validate weighted counts are reasonable
   (e.g., no values > 1 million which would indicate data quality issue)

Why This Solution Won:
-----------------------
The exponential weighting system (5 themes = 500x) was the key innovation
that separated signal from noise in supply chain disruption detection. It
mathematically amplifies multi-dimensional crisis events while filtering
out generic port news, providing actionable insights for logistics planning.

==============================================================================
"""
