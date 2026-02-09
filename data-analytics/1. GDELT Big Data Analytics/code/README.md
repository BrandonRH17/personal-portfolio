# Code Documentation

This folder contains the core analytical notebooks used for GDELT data exploration and maritime port disruption analysis.

---

## 📓 Notebooks Overview

### 1. Country Exploration.ipynb
**Purpose**: Comprehensive theme-based sentiment analysis for predicting port disruptions

**What It Does:**
- Analyzes GDELT Global Knowledge Graph (GKG) data for news patterns
- Identifies port-related news through theme extraction (infrastructure, trade, incidents, etc.)
- Filters emotionally-charged content to ensure objective analysis
- Implements two analytical approaches:
  - **Approach 1**: Time-series growth analysis of negative news by theme
  - **Approach 2**: Weighted scoring system emphasizing multi-theme news

**Key Techniques:**
- Data quality validation and cleaning
- Sentiment analysis (tone and polarity filtering)
- Theme identification and categorization
- Time-series pattern detection
- Window functions for growth analysis

**Output:**
- Daily news counts by location and theme combination
- Growth metrics for identifying anomalous spikes
- Weighted scores prioritizing complex, multi-factor events

**Use Case:**
Maritime operators can monitor news trends to predict disruptions 2-7 days before they occur, based on patterns in negative news coverage.

---

### 2. Ports Exploration.ipynb
**Purpose**: Geospatial analysis to identify GDELT events occurring near ports

**What It Does:**
- Calculates geodesic distances between GDELT events and port locations
- Filters events by type (conflict, protest, violence - CAMEO codes 14-22)
- Determines optimal distance thresholds for "port-related" classification
- Validates methodology using Vancouver case study (June-July 2023 labor disputes)

**Key Techniques:**
- Geopy geodesic distance calculation (WGS-84 ellipsoidal model)
- User-Defined Functions (UDFs) for distributed geospatial processing
- Distance parameter tuning and validation
- Event type filtering based on CAMEO event codes

**Output:**
- Distance calculations for all events to target ports
- Filtered event lists within specified radius (25 km threshold)
- Distribution analysis for parameter validation

**Use Case:**
Establishes distance-based filters used throughout the data engineering and ML pipelines to identify which GDELT events are relevant to specific ports.

---

## 🔄 How They Work Together

```
┌─────────────────────────────────────────────────────────────────┐
│                   GDELT Data Processing Flow                     │
└─────────────────────────────────────────────────────────────────┘

    ┌──────────────────────┐
    │  Raw GDELT Data      │
    │  (Bronze Layer)      │
    └──────────┬───────────┘
               │
               ├────────────────────────────────────────────┐
               │                                            │
               ▼                                            ▼
    ┌──────────────────────┐                  ┌──────────────────────┐
    │ Ports Exploration    │                  │ Country Exploration  │
    │                      │                  │                      │
    │ • Distance calc      │                  │ • Theme extraction   │
    │ • Event filtering    │                  │ • Sentiment analysis │
    │ • Proximity rules    │                  │ • Pattern detection  │
    └──────────┬───────────┘                  └──────────┬───────────┘
               │                                            │
               │  Geographic                    News        │
               │  Filters                       Patterns    │
               │                                            │
               └────────────────┬───────────────────────────┘
                                │
                                ▼
                  ┌──────────────────────────────┐
                  │   ML Model Training          │
                  │   (Disruption Prediction)    │
                  └──────────────────────────────┘
                                │
                                ▼
                  ┌──────────────────────────────┐
                  │   Power BI Dashboard         │
                  │   (Operational Monitoring)   │
                  └──────────────────────────────┘
```

### Workflow:

1. **Ports Exploration** establishes which events are geographically relevant to each port
2. **Country Exploration** analyzes news themes and sentiment for those relevant events
3. **Combined insights** feed into ML models for disruption prediction
4. **Predictions** are visualized in Power BI for operational decision-making

---

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|---------|
| **PySpark** | Distributed data processing at scale |
| **Spark SQL** | SQL-based data transformation and filtering |
| **Geopy** | Accurate geospatial distance calculations |
| **Window Functions** | Time-series analysis and growth metrics |
| **GDELT GKG** | Global news metadata with themes and sentiment |
| **GDELT Events** | Structured event data with CAMEO codes |

---

## 📊 Data Quality Practices

Both notebooks demonstrate professional data quality practices:

- ✅ **Null handling**: Filter missing values before processing
- ✅ **Standardization**: Convert all data to consistent formats
- ✅ **Validation**: Flag suspicious values (e.g., 999.999 coordinates)
- ✅ **Bias removal**: Filter emotionally-charged content
- ✅ **Completeness**: Use cross-joins to ensure full date coverage
- ✅ **Documentation**: Every transformation is explained and justified

---

## 🚀 Running the Notebooks

### Requirements:
- Databricks environment with PySpark
- Access to Bronze layer tables:
  - `BRONZE.GDELT_EVENTS`
  - `BRONZE.GDELT_GKG`
  - `BRONZE.PORTS_DICTIONARY`
  - `BRONZE.CAMEO_DICTIONARY`

### Execution Order:
1. **Ports Exploration** can run independently (establishes distance parameters)
2. **Country Exploration** can run independently (analyzes news patterns)
3. Both outputs are combined in downstream data science/engineering processes

### Expected Runtime:
- **Ports Exploration**: ~5-10 minutes (fewer records, focused filter)
- **Country Exploration**: ~15-30 minutes (larger dataset, complex transformations)

*Runtime varies based on cluster size and data volume*

---

## 📖 Additional Resources

- [GDELT Project Documentation](https://www.gdeltproject.org/data.html)
- [CAMEO Event Codes Reference](http://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf)
- [Geopy Documentation](https://geopy.readthedocs.io/)
- [PySpark SQL Functions Guide](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)

---
