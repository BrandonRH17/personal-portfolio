# Data Engineering Code Documentation

This folder contains the complete data engineering pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) for GDELT data processing.

---

## 📂 Folder Structure

```
code/
├── 1. bronze/                    # Raw data ingestion layer
│   ├── workflows/
│   │   ├── events/              # GDELT Events ingestion
│   │   └── gkg/                 # GDELT GKG ingestion
│   └── testing/                 # Development notebooks
├── 2. silver/                    # Cleaned data layer
│   ├── workflow/
│   │   └── gkg/                 # GKG data scraping & cleaning
│   └── testing/                 # Development notebooks
├── 3. gold/                      # Analytics-ready layer
│   ├── workflow/
│   │   └── gkg/                 # Aggregated news summaries
│   └── testing/                 # Development notebooks
└── readme-de.md                  # Original engineering notes
```

---

## 🔷 1. Bronze Layer: Raw Data Ingestion

**Purpose**: Ingest raw GDELT data with minimal transformation

### Events Workflow (4 steps)

#### 00_get_events_control_date.py
- **Purpose**: Retrieve last processed date from TABLE_CONTROL
- **Output**: Sets task values for next date to process
- **Key Logic**: Calculates `last_date + 1 day` for incremental loading

#### 01_download_and_unzip.py.py ✅ FULLY DOCUMENTED
- **Purpose**: Download GDELT Events ZIP, extract CSV, convert to Parquet
- **Process**: HTTP download → In-memory extraction → Pandas DataFrame → Parquet → S3 upload
- **Schema**: 57 columns (Events 1.0 format)
- **Output**: Parquet file in S3 (`s3://bucket/events/gdelt/YYYYMMDD.parquet`)

#### 02_upsert_delta_table.py.py ✅ FULLY DOCUMENTED
- **Purpose**: Read Parquet from S3 and upsert into Delta Lake table
- **Process**: Read from S3 → Add metadata columns → Delta merge (upsert)
- **Merge Logic**: Match on `GlobalEventID` (primary key)
  - If exists: UPDATE all columns
  - If new: INSERT new record
- **Benefits**: ACID transactions, deduplication, schema enforcement

#### 03_update_events_control_date.py ✅ FULLY DOCUMENTED
- **Purpose**: Update TABLE_CONTROL with successfully processed date
- **Impact**: Enables incremental processing for next run
- **Query**: `UPDATE TABLE_CONTROL SET LAST_UPDATE_DATE = '{date}' WHERE TABLE_NAME = 'gdelt_events'`

### GKG Workflow (3 steps)

#### 01_download_and_unzip_gkg.py.py
- **Purpose**: Download GDELT GKG (Global Knowledge Graph) data
- **Schema**: 11 columns including THEMES, LOCATIONS, TONE, etc.
- **Key Difference vs Events**: Much larger files (100-500 MB compressed)
- **Process**: Same as events workflow but with GKG-specific schema

#### 02_upsert_delta_table_gkg.py.py
- **Purpose**: Upsert GKG data into Delta table
- **Schema Highlights**:
  - `DATE`: Event date (primary key component)
  - `THEMES`: Semicolon-delimited theme codes
  - `LOCATIONS`: Location information with coordinates
  - `TONE`: Sentiment metrics (tone, polarity, positive/negative scores)
- **Merge Logic**: Complex merge condition (may use composite key)

#### 03_update_gkg_control_date.py
- **Purpose**: Update control table for GKG processing
- **Table**: Updates `gdelt_gkg` record in TABLE_CONTROL

### Testing Notebooks

#### gdelt-events-ingestion.py
- **Purpose**: Development notebook for initial events pipeline testing
- **Use**: Historical data backfill and pipeline validation

#### gdelt-events-ingestion-daily.py
- **Purpose**: Daily incremental ingestion testing
- **Use**: Validate incremental loading logic before productionization

#### gdelt-gkg-ingestion.py
- **Purpose**: Development notebook for GKG pipeline testing
- **Use**: Schema validation and transformation testing

---

## 🔷 2. Silver Layer: Data Cleaning & Transformation

**Purpose**: Clean, filter, and augment raw data for analytics

### GKG Workflow (3 steps)

#### 00_get_gkg_control_date.py
- **Purpose**: Retrieve last processed date for Silver GKG processing
- **Source**: Queries TABLE_CONTROL for silver layer processing date
- **Note**: Separate control from Bronze to allow independent processing

#### 01_scrap_data.py.py
- **Purpose**: Parse and extract structured data from semi-structured GKG fields
- **Key Transformations**:
  - **THEMES**: Split semicolon-delimited string into array
  - **LOCATIONS**: Parse complex location string (type#name#country#coords)
  - **TONE**: Split comma-delimited tone metrics
  - **Data Quality**: Remove emotionally-charged news (neutral tone + high polarity)
- **Output**: Cleaned Silver GKG table

#### 03_update_gkg_control_date.py
- **Purpose**: Update Silver layer control date
- **Table**: Updates silver-specific control record

### Testing Notebook

#### gdelt_gkg_scraping.py
- **Purpose**: Development testing for GKG scraping logic
- **Use**: Validate parsing logic and data quality filters

---

## 🔷 3. Gold Layer: Analytics-Ready Aggregations

**Purpose**: Create business-ready aggregated tables for dashboards and ML

### GKG Workflow (3 steps)

#### 00_get_gkg_control_date.py
- **Purpose**: Retrieve last processed date for Gold layer
- **Source**: Queries TABLE_CONTROL for gold layer processing date

#### 01_build_gdelt_gkg_weighted_news_summary.py.py
- **Purpose**: Create aggregated news summaries with weighted scores
- **Key Aggregations**:
  - **Weighted News Count**: Theme-based weighting (more themes = higher weight)
  - **Port-Level Metrics**: Aggregate by port location and date
  - **Risk Scores**: Calculate disruption risk percentages
- **Output**: Gold table consumed by Power BI dashboards

#### 03_update_gkg_control_date.py
- **Purpose**: Update Gold layer control date
- **Table**: Updates gold-specific control record

### Testing Notebooks

#### Summary News Workflow.py
- **Purpose**: End-to-end testing of news summarization pipeline
- **Use**: Validate aggregation logic and output format

#### gdelt_gkg_weighted_news_summary.py
- **Purpose**: Development testing for weighted scoring algorithm
- **Use**: Tune weighting parameters and validate calculations

---

## 🔄 Workflow Orchestration

### Databricks Workflows Configuration

**Bronze Layer** (Runs every 15 minutes):
```
00_get_control_date
        ↓
    (parallel)
    ↙        ↘
Events       GKG
01_download  01_download
    ↓            ↓
02_upsert    02_upsert
    ↓            ↓
03_update    03_update
```

**Silver Layer** (Triggered after Bronze success):
```
00_get_control_date
        ↓
01_scrap_data
        ↓
03_update_control_date
```

**Gold Layer** (Triggered after Silver success):
```
00_get_control_date
        ↓
01_build_weighted_summary
        ↓
03_update_control_date
```

### Dependency Chain
```
Bronze → Silver → Gold → Power BI / ML Models
```

---

## 🛠️ Key Technologies

| Technology | Purpose | Why We Use It |
|------------|---------|---------------|
| **PySpark** | Distributed processing | Handle billions of records |
| **Delta Lake** | ACID transactions | Data reliability & time travel |
| **Delta Live Tables** | Data quality | Schema enforcement & lineage |
| **Databricks Workflows** | Orchestration | Automated execution & dependencies |
| **AWS S3** | Storage | Scalable, durable object storage |
| **Unity Catalog** | Governance | Access control & data lineage |
| **Parquet** | File format | Columnar storage for query performance |

---

## 📊 Data Schemas

### GDELT Events (57 columns)
- **Event Identification**: GlobalEventID (PK), dates
- **Actors**: Actor1/Actor2 information (30 columns)
- **Event**: CAMEO codes, Goldstein scale, mentions, tone
- **Geography**: Actor1/Actor2/Action locations with coordinates

### GDELT GKG (11 columns)
- **DATE**: Event date
- **THEMES**: Semicolon-delimited theme codes
- **LOCATIONS**: Complex location strings
- **TONE**: Comma-delimited sentiment metrics
- **PERSONS/ORGANIZATIONS**: Entity mentions
- **SOURCES**: Source URLs

---

## 🔍 Data Quality Practices

### Schema Enforcement
- Explicit schema definitions for all tables
- Type validation at each layer
- Nullable constraints where appropriate

### Deduplication
- Primary key enforcement (GlobalEventID for events)
- Upsert logic prevents duplicates
- Late-arriving data handled gracefully

### Incremental Processing
- Control table pattern for tracking progress
- Only process new data each run
- Idempotent operations (safe to re-run)

### Error Handling
- Try-catch blocks with detailed logging
- Workflow failure notifications via email
- Automatic retry logic in Databricks

---

## 🚀 Running the Pipeline

### Prerequisites:
- Databricks workspace with Delta Lake enabled
- AWS S3 bucket for data lake storage
- Unity Catalog configured
- Databricks Workflows orchestration

### Environment Variables (Databricks Widgets):
```python
aws_access_key          # AWS access key
aws_secret_access_key   # AWS secret key
aws_delta_table_path    # Delta table location in S3
aws_raw_path            # Raw Parquet files location
```

### Manual Execution:
1. Run Bronze layer workflows (events + gkg)
2. Verify TABLE_CONTROL updated
3. Run Silver layer workflow
4. Run Gold layer workflow
5. Validate output tables

### Automated Execution:
- Databricks Workflows handle orchestration
- Runs every 15 minutes for near real-time processing
- Automatic dependency management
- Email notifications on success/failure

---

## 📈 Performance Metrics

- **Bronze Layer**: ~2-5 minutes per day of data
- **Silver Layer**: ~3-7 minutes (parsing overhead)
- **Gold Layer**: ~1-2 minutes (aggregations)
- **Total Pipeline**: ~6-14 minutes end-to-end
- **Data Volume**: 200-500 MB compressed → 2-5 GB uncompressed per day

---

## 🔧 Troubleshooting

### Common Issues:

**"Control date not found"**
- Solution: Initialize TABLE_CONTROL with base dates

**"S3 access denied"**
- Solution: Verify AWS credentials in widgets

**"Delta table not found"**
- Solution: Run initial table creation or verify path

**"Merge conflict"**
- Solution: Check for schema mismatches

---

## 📝 Additional Resources

- [GDELT Project Documentation](https://www.gdeltproject.org/data.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks Workflows Guide](https://docs.databricks.com/workflows/index.html)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

---

*This pipeline was developed as part of the Overall Grand Winner solution at Factored Datathon 2024.*
