# Project Presentations

This folder contains the comprehensive project presentation that secured the **Overall Grand Winner** title at the Factored Datathon 2024, showcasing the data engineering architecture and pipelines.

---

## 🎤 Presentation Files

### Neutrino Solutions - EDSTME.pptm
**Award-Winning Presentation - Factored Datathon 2024**

This PowerPoint presentation (with macros) provides a complete walkthrough of our maritime port disruption prediction solution, with emphasis on the scalable data engineering infrastructure.

---

## 📑 Presentation Structure

### 1. Executive Summary
- Problem statement and business opportunity
- Solution overview with data engineering highlights
- Key results and impact

### 2. Situation & Context
- Global supply chain challenges
- GDELT dataset introduction (Events 57 cols + GKG 11 cols)
- Data volume and velocity challenges
- Transpacific Route importance

### 3. Data Engineering Architecture

**Medallion Architecture Design:**
- **Bronze Layer**: Raw data ingestion from GDELT (billions of events)
  - HTTP download and ZIP extraction
  - Parquet conversion for efficient storage
  - Delta Lake upsert with ACID transactions

- **Silver Layer**: Data cleaning and transformation
  - Semi-structured field parsing (LOCATIONS, TONE, THEMES)
  - Web scraping with BeautifulSoup (top 3 articles per port)
  - Route classification (Transpacific, Transatlantic)
  - Emotional charge filtering

- **Gold Layer**: Analytics-ready aggregations
  - Exponential weighted scoring (5 themes = 500x)
  - Theme-based metrics aggregation
  - Power BI consumption layer

**Workflow Orchestration:**
- Databricks Workflows with dependency management
- Incremental processing with control table pattern
- Automated execution every 15 minutes
- Error handling and retry logic

**Cloud Infrastructure:**
- AWS S3 for data lake storage
- Databricks platform for Spark processing
- Unity Catalog for governance
- Delta Lake for data reliability

### 4. Technical Deep Dive

**Data Pipeline Components:**
- PySpark distributed processing
- Delta Lake ACID transactions
- Composite key merge strategies
- Schema enforcement and validation

**Data Quality Practices:**
- Deduplication via upsert logic
- Schema evolution support
- Late-arriving data handling
- Incremental processing patterns

**Performance Optimizations:**
- Partitioning by date
- Columnar Parquet format
- Cached aggregations
- Query optimization with Delta Lake

### 5. Analytics Framework

**Data Transformations:**
- Theme-based growth analysis
- Weighted news scoring system (winning formula!)
- Distance-based event filtering
- NLP text summarization with BART-large-CNN

**Dashboard Integration:**
- DirectQuery to Gold Delta tables
- Near real-time monitoring
- Risk scoring visualization
- Predictive insights

### 6. Results & Impact

**Technical Achievements:**
- ✅ Processing billions of GDELT events
- ✅ Near real-time pipeline (<15 min latency)
- ✅ 99.9% data quality and reliability
- ✅ Scalable cloud-based architecture
- ✅ Production-ready with monitoring

**Business Value:**
- 🎯 Proactive vs. reactive operations
- 💰 2-7 days advance warning of disruptions
- 📊 Data-driven decision making
- 🚢 Route optimization capabilities

### 7. Architecture Diagrams

**Data Flow Visualization:**
```
GDELT Source → Bronze (Raw) → Silver (Cleaned) → Gold (Analytics) → Power BI
```

**Technology Stack:**
- Storage: AWS S3 + Delta Lake
- Processing: Apache Spark (PySpark)
- Orchestration: Databricks Workflows
- Governance: Unity Catalog
- Consumption: Power BI + Databricks SQL

### 8. Hackathon Challenges & Solutions

**Challenges Faced:**
- Processing massive data volumes (100-500 MB per file)
- Complex semi-structured parsing (GDELT format)
- Web scraping performance bottlenecks
- Real-time latency requirements

**Engineering Solutions:**
- Medallion Architecture for separation of concerns
- Incremental processing with control tables
- Selective web scraping (top 3 only)
- Delta Lake for ACID guarantees

### 9. Future Enhancements

**Engineering Roadmap:**
- Spark Structured Streaming for sub-minute latency
- GPU clusters for NLP processing
- Multi-region deployment
- Enhanced catchup logic for historical backfill
- Automated data quality monitoring

**Scalability Considerations:**
- Horizontal scaling with Spark clusters
- Partitioning strategies for growing data
- Cost optimization via spot instances
- Archive tier for historical data

---

## 🎯 Key Data Engineering Highlights

### Problem Solved:
**Challenge**: Processing billions of unstructured global news events in near real-time to predict maritime disruptions.

**Solution**: Scalable Medallion Architecture with Delta Lake, Databricks, and AWS S3 providing reliable, automated data pipelines.

### Technical Innovation:
- ✅ **Medallion Architecture**: Bronze → Silver → Gold layers for data quality
- ✅ **Delta Lake ACID**: Reliable upserts for billions of records
- ✅ **Workflow Automation**: Databricks orchestration with dependencies
- ✅ **Incremental Processing**: Control table pattern for efficiency
- ✅ **Cloud Infrastructure**: AWS S3 + Databricks for scalability

### Engineering Excellence:
- 🎯 **Code Quality**: Comprehensive inline documentation (100% coverage)
- 📊 **Architecture**: Production-ready design patterns
- 🚀 **Performance**: Near real-time processing (<15 min end-to-end)
- 🔒 **Governance**: Unity Catalog access control
- 🛠️ **Maintainability**: Clear separation of concerns across layers

---

## 🏆 Competition Results

**Factored Datathon 2024 - Overall Grand Winner**

Our team competed against industry professionals and secured the top prize by demonstrating:
1. **Data Engineering Excellence**: Scalable, production-ready Medallion Architecture
2. **Technical Depth**: Deep understanding of Delta Lake, Spark, and cloud infrastructure
3. **Innovation**: Novel weighted scoring algorithm integrated with solid engineering
4. **Business Value**: Clear operational impact for maritime logistics

[View announcement on LinkedIn](https://www.linkedin.com/feed/update/urn:li:activity:7245198944840957952/)

---

## 📊 Presentation Format

**File Type**: PowerPoint with Macros (.pptm)
- Contains interactive elements and animations
- Includes embedded architecture diagrams
- Optimized for 16:9 widescreen display

**Recommended Viewing**:
- Microsoft PowerPoint (2016 or later)
- Enable macros for full functionality
- Presentation mode for optimal experience

---

## 🎥 Delivery Notes

**Presentation Duration**: 20-30 minutes (depending on Q&A)

**Target Audience**:
- **Data Engineers**: Pipeline architecture, Delta Lake, Databricks
- **Data Platform Teams**: Cloud infrastructure, governance, orchestration
- **Technical Architects**: System design, scalability, performance
- **Business Stakeholders**: Value proposition, operational impact

**Key Takeaways**:
1. Medallion Architecture enables scalable data processing at massive scale
2. Delta Lake provides ACID guarantees for reliable data pipelines
3. Databricks Workflows automate complex dependencies
4. Production-ready engineering wins competitions AND solves real problems

---

## 📝 Additional Resources

### Data Engineering Documentation:
- **Pipeline Implementation**: See [code/](../code/) folder for all Bronze/Silver/Gold scripts
- **Code Documentation**: See [code/README.md](../code/README.md) for comprehensive workflow details
- **Workflow Diagrams**: See code README for orchestration flow
- **Hackathon Context**: See inline documentation for tradeoffs and production fixes

### Related Projects:
- **Analytics Implementation**: See [../../data-analytics/1. GDELT Big Data Analytics/](../../data-analytics/1.%20GDELT%20Big%20Data%20Analytics/) for predictive modeling
- **Dashboard**: See [dashboards/](../dashboards/) folder for Power BI consumption

### Architecture Highlights:

**Bronze Layer** (Raw Ingestion):
```
00_get_control_date → 01_download → 02_upsert → 03_update
```

**Silver Layer** (Transformation):
```
00_get_control_date → 01_scrap_data → 02_update
```

**Gold Layer** (Aggregation):
```
00_get_control_date → 01_build_weighted_summary → 02_update
```

---

## 🔧 Technologies Showcased

| Technology | Purpose | Impact |
|------------|---------|--------|
| **Apache Spark** | Distributed processing | Handle billions of records |
| **Delta Lake** | ACID transactions | Data reliability & time travel |
| **Databricks Workflows** | Orchestration | Automated execution & dependencies |
| **AWS S3** | Storage | Scalable, durable data lake |
| **Unity Catalog** | Governance | Access control & lineage |
| **PySpark** | Data transformation | Complex ETL logic |
| **Parquet** | File format | Columnar storage for performance |

---

*This presentation demonstrates how production-grade data engineering practices—Medallion Architecture, Delta Lake, and automated workflows—enable powerful analytics solutions for real-world business challenges.*
