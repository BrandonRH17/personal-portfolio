# Power BI Dashboards

This folder contains the interactive Power BI dashboards developed for near real-time maritime port disruption monitoring, powered by the data engineering pipelines implemented in this project.

---

## 📊 Dashboard Files

### Datathon 2024 - Neutrino Solutions.pbix
**Award-Winning Dashboard - Overall Grand Winner**

This Power BI dashboard provides comprehensive visualization and monitoring capabilities for maritime operators, consuming data from the Gold layer of our Medallion Architecture.

#### Key Features:

**1. Near Real-Time Port Status**
- Live monitoring of port conditions across the Transpacific Route
- Visual indicators for operational status (normal, warning, critical)
- Geographic visualization of monitored ports
- Data refreshed from Gold layer Delta tables

**2. Disruption Probability Forecasting**
- Predictive analytics showing disruption likelihood for upcoming days
- 2-7 day advance warning system
- Trend analysis and pattern detection
- Powered by weighted news scoring algorithm

**3. Risk Scoring Dashboard**
- Port-by-port risk percentage calculations
- Breakdown by threat type:
  - 🪧 Labor disputes (strikes, union actions)
  - ⚠️ Civil unrest (riots, protests)
  - 🌪️ Environmental hazards (climate events)
- Exponential weighting system (5 themes = 500x multiplier)

**4. News Event Tracking**
- Theme-based news categorization
- Sentiment analysis visualization
- Event timeline and history
- Filtered for emotional charge removal (neutrality + polarity paradox)

**5. Actionable Insights**
- Route optimization recommendations
- Contingency planning support
- Alert system for threshold breaches

---

## 🚀 How to Use

### Requirements:
- **Power BI Desktop** (recommended) or Power BI Service
- Access to underlying Gold layer Delta tables (if connecting live)

### Opening the Dashboard:
1. Download `Datathon 2024 - Neutrino Solutions.pbix`
2. Open with Power BI Desktop
3. Refresh data connections if needed
4. Navigate through report pages using tabs

### Dashboard Pages:
- **Overview**: Executive summary and key metrics
- **Port Status**: Real-time port monitoring
- **Risk Analysis**: Detailed risk scoring by port
- **News Insights**: Event tracking and sentiment
- **Predictions**: Forecasting and trend analysis

---

## 📈 Technical Details

**Data Pipeline Architecture:**
This dashboard consumes data from our Medallion Architecture:

```
Bronze Layer (Raw GDELT Data)
    ↓
Silver Layer (Parsed, Cleaned, Scraped)
    ↓
Gold Layer (Aggregated, Weighted Metrics) ← Power BI connects here
    ↓
Dashboard Visualizations
```

**Data Sources:**
- **Gold Layer Delta Tables**:
  - `gold.g_gdelt_gkg_weights_report` - Weighted news summaries
  - `gold.gdelt_events_aggregated` - Event-level aggregations
- **Reference Data**:
  - Port location coordinates
  - Shipping route definitions (Transpacific, Transatlantic)

**Data Transformations:**
- Emotional charge filtering (Silver layer)
- Exponential weighted scoring (Gold layer)
- Semi-structured field parsing (Silver layer)
- Web scraping and NLP summarization (Silver/Gold layers)

**Refresh Frequency:**
- Near real-time updates via Databricks Workflows
- Bronze layer: Every 15 minutes
- Silver/Gold layers: Triggered after Bronze success
- Dashboard refresh: Configurable (default 4 hours)

**Performance:**
- Optimized for large-scale data (billions of records)
- Uses DirectQuery for live data from Delta Lake
- Cached aggregations for fast rendering
- Partitioned by date for efficient queries

---

## 🏆 Recognition

This dashboard was a key component of the **Overall Grand Winner** solution at the Factored Datathon 2024, demonstrating effective integration of data engineering pipelines with business intelligence visualization.

### Data Engineering Highlights:
- ✅ Scalable Medallion Architecture processing billions of events
- ✅ Delta Lake ACID transactions for data reliability
- ✅ Automated Databricks Workflows with dependency management
- ✅ Unity Catalog governance and access control
- ✅ AWS S3 cloud storage infrastructure

---

## 📝 Notes

- Dashboard designed for 1920x1080 display resolution
- Interactive filters allow drill-down by country, port, or date range
- Export capabilities for sharing insights with stakeholders
- Mobile-optimized views available in Power BI Service
- Connects directly to Delta Lake tables via Databricks SQL endpoint

---

## 🔗 Related Documentation

- **Data Engineering Pipelines**: See [code/](../code/) folder for complete Medallion Architecture implementation
- **Pipeline Documentation**: See [code/README.md](../code/README.md) for workflow details
- **Architecture Diagrams**: See [presentations/](../presentations/) folder
- **Analytics Implementation**: See [../../data-analytics/1. GDELT Big Data Analytics/](../../data-analytics/1.%20GDELT%20Big%20Data%20Analytics/) for predictive modeling

---

*This dashboard is powered by production-ready data engineering pipelines implementing Bronze → Silver → Gold medallion architecture with Delta Lake, Databricks, and AWS S3.*
