# Power BI Dashboards

This folder contains the interactive Power BI dashboards developed for near real-time maritime port disruption monitoring.

---

## 📊 Dashboard Files

### Datathon 2024 - Neutrino Solutions.pbix
**Award-Winning Dashboard - Overall Grand Winner**

This Power BI dashboard provides comprehensive visualization and monitoring capabilities for maritime operators.

#### Key Features:

**1. Near Real-Time Port Status**
- Live monitoring of port conditions across the Transpacific Route
- Visual indicators for operational status (normal, warning, critical)
- Geographic visualization of monitored ports

**2. Disruption Probability Forecasting**
- Predictive analytics showing disruption likelihood for upcoming days
- 2-7 day advance warning system
- Trend analysis and pattern detection

**3. Risk Scoring Dashboard**
- Port-by-port risk percentage calculations
- Breakdown by threat type:
  - 🪧 Labor disputes (strikes, union actions)
  - ⚠️ Civil unrest (riots, protests)
  - 🌪️ Environmental hazards (climate events)

**4. News Event Tracking**
- Theme-based news categorization
- Sentiment analysis visualization
- Event timeline and history

**5. Actionable Insights**
- Route optimization recommendations
- Contingency planning support
- Alert system for threshold breaches

---

## 🚀 How to Use

### Requirements:
- **Power BI Desktop** (recommended) or Power BI Service
- Access to underlying data sources (if connecting live)

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

**Data Sources:**
- GDELT Events (processed through Databricks)
- GDELT GKG (Global Knowledge Graph)
- Port location reference data
- Historical disruption records

**Refresh Frequency:**
- Near real-time updates (configurable)
- Default: Every 4 hours

**Performance:**
- Optimized for large-scale data
- Uses DirectQuery for live data
- Cached aggregations for fast rendering

---

## 🏆 Recognition

This dashboard was a key component of the **Overall Grand Winner** solution at the Factored Datathon 2024, demonstrating effective data visualization for complex predictive analytics.

---

## 📝 Notes

- Dashboard designed for 1920x1080 display resolution
- Interactive filters allow drill-down by country, port, or date range
- Export capabilities for sharing insights with stakeholders
- Mobile-optimized views available in Power BI Service

---

For technical details on the underlying analytics, see the [code/](../code/) folder documentation.
