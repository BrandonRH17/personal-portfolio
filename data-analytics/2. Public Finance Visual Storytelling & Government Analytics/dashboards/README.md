# Power BI Dashboard & Architecture Diagrams

This folder contains the award-winning Power BI dashboard and high-level architecture diagrams developed for the IMF Open Innovation Challenge - ChallengeGT Guatemala 2022.

---

## 📊 Dashboard Files

### DASH.pbix (13.1 MB)

**Award-Winning Dashboard - Best Storytelling and Financial Analytics**

This Power BI dashboard provides comprehensive fraud detection and anomaly analysis capabilities for government procurement oversight, specifically designed for non-technical government stakeholders.

#### Key Features:

**1. Anomaly Detection Visualizations**
- Historical price comparison analysis
- Statistical outlier identification
- Vendor benchmarking matrices
- Entity-level spending patterns

**2. Purchase Splitting Detection**
- GTQ 50,000 threshold analysis
- Temporal clustering (30-day windows)
- Cumulative purchase aggregation
- Suspicious pattern flagging

**3. Risk Scoring Dashboard**
- Automated risk assessment
- Color-coded severity levels (green/yellow/red)
- Vendor risk profiles
- Transaction-level alerts

**4. Interactive Storytelling**
- Narrative flow design
- Drill-down investigation capabilities
- Time-series trend analysis
- Geospatial distribution maps

---

## 📐 Architecture Diagram

### MINFIN Diagram.drawio.pdf (37 KB)

**High-Level System Architecture**

This simplified diagram illustrates the data flow and integration architecture for the fraud detection solution.

#### Design Approach:

**Audience-Driven Simplicity:**
Given that the presentation audience consisted primarily of **non-technical government officials** (Guatemala Ministry of Public Finance, Ministry of Public Health, IMF fiscal policy experts), the diagram intentionally focuses on:

- ✅ **Conceptual clarity** over technical detail
- ✅ **Data flow** between government systems
- ✅ **Business logic** rather than implementation specifics
- ✅ **Stakeholder-friendly** visual communication

**Why High-Level?**

The diagram was designed to communicate:
1. **Data Sources:** Which government systems provide input (Guatecompras, SICOIN, SIGES, RGAE)
2. **Integration Points:** How data flows between systems
3. **Analytics Process:** Transformation from raw data to insights
4. **Decision Support:** How insights reach government decision-makers

**Technical Details Were Delivered Separately:**
- Detailed data models shared with technical government staff
- Implementation specifications in written documentation
- Code logic explained in technical appendix (not in presentation)

---

## 🚀 How to Use the Dashboard

### Requirements:
- **Power BI Desktop** (recommended) or Power BI Service
- Dataset embedded in .pbix file (no live connection required for demo)

### Opening the Dashboard:
1. Download `DASH.pbix`
2. Open with Power BI Desktop
3. Navigate through report pages using tabs
4. Use interactive filters for detailed investigation

### Dashboard Pages:
- **Executive Summary**: Key metrics and high-level insights
- **Anomaly Detection**: Price inflation and outlier analysis
- **Purchase Splitting**: Threshold evasion pattern detection
- **Vendor Analysis**: Supplier risk scoring and comparison
- **Temporal Trends**: Time-series analysis and forecasting
- **Entity Overview**: Healthcare institution-level breakdown

---

## 📈 Technical Approach

### Data Sources Integrated:
- **Guatecompras** - E-procurement platform (contract data)
- **SICOIN** - Integrated Accounting System (budget execution)
- **SIGES** - Budget Management System (appropriations and commitments)
- **RGAE** - Business Registry (vendor information)

### Analytical Techniques:
- **Z-score outlier detection** for price anomalies
- **Historical baseline comparison** (pre-pandemic vs. pandemic periods)
- **Temporal clustering** for purchase splitting detection
- **Risk scoring algorithms** for vendor assessment

### Visualization Design:
- **Time-series line charts** for trend analysis
- **Heat maps** for pattern detection
- **Scatter plots** for price comparison
- **Geographic maps** for entity distribution
- **Card visuals** for KPIs
- **Interactive slicers** for date/entity/vendor filtering

---

## 🏆 Recognition

This dashboard was the centerpiece of the **Best Storytelling and Financial Analytics** winning solution at the IMF Open Innovation Challenge, demonstrating:

- ✅ **Clear communication** of complex financial data to non-technical audiences
- ✅ **Actionable insights** for government decision-makers
- ✅ **Innovative fraud detection** methodologies
- ✅ **User-friendly design** optimized for government stakeholders
- ✅ **Visual storytelling** that won the storytelling award

---

## 📝 Design Philosophy

**Storytelling Principles Applied:**

1. **Progressive Disclosure:**
   - Start with executive summary
   - Allow drill-down for details
   - Respect varied technical expertise

2. **Evidence-Based Narrative:**
   - Visual proof of fraud patterns
   - Comparative analysis (before/after)
   - Quantified impact metrics

3. **Action-Oriented:**
   - Flag specific transactions for investigation
   - Recommend audit priorities
   - Enable immediate follow-up

4. **Accessibility:**
   - Designed for 1920x1080 resolution
   - Color-blind friendly palette
   - Spanish language labels
   - Minimal technical jargon

---

## 🎯 Impact

**Government Adoption:**
- Dashboard format accepted by Guatemala Ministry of Public Finance
- Methodology recognized as replicable for other sectors
- Visual approach praised for accessibility to non-technical officials

**Institutional Value:**
- Enhanced transparency in public procurement
- Evidence-based fraud investigation capability
- Capacity building for government analytics teams

---

For complete project context and results, see the main [project README](../README.md).
