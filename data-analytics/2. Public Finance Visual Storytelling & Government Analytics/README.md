# Public Finance Visual Storytelling & Government Analytics

**IMF Open Innovation Challenge - ChallengeGT Guatemala 2022**

🏆 **Winner: Best Storytelling and Financial Analytics**

---

## 📋 Project Overview

This project showcases advanced data analytics and visual storytelling techniques applied to government public finance data, specifically targeting fraud detection and anomaly identification in public health procurement during the COVID-19 pandemic.

Developed for the International Monetary Fund's (IMF) Open Innovation Challenge in partnership with Guatemala's Ministry of Public Finance and Ministry of Public Health and Social Assistance (MSPAS), this solution won the award for **Best Storytelling and Financial Analytics** among 10 finalist teams selected from 37 applicants.

---

## 🎯 Challenge Context (Situation)

### The Problem

**ChallengeGT: Promoting Innovation from Procurement to Payments**

Guatemala's government faced significant challenges in public financial management, particularly in the health sector during the COVID-19 pandemic:

- **Price Inflation:** Medical supply vendors artificially inflating prices during emergency procurement
- **Purchase Splitting (Fraccionamiento):** Entities deliberately splitting purchases into multiple transactions below the GTQ 50,000 transparency reporting threshold
- **Lack of Oversight:** Difficulty detecting anomalous spending patterns across multiple procurement systems
- **Data Fragmentation:** Information scattered across 4+ government systems

### Government Data Sources

The challenge provided access to multiple integrated government systems:

1. **Guatecompras** - E-procurement platform (contract data)
2. **SICOIN** - Integrated Accounting System (budget execution)
3. **SIGES** - Budget Management System (appropriations and commitments)
4. **RGAE** - Business Registry (vendor information)

### Judging Criteria

Solutions were evaluated on:
- ✅ **Feasibility and projection capabilities**
- ✅ **Creativity, originality, and innovation**
- ✅ **Design quality, presentation, and replicability**

Jury panel included IMF staff, Guatemala government officials, CAPTAC-DR, and Inter-American Development Bank members.

---

## 🎯 Objectives (Task)

### Team: University of the Isthmus Champions

Our mission was to develop a **predictive analytics platform** using historical contracting data to:

1. **Detect Anomalous Spending Patterns**
   - Identify price inflation in medical supplies vs. historical baselines
   - Compare prices across similar entities and procurement categories
   - Flag statistical outliers in purchase amounts

2. **Identify Purchase Splitting Schemes**
   - Detect multiple purchases <GTQ 50,000 within 30-day windows
   - Track cumulative spending by vendor/entity combinations
   - Pattern recognition for threshold evasion tactics

3. **Enable Predictive Decision-Making**
   - Provide historical benchmarks for future procurement
   - Calculate expected price ranges based on comparable purchases
   - Risk scoring for vendors and procurement processes

4. **Communicate Insights Through Visual Storytelling**
   - Transform complex financial data into actionable visualizations
   - Create intuitive dashboards for government decision-makers
   - Present fraud patterns in compelling, evidence-based narratives

---

## ⚙️ Methodology (Action)

### Data Analytics Approach

**1. Data Integration & Cleaning**
- Consolidated data from 4 government systems
- Standardized vendor names and product categories
- Handled missing values and data quality issues
- Created unified data model for cross-system analysis

**2. Anomaly Detection Logic**
- **Historical Comparison:** Calculated price baselines from pre-pandemic data
- **Peer Benchmarking:** Compared purchases across similar healthcare entities
- **Statistical Methods:** Z-score analysis for outlier identification
- **Temporal Analysis:** Tracked price trends over time

**3. Purchase Splitting Detection**
- **Threshold Analysis:** Flagged purchases just below GTQ 50,000
- **Temporal Clustering:** Identified multiple purchases within 30 days
- **Vendor-Entity Patterns:** Tracked suspicious recurring combinations
- **Cumulative Aggregation:** Summed split purchases to reveal true amounts

**4. Predictive Analytics**
- **Expected Price Ranges:** Based on historical data and entity type
- **Risk Scoring:** Weighted scoring for vendors and transactions
- **Trend Forecasting:** Predicted future spending patterns
- **Recommendation Engine:** Suggested optimal purchasing decisions

### Visual Storytelling Techniques

**Power BI Dashboard Development:**

**Interactive Visualizations:**
- Time-series trend analysis
- Geospatial distribution maps
- Vendor comparison matrices
- Anomaly heat maps
- Purchase pattern timelines

**Storytelling Elements:**
- Clear narrative flow (discovery → investigation → evidence)
- Color-coded risk levels (green/yellow/red)
- Drill-down capabilities for detailed investigation
- Automated insight generation
- Executive summary cards

**User Experience Design:**
- Intuitive navigation for non-technical government officials
- Filter-based exploration
- Mobile-responsive layouts
- Export capabilities for reporting

---

## 🏆 Results & Impact (Result)

### Competition Achievement

**🥇 Winner: Best Storytelling and Financial Analytics**

- **Prize:** $5,000 USD for prototype completion
- **Recognition:** Selected as one of 3 winners from 10 finalists (37 original applicants)
- **Timeline:** August 1-19, 2022 (implementation phase)

### Key Insights Discovered

**1. Price Inflation Detection**
- ✅ Identified multiple instances of medical supply prices 2-5x higher than historical averages
- ✅ Detected vendors consistently charging above peer benchmarks
- ✅ Quantified excess spending during COVID-19 emergency period

**2. Purchase Splitting Patterns**
- ✅ Flagged systematic splitting: multiple purchases of GTQ 45,000-49,999
- ✅ Identified vendors with suspicious transaction frequency (<30 days)
- ✅ Calculated hidden totals exceeding GTQ 200,000 in split purchases

**3. Entity-Level Patterns**
- ✅ Specific health entities showing consistent anomalous behavior
- ✅ Geographic clustering of irregular procurement patterns
- ✅ Repeat vendor-entity combinations with inflated prices

### Analytical Value Delivered

**Transparency Enhancement:**
- Exposed previously hidden spending patterns
- Made complex procurement data accessible to oversight bodies
- Enabled evidence-based fraud investigation

**Financial Impact:**
- Quantified potential savings from correcting price anomalies
- Identified high-risk transactions for audit prioritization
- Provided benchmarks for future procurement validation

**Institutional Capability:**
- Demonstrated feasibility of data-driven procurement oversight
- Created replicable methodology for other sectors
- Established foundation for continuous monitoring system

---

## 🛠️ Technologies & Tools

| Category | Technologies |
|----------|-------------|
| **Data Visualization** | Microsoft Power BI |
| **Data Sources** | Guatecompras, SICOIN, SIGES, RGAE |
| **Analytics Techniques** | Statistical analysis, anomaly detection, pattern recognition |
| **Storytelling** | Visual narratives, interactive dashboards, risk scoring |

---

## 📂 Project Structure

```
2. Public Finance Visual Storytelling & Government Analytics/
├── code/                    # Data cleaning and transformation scripts
├── dashboards/              # Power BI dashboard files
├── presentations/           # Challenge presentation materials
└── README.md                # This file
```

---

## 🎓 Key Learnings

### Analytical Insights

1. **Data Quality is Critical:** Government data requires extensive cleaning and standardization
2. **Context Matters:** Understanding procurement regulations (e.g., GTQ 50K threshold) is essential
3. **Historical Baselines:** Pre-pandemic data provided crucial reference points
4. **Pattern Recognition:** Automated detection outperforms manual review at scale

### Storytelling Principles

1. **Clarity Over Complexity:** Simplified visualizations for government stakeholders
2. **Evidence-Based Narrative:** Let data tell the story through clear patterns
3. **Actionable Insights:** Focus on findings that enable decisions
4. **Interactive Exploration:** Allow users to investigate their own questions

### Public Finance Domain

1. **Fraud Techniques Evolve:** Purchase splitting shows sophisticated evasion tactics
2. **Emergency Context:** COVID-19 created urgency that enabled irregularities
3. **Systemic Patterns:** Anomalies often cluster by entity and vendor
4. **Oversight Gaps:** Threshold-based reporting creates exploitable loopholes

---

## 🔗 Related Resources

- **IMF Fiscal Affairs Department:** Partner organization
- **Guatemala Ministry of Public Finance:** Government counterpart
- **Bill and Melinda Gates Foundation:** Digital Revolutions program sponsor
- **CAPTAC-DR:** Regional technical assistance center

---

## 👥 Team Contribution

As a member of **University of the Isthmus Champions**, my specific contributions included:

- Data analytics methodology design
- Anomaly detection algorithm development
- Power BI dashboard creation
- Visual storytelling and narrative development
- Presentation of findings to IMF and government officials

---

## 📊 Impact Metrics

**Challenge Statistics:**
- 🎯 10 finalist teams (from 37 applicants)
- 🏆 3 winning teams ($5,000 each)
- 🌎 International jury (IMF, IDB, Guatemala government)
- 📅 August 1-19, 2022 implementation phase

**Analytical Coverage:**
- 📦 Thousands of procurement transactions analyzed
- 💰 GTQ millions in spending reviewed
- 🏥 Multiple health entities investigated
- 🔍 Multi-year historical comparison

---

## 🚀 Future Enhancements

**Potential Extensions:**
- Real-time monitoring integration with government systems
- Machine learning for automated anomaly scoring
- Expanded coverage to additional procurement categories
- Mobile app for field auditors
- API integration for continuous data ingestion

---

*This project demonstrates the power of data analytics and visual storytelling to combat public sector fraud and improve government transparency in developing economies.*

---

**Competition:** IMF Open Innovation Challenge - ChallengeGT Guatemala 2022
**Team:** University of the Isthmus Champions
**Award:** 🏆 Best Storytelling and Financial Analytics
**Prize:** $5,000 USD
**Partner Organizations:** International Monetary Fund, Guatemala Ministry of Public Finance, Bill and Melinda Gates Foundation
