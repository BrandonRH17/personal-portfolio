# GDELT Big Data Analytics
### Maritime Port Disruption Prediction

---

## 📋 Project Overview (STAR Format)

### 🎯 Situation

This project leveraged the publicly available [GDELT (Global Database of Events, Language, and Tone)](https://www.gdeltproject.org/) dataset to address critical global challenges. GDELT monitors world news media from nearly every corner of every country in print, broadcast, and web formats, making it an invaluable resource for political and social solutions that can support the world.

For this initiative, I focused on using GDELT data to **predict potential disruptions in maritime ports before they occurred**, enabling proactive decision-making for maritime operations.

### 📝 Task

The challenge required a comprehensive approach spanning multiple disciplines:

- **Data Engineering**: Design and implement scalable data pipelines to process massive volumes of GDELT data
- **Machine Learning**: Develop predictive models to identify patterns indicating potential port disruptions
- **Data Analytics**: Create consolidated dashboards that maritime operators could easily interpret and act upon

### ⚙️ Action

**Technologies & Architecture:**

- **Databricks**: Cloud-based unified analytics platform for data processing and ML
- **Databricks Workflows with Medallion Architecture**:
  - Bronze layer: Raw GDELT data ingestion
  - Silver layer: Cleaned and transformed data
  - Gold layer: Analytics-ready aggregated datasets
- **GitHub**: Version control and collaborative development
- **Spark SQL**: Large-scale data extraction, transformation, and analysis
- **Power BI**: Interactive dashboard development for maritime operators

**Key Analysis Components:**

1. **Country Exploration**: Analysis of negative news patterns corresponding to port activities using growth percentages and weighted averages
2. **Goldenstein Scale Analysis**: Concrete analysis of how well this metric fits the objective of predicting disruptions
3. **Port Event Identification**: Distance-based parameters to identify port-related events and news

### 🎯 Result

🏆 **Overall Grand Winner - Factored Datathon 2024**

Our solution was recognized as the **Overall Grand Winner** at the [Factored Datathon 2024](https://www.linkedin.com/feed/update/urn:li:activity:7245198944840957952/), competing against teams from across the industry.

**Key Achievements:**

✅ **Near Real-Time Monitoring Dashboard**
- Developed a robust Power BI dashboard enabling maritime operators to visualize port status in near real-time
- Provided predictive insights showing disruption probability for upcoming days
- See the complete dashboard implementation in the [dashboards/](#) section

✅ **Risk Scoring System**
- Implemented a sophisticated risk percentage calculation for each monitored port
- Successfully identified and quantified threats including:
  - **Labor disputes**: Strike threats and union actions
  - **Civil unrest**: Riots and protests near port facilities
  - **Environmental hazards**: Climate events affecting operations

✅ **Proactive Decision-Making**
- Enabled logistics operators to take preventive actions 2-7 days before potential disruptions
- Reduced operational uncertainty through data-driven risk assessment
- Provided actionable insights for route optimization and contingency planning

**Impact:**
The solution transformed reactive crisis management into proactive risk mitigation, allowing maritime operators to make informed decisions based on predictive analytics rather than responding to disruptions after they occur.

📊 **View Full Presentation**: See the complete project presentation and methodology in the [presentations/](#) section

---

## 📂 Project Structure

```
├── code/
│   ├── README.md                      # Code documentation and workflow guide
│   ├── Country Exploration.ipynb      # Theme-based sentiment analysis
│   └── Ports Exploration.ipynb        # Geospatial event filtering
├── presentations/                      # Project presentations and methodology
└── dashboards/                         # Power BI dashboards and visualizations
```

---

## 🔗 Resources

- [GDELT Project](https://www.gdeltproject.org/)
- [GDELT Documentation](https://www.gdeltproject.org/data.html)
- [Original Project Repository](https://github.com/BrandonRH17/factored-datathon-2024-Neutrino-Solutions)

---

