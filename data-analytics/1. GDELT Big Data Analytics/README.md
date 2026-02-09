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

*[Results section to be added]*

---

## 📂 Project Structure

```
├── code/
│   ├── Country Exploration.ipynb      # Exploration of GDELT patterns and insights
│   ├── Country Exploration - BR.py    # Goldenstein Scale analysis
│   └── Ports Exploration.py           # Port event identification analysis
├── presentations/                      # [To be added]
└── dashboards/                         # [To be added]
```

---

## 🔗 Resources

- [GDELT Project](https://www.gdeltproject.org/)
- [GDELT Documentation](https://www.gdeltproject.org/data.html)
- [Original Project Repository](https://github.com/BrandonRH17/factored-datathon-2024-Neutrino-Solutions)

---

