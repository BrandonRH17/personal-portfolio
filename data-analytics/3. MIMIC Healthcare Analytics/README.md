# MIMIC Healthcare Analytics

## Status
**WORK IN PROGRESS** - This project is currently under active development.

## Overview
Analysis of the MIMIC-IV Clinical Database Demo, exploring patient demographics, hospital admissions, diagnoses, and clinical outcomes using real deidentified healthcare data.

## Dataset
- **Source:** MIMIC-IV Clinical Database Demo v2.2
- **Origin:** Beth Israel Deaconess Medical Center
- **Size:** 100 patients, 26 tables
- **License:** MIT

## Project Structure
```
3. MIMIC Healthcare Analytics/
├── notebooks/           # Jupyter notebooks for analysis
├── code/               # Python scripts
├── data/               # Data files (downloaded separately)
├── dashboards/         # Visualizations and dashboards
└── presentations/      # Project presentations
```

## Current Progress

### Completed
- Initial data acquisition and setup
- Exploratory Data Analysis: Patients & Admissions
- Exploratory Data Analysis: Diagnoses

### In Progress
- Additional clinical data analysis
- Visualization dashboards
- Key insights documentation

## Setup Instructions

### Prerequisites
- Python 3.8+
- Jupyter Notebook
- Required packages: pandas, numpy, matplotlib, seaborn

### Installation
```bash
pip install pandas numpy matplotlib seaborn jupyter kaggle
```

### Data Download
1. Ensure you have Kaggle API credentials configured
2. Download the dataset:
```bash
kaggle datasets download -d montassarba/mimic-iv-clinical-database-demo-2-2
```
3. Extract to `data/` folder

## Notebooks

### 01 - Exploratory Analysis: Patients & Admissions
Analysis of patient demographics and hospital admission patterns:
- Gender and age distributions
- Admission types and locations
- Length of stay analysis
- Mortality rates
- Readmission patterns

### 02 - Exploratory Analysis: Diagnoses
Analysis of ICD diagnosis codes and their relationships:
- Most common diagnoses
- Comorbidities analysis
- Diagnoses associated with mortality
- Length of stay by diagnosis
- Demographics and diagnosis patterns

## Key Technologies
- Python (pandas, numpy)
- Data Visualization (matplotlib, seaborn)
- Healthcare Data Standards (ICD-9/ICD-10)
- SQL-like data operations (joins, aggregations)

## License
This analysis is for educational and portfolio purposes. The MIMIC-IV dataset is used under MIT License.

---

**Author:** Brandon Rodriguez
**Last Updated:** 2026-02-10
