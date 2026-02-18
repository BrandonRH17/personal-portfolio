# MIMIC Healthcare Analytics

## Overview
Analysis of the MIMIC-IV Clinical Database Demo, exploring patient demographics, hospital admissions, diagnoses, and clinical outcomes using real deidentified healthcare data.

## Dataset
- **Source:** MIMIC-IV Clinical Database Demo v2.2
- **Origin:** Beth Israel Deaconess Medical Center
- **Size:** 100 patients, 26 tables, 275 admissions, 140 ICU stays
- **License:** MIT

## Project Structure
```
3. MIMIC Healthcare Analytics/
├── notebooks/           # Jupyter notebooks for analysis
│   ├── 01_exploratory_analysis_patients_admissions.ipynb
│   ├── 02_exploratory_analysis_diagnoses.ipynb
│   └── 03_exploratory_analysis_icu.ipynb
└── presentations/       # Executive presentations and reports
    ├── Executive_Presentation_ICU_Prioritization.ipynb
    ├── Executive_Summary_ICU_Patient_Prioritization.md
    └── Executive_Summary_ICU_Patient_Prioritization.html
```

## Key Findings
- **ICU Admission Rate:** 46.5% of hospitalized patients required ICU care
- **ICU Mortality Rate:** 14.3%, with highest risk in patients aged 80+ (26.3%)
- **Average ICU Stay:** 3.7 days; ICU patients had 2.1x longer hospital stays (10.2 days)
- **Top mortality diagnoses:** Palliative care encounters (77.8%), Acute respiratory failure (75.0%), Intracerebral hemorrhage (66.7%)

## Notebooks

### 01 - Exploratory Analysis: Patients & Admissions
Analysis of patient demographics and hospital admission patterns:
- Gender and age distributions
- Admission types and locations
- Length of stay analysis
- Mortality rates
- Readmission patterns

### 02 - Exploratory Analysis: Diagnoses
Analysis of ICD diagnosis codes and their clinical relationships:
- Most common diagnoses
- Comorbidities analysis
- Diagnoses associated with mortality
- Length of stay by diagnosis
- Demographics and diagnosis patterns

### 03 - Exploratory Analysis: ICU
Deep dive into ICU patient outcomes and risk stratification:
- ICU admission patterns and severity distribution
- Age-based mortality risk stratification
- High-mortality diagnosis identification
- Length of stay and resource utilization
- Clinical insights for patient prioritization

## Executive Presentation
The `presentations/` folder contains an executive-level ICU patient prioritization report in three formats (Markdown, HTML, Jupyter Notebook), suitable for clinical decision-making and resource allocation planning. See [presentations/README.md](presentations/README.md) for details.

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

## Technologies
- Python (pandas, numpy)
- Data Visualization (matplotlib, seaborn)
- Healthcare Data Standards (ICD-9/ICD-10)
- SQL-like data operations (joins, aggregations)

## License
This analysis is for educational and portfolio purposes. The MIMIC-IV dataset is used under MIT License.

---

**Author:** Brandon Rodriguez
**Last Updated:** 2026-02-17
