# Analytics Engineering with Marketing Data
### Campaign Performance Pipeline with dbt & DuckDB

---

## 📋 Project Overview (STAR Format)

### 🎯 Situation

Marketing teams generate large volumes of data across campaigns, ad clicks, and conversions, but raw data often arrives with inconsistent formatting, mixed casing, and unvalidated fields. Without a structured transformation layer, deriving reliable campaign performance metrics becomes error-prone and difficult to maintain.

This project addresses that challenge by building a **modern analytics engineering pipeline** using dbt and DuckDB — demonstrating how raw marketing data can be systematically cleaned, tested, and transformed into business-ready insights.

### 📝 Task

Design and implement an end-to-end data transformation pipeline that:

- Ingests synthetic but realistic marketing data (50K customers, 200 campaigns, 300K clicks, 60K conversions)
- Cleans and standardizes raw data through a staging layer
- Aggregates key campaign performance metrics in a mart layer
- Validates data quality through comprehensive testing at every layer

### ⚙️ Action

**Technologies & Architecture:**

- **dbt-core**: SQL transformation framework following software engineering best practices
- **DuckDB**: Embedded analytical database — zero infrastructure, runs locally
- **Python + Faker**: Reproducible synthetic data generation (seed = 42)
- **Jupyter Notebook**: Raw data exploration and transformation design

**Pipeline Architecture:**

```
Raw Data (Python/Faker)  →  DuckDB  →  dbt Staging (views)  →  dbt Mart (tables)
                           raw schema   staging schema         marts schema
```

**Key Implementation Details:**

1. **Staging Layer** — 4 models that clean each source table:
   - Email validation and lowercasing via custom `clean_email` macro
   - Name and country standardization via `proper_case` macro
   - Type casting (timestamps → dates) and rounding (USD to 2 decimals)

2. **Mart Layer** — Campaign performance aggregation:
   - Total clicks, unique clicked users, average cost per click
   - Converted users, cost per conversion, total revenue generated
   - Joins across campaigns, clicks, and conversions

3. **Testing Strategy** — Generic tests (unique, not_null, accepted_values) + custom singular tests for business rules (non-negative costs, valid conversion counts)

### 🎯 Result

✅ **Fully Reproducible Pipeline** — Anyone can clone, generate data, and run the entire pipeline in under 2 minutes with 6 commands

✅ **Data Quality Assurance** — 20+ automated tests validating uniqueness, nullability, accepted values, and business logic

✅ **Clean Architecture** — Follows dbt best practices: source definitions, YAML documentation, staging/mart separation, reusable macros

✅ **Zero Infrastructure** — Runs entirely on DuckDB (no cloud services, no database server needed)

---

## 📂 Project Structure

```
├── code/
│   ├── models/
│   │   ├── staging/          # 4 cleaning models + YAML docs
│   │   └── mart/             # Campaign performance aggregation
│   ├── macros/               # clean_email, proper_case
│   ├── tests/                # Custom singular tests
│   ├── profiles/             # DuckDB connection config
│   ├── generate_data.py      # Synthetic data generator
│   ├── exploration.ipynb     # Raw data exploration
│   ├── dbt_project.yml
│   ├── requirements.txt
│   └── README.md             # Technical setup guide
└── README.md                 # This file
```

---

## 🚀 Quick Start

```bash
cd code
python -m venv venv && venv\Scripts\activate   # Windows
pip install -r requirements.txt
python generate_data.py
dbt run --profiles-dir profiles
dbt test --profiles-dir profiles
```

See [code/README.md](code/README.md) for detailed setup instructions.

---
