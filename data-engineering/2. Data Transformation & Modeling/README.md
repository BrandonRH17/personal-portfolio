# Data Transformation & Modeling
### ELT Pipeline with dbt & DuckDB — Marketing Campaign Data

---

## Overview

This project demonstrates foundational **data engineering best practices** by building a fully structured ELT transformation pipeline using dbt and DuckDB. It uses synthetic marketing data (50K customers, 200 campaigns, 300K ad clicks, 60K conversions) to showcase how raw data should be systematically ingested, cleaned, modeled, tested, and documented at each layer.

The goal is not the marketing domain itself — it's the **engineering discipline** applied to it: how to structure a transformation project so it's maintainable, testable, and reproducible.

---

## Core Data Engineering Concepts Demonstrated

### 1. ELT over ETL
Data lands raw into DuckDB first, then transformations happen in-database using SQL — following the modern ELT (Extract → Load → Transform) pattern rather than transforming before loading. This leverages the power of the analytical engine at transformation time, keeping the pipeline simpler and more flexible.

### 2. Layered Architecture (Staging → Mart)
```
Raw Data (Python/Faker)  →  DuckDB (raw schema)  →  Staging (views)  →  Mart (tables)
```
Each layer has a single, clear responsibility:

| Layer | Schema | Materialization | Purpose |
|-------|--------|-----------------|---------|
| Raw | `raw` | External tables | Source data as-is, never modified |
| Staging | `staging` | Views | 1:1 source cleaning — types, naming, formatting |
| Mart | `marts` | Tables | Business logic — joins, aggregations, metrics |

This separation enforces the principle that **cleaning and business logic never mix in the same model**.

### 3. Naming Conventions
Consistent prefixes make pipeline structure immediately readable:
- `stg_<source>__<entity>` → staging models (e.g., `stg_ga__customers`)
- `mrt_<source>__<metric>` → mart models (e.g., `mrt_ga__campaign_performance`)

This convention is a dbt community standard and signals at a glance what layer and source a model belongs to.

### 4. DRY Principle with Reusable Macros
Instead of repeating transformation logic across models, shared logic is extracted into macros:

- `clean_email(column)` — lowercases and trims email addresses
- `proper_case(column)` — standardizes name and country capitalization

Macros are the dbt equivalent of functions: write once, use everywhere, test centrally.

### 5. Source Declarations and Documentation
All raw data sources are declared in `sources.yml`, which:
- Establishes a contract between raw data and staging models
- Enables source freshness checks (detects stale or late-arriving data)
- Auto-generates lineage in the dbt DAG

Model documentation lives alongside the code in YAML schema files — keeping docs and implementation in sync rather than drifting apart in separate wikis.

### 6. Testing Strategy
Data quality is enforced at every layer through two test types:

**Generic tests** (applied via YAML — no SQL needed):
- `unique` — primary key integrity
- `not_null` — required field validation
- `accepted_values` — enum and categorical validation

**Singular tests** (custom SQL assertions for business rules):
- `assert_ad_clicks_cost_not_negative.sql` — cost fields must be ≥ 0
- `assert_cost_per_conversion_not_negative.sql` — derived metrics must be valid
- `assert_q_converted_users_not_null.sql` — conversion counts must be present

This two-tier approach covers both structural integrity and domain-specific business logic.

### 7. Reproducibility
The entire pipeline is deterministic and self-contained:
- Data generation uses a fixed seed (`seed = 42`) — same data every run
- No cloud services or external databases required — DuckDB runs embedded
- Six commands from clone to fully tested pipeline:

```bash
python -m venv venv && venv\Scripts\activate
pip install -r requirements.txt
python generate_data.py
dbt run --profiles-dir profiles
dbt test --profiles-dir profiles
```

---

## Project Structure

```
2. Data Transformation & Modeling/
├── code/
│   ├── models/
│   │   ├── staging/              # Cleaning layer — 4 models + YAML docs + source declarations
│   │   │   ├── sources.yml
│   │   │   ├── staging.yml
│   │   │   ├── stg_ga__ad_clicks.sql
│   │   │   ├── stg_ga__campaigns.sql
│   │   │   ├── stg_ga__conversions.sql
│   │   │   └── stg_ga__customers.sql
│   │   └── mart/                 # Business logic layer — aggregated campaign performance
│   │       ├── mart.yml
│   │       └── mrt_ga__campaign_performance.sql
│   ├── macros/                   # Reusable SQL functions
│   │   ├── clean_email.sql
│   │   └── proper_case.sql
│   ├── tests/                    # Custom singular tests (business rules)
│   │   ├── staging/
│   │   │   └── assert_ad_clicks_cost_not_negative.sql
│   │   └── mart/
│   │       ├── assert_cost_per_conversion_not_negative.sql
│   │       └── assert_q_converted_users_not_null.sql
│   ├── profiles/
│   │   └── profiles.yml          # DuckDB connection config
│   ├── generate_data.py          # Reproducible synthetic data generator (seed=42)
│   ├── exploration.ipynb         # Raw data exploration before modeling
│   ├── dbt_project.yml
│   ├── requirements.txt
│   └── README.md                 # Technical setup guide
└── README.md                     # This file
```

---

## Pipeline Lineage

```
sources.yml declares:
  raw.ga__customers     ──→  stg_ga__customers     ──┐
  raw.ga__campaigns     ──→  stg_ga__campaigns     ──┤
  raw.ga__ad_clicks     ──→  stg_ga__ad_clicks     ──┼──→  mrt_ga__campaign_performance
  raw.ga__conversions   ──→  stg_ga__conversions   ──┘
```

Each staging model is a view that reads directly from one raw table. The mart joins all four cleaned sources and computes campaign-level KPIs: total clicks, unique users, cost per click, conversions, cost per conversion, and total revenue.

---

## Mart Output: Campaign Performance

| Column | Description |
|--------|-------------|
| `campaign_id` | Unique campaign identifier |
| `campaign_name` | Cleaned campaign name |
| `total_clicks` | Total ad clicks for the campaign |
| `unique_clicked_users` | Distinct users who clicked |
| `avg_cost_per_click` | Average CPC across all clicks |
| `total_converted_users` | Users who converted |
| `cost_per_conversion` | Total cost / converted users |
| `total_revenue` | Sum of revenue from conversions |

---

## Technologies

| Tool | Role |
|------|------|
| **dbt-core** | SQL transformation framework — models, macros, tests, docs |
| **DuckDB** | Embedded OLAP database — zero infrastructure, local execution |
| **Python + Faker** | Synthetic data generation (seed-based, reproducible) |
| **Jupyter** | Raw data exploration before modeling decisions |

---

## Setup

See [code/README.md](code/README.md) for step-by-step instructions.

---

**Author:** Brandon Rodriguez
**Last Updated:** 2026-02-17
