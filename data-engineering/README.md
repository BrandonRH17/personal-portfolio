<div align="center">
  <img src="https://anthropos.work/_next/image?url=https%3A%2F%2Fcontent.anthropos.work%2Fassets%2Ff6e18932-35b9-4d8f-9cb5-cb22e84be577&w=3840&q=90" alt="Data Engineering Header" width="100%" style="max-width: 800px;" />
</div>

# Data Engineering

End-to-end data engineering projects covering big data pipelines, cloud-scale processing, and structured transformation workflows — from raw ingestion to analytics-ready tables.

---

## Projects at a Glance

| # | Project | Scale | Core Technologies | Key Concepts |
|---|---------|-------|------------------|--------------|
| 1 | [GDELT Big Data Engineering](#1-gdelt-big-data-engineering--maritime-port-disruption-prediction) | Cloud / Distributed | Databricks, Spark SQL, Delta Lake | Medallion architecture, workflow orchestration, big data ingestion |
| 2 | [Data Transformation & Modeling](#2-data-transformation--modeling) | Local / Embedded | dbt, DuckDB, Python | ELT pattern, layered modeling, data quality testing |

---

## Find Your Project By...

### Technology
| If you're looking for... | Go to |
|--------------------------|-------|
| Databricks / Apache Spark | Project 1 |
| Delta Lake / Unity Catalog | Project 1 |
| dbt (data build tool) | Project 2 |
| DuckDB / embedded OLAP | Project 2 |
| Python data pipelines | Projects 1, 2 |
| SQL transformation frameworks | Project 2 |

### Concept / Pattern
| If you're looking for... | Go to |
|--------------------------|-------|
| Medallion architecture (Bronze → Silver → Gold) | Project 1 |
| ELT pattern and layered modeling (Staging → Mart) | Project 2 |
| Workflow orchestration (Databricks Workflows) | Project 1 |
| Data quality testing and validation | Project 2 |
| Reusable macros and DRY pipeline design | Project 2 |
| Source declarations and lineage | Project 2 |
| Cloud-scale distributed processing | Project 1 |
| Reproducible local pipelines | Project 2 |

### Scale
| If you're looking for... | Go to |
|--------------------------|-------|
| Production-scale, cloud infrastructure | Project 1 |
| Local, zero-infrastructure pipeline | Project 2 |

---

## Project Summaries

### 1. GDELT Big Data Engineering — Maritime Port Disruption Prediction
**Award: Overall Grand Winner — Factored Datathon 2024**

Production-grade data lakehouse built on Databricks to process the GDELT global news dataset at scale. Implements a full medallion architecture: Bronze (raw ingestion with Delta upserts and control date tracking), Silver (web scraping and GKG enrichment), Gold (weighted news summaries for analytics). Automated via Databricks Workflows with dependency management between layers.

**Stack:** Databricks, Spark SQL, Delta Lake, Python, Databricks Workflows
**Highlights:** Incremental ingestion, control date pattern, Bronze/Silver/Gold separation, cloud-scale execution

[View Project](1.%20GDELT%20Big%20Data%20Engineering/README.md)

---

### 2. Data Transformation & Modeling
Structured ELT pipeline built with dbt and DuckDB over synthetic marketing data (50K customers, 200 campaigns, 300K clicks). Demonstrates data engineering fundamentals: layered architecture (staging → mart), naming conventions, reusable macros, source declarations, and a two-tier testing strategy (generic + singular tests). Fully reproducible with zero infrastructure required.

**Stack:** dbt-core, DuckDB, Python, Faker
**Highlights:** ELT pattern, staging/mart separation, 20+ automated tests, pipeline lineage, deterministic data generation

[View Project](2.%20Data%20Transformation%20%26%20Modeling/README.md)

---

**Author:** Brandon Rodriguez
