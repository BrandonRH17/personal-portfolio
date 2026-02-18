# Marketing Analytics — dbt + DuckDB

An end-to-end analytics engineering project that transforms raw marketing data into campaign performance insights using **dbt** and **DuckDB**.

## Architecture

```
Raw Data (Python/Faker)  →  DuckDB  →  dbt Staging  →  dbt Mart
     generate_data.py         ↓         (views)        (tables)
                           raw schema    staging schema  marts schema
```

**Raw layer** — Synthetic data generated with Faker: customers, campaigns, ad clicks, and conversions.

**Staging layer** — Cleans and standardizes each source table: proper casing, email validation, type casting, rounding.

**Mart layer** — Business-ready aggregations: campaign performance metrics like cost per click, cost per conversion, and total revenue.

## Project Structure

```
├── models/
│   ├── staging/
│   │   ├── stg_ga__customers.sql
│   │   ├── stg_ga__campaigns.sql
│   │   ├── stg_ga__ad_clicks.sql
│   │   ├── stg_ga__conversions.sql
│   │   ├── sources.yml
│   │   └── staging.yml
│   └── mart/
│       ├── mrt_ga__campaign_performance.sql
│       └── mart.yml
├── macros/
│   ├── clean_email.sql          # Validates and lowercases emails
│   └── proper_case.sql          # Capitalizes first letter of strings
├── tests/
│   ├── staging/
│   │   └── assert_ad_clicks_cost_not_negative.sql
│   └── mart/
│       ├── assert_cost_per_conversion_not_negative.sql
│       └── assert_q_converted_users_not_null.sql
├── profiles/
│   └── profiles.yml             # DuckDB connection config
├── generate_data.py             # Synthetic data generator
├── exploration.ipynb            # Raw data exploration notebook
├── dbt_project.yml
└── requirements.txt
```

## Quick Start

```bash
# 1. Clone the repo
git clone <your-repo-url>
cd marketing_analytics

# 2. Create a virtual environment
python -m venv venv
source venv/bin/activate        # Linux/Mac
venv\Scripts\activate           # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Generate synthetic data into DuckDB
python generate_data.py

# 5. Run dbt models
dbt run --profiles-dir profiles

# 6. Run tests
dbt test --profiles-dir profiles
```

## Data Model

| Raw Table | Records | Description |
|-----------|---------|-------------|
| `raw.customers` | 50,000 | Customer profiles (name, email, country, signup date) |
| `raw.campaigns` | 200 | Marketing campaigns across 5 channels |
| `raw.ad_clicks` | 300,000 | Individual click events with device and cost |
| `raw.conversions` | 60,000 | Conversion events (purchase, signup, lead, download) |

### Mart: Campaign Performance

The final `mrt_ga__campaign_performance` table provides per-campaign metrics:

- Total clicks and unique clicked users
- Average cost per click
- Converted users and average cost per conversion
- Total revenue generated

## Tests

The project includes both **generic tests** (unique, not_null, accepted_values) defined in YAML and **custom singular tests** that validate business rules like non-negative costs and valid conversion counts.

```bash
dbt test --profiles-dir profiles
```

## Tech Stack

- **dbt-core** — SQL transformation framework
- **DuckDB** — Embedded analytical database
- **Python** — Data generation with Faker and Pandas
