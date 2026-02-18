import duckdb
import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)
Faker.seed(42)

# ---- Parameters ----
N_CUSTOMERS   = 50_000
N_CAMPAIGNS   = 200
N_CLICKS      = 300_000
N_CONVERSIONS = 60_000

print("Generating customers...")
customers = pd.DataFrame({
    "customer_id":  range(1, N_CUSTOMERS + 1),
    "first_name":   [fake.first_name() for _ in range(N_CUSTOMERS)],
    "last_name":    [fake.last_name()  for _ in range(N_CUSTOMERS)],
    "email":        [fake.email()      for _ in range(N_CUSTOMERS)],
    "country":      [fake.country()    for _ in range(N_CUSTOMERS)],
    "signup_date":  [fake.date_between(start_date="-2y", end_date="today")
                     for _ in range(N_CUSTOMERS)],
})

print("Generating campaigns...")
channels  = ["paid_search", "paid_social", "email", "organic", "display"]
statuses  = ["active", "paused", "completed"]
campaigns = pd.DataFrame({
    "campaign_id":   range(1, N_CAMPAIGNS + 1),
    "campaign_name": [fake.bs().title() for _ in range(N_CAMPAIGNS)],
    "channel":       [random.choice(channels)  for _ in range(N_CAMPAIGNS)],
    "start_date":    [fake.date_between(start_date="-2y", end_date="-30d")
                      for _ in range(N_CAMPAIGNS)],
    "budget_usd":    [round(random.uniform(500, 20000), 2) for _ in range(N_CAMPAIGNS)],
    "status":        [random.choice(statuses)  for _ in range(N_CAMPAIGNS)],
})
campaigns["end_date"] = campaigns["start_date"].apply(
    lambda d: fake.date_between(start_date=d, end_date="today")
)

print("Generating ad clicks...")
ad_clicks = pd.DataFrame({
    "click_id":    range(1, N_CLICKS + 1),
    "campaign_id": [random.randint(1, N_CAMPAIGNS)  for _ in range(N_CLICKS)],
    "customer_id": [random.randint(1, N_CUSTOMERS)  for _ in range(N_CLICKS)],
    "click_date":  [fake.date_between(start_date="-2y", end_date="today")
                    for _ in range(N_CLICKS)],
    "device":      [random.choice(["mobile", "desktop", "tablet"])
                    for _ in range(N_CLICKS)],
    "cost_usd":    [round(random.uniform(0.10, 5.00), 2) for _ in range(N_CLICKS)],
})

print("Generating conversions...")
conversion_types = ["purchase", "signup", "lead", "download"]
sampled_clicks   = random.sample(range(1, N_CLICKS + 1), N_CONVERSIONS)
conversions = pd.DataFrame({
    "conversion_id":   range(1, N_CONVERSIONS + 1),
    "click_id":        sampled_clicks,
    "customer_id":     [random.randint(1, N_CUSTOMERS)  for _ in range(N_CONVERSIONS)],
    "campaign_id":     [random.randint(1, N_CAMPAIGNS)  for _ in range(N_CONVERSIONS)],
    "conversion_date": [fake.date_between(start_date="-2y", end_date="today")
                        for _ in range(N_CONVERSIONS)],
    "revenue_usd":     [round(random.uniform(5, 500), 2) for _ in range(N_CONVERSIONS)],
    "conversion_type": [random.choice(conversion_types) for _ in range(N_CONVERSIONS)],
})

# ---- Load into DuckDB ----
print("Loading into DuckDB...")
con = duckdb.connect("dev.duckdb")

con.execute("CREATE SCHEMA IF NOT EXISTS raw")
con.execute("CREATE OR REPLACE TABLE raw.customers    AS SELECT * FROM customers")
con.execute("CREATE OR REPLACE TABLE raw.campaigns    AS SELECT * FROM campaigns")
con.execute("CREATE OR REPLACE TABLE raw.ad_clicks    AS SELECT * FROM ad_clicks")
con.execute("CREATE OR REPLACE TABLE raw.conversions  AS SELECT * FROM conversions")

con.close()
print("Done! Data loaded into dev.duckdb")
print(f"   customers:   {len(customers):,} rows")
print(f"   campaigns:   {len(campaigns):,} rows")
print(f"   ad_clicks:   {len(ad_clicks):,} rows")
print(f"   conversions: {len(conversions):,} rows")