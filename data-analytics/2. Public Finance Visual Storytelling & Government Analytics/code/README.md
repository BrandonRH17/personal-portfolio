# Data Analytics Code

This folder contains the data cleaning, transformation, and analysis scripts developed for the IMF ChallengeGT project.

---

## 📂 Code Structure

### Data Processing Scripts

**Data Integration:**
- Scripts for consolidating data from Guatecompras, SICOIN, SIGES, and RGAE systems
- Data standardization and quality checks
- Vendor name normalization
- Product category mapping

**Data Cleaning:**
- Missing value imputation
- Outlier handling
- Date/time standardization
- Currency normalization

**Feature Engineering:**
- Historical baseline calculation
- Statistical measures (mean, median, z-scores)
- Temporal features (days between purchases)
- Vendor-entity relationship mapping

---

## 🔍 Analytical Algorithms

### 1. Anomaly Detection Logic

**Price Inflation Detection:**
```
1. Calculate historical price baseline (pre-pandemic)
2. Compare current prices to baseline
3. Flag outliers using z-score > 2.5
4. Benchmark against peer entity purchases
```

**Statistical Measures:**
- Z-score calculation for outlier detection
- Interquartile range (IQR) method
- Percentage deviation from baseline
- Peer comparison analysis

### 2. Purchase Splitting Detection

**Threshold Evasion Algorithm:**
```
1. Filter purchases < GTQ 50,000
2. Group by vendor-entity combination
3. Identify multiple purchases within 30 days
4. Calculate cumulative totals
5. Flag patterns exceeding threshold
```

**Pattern Recognition:**
- Temporal clustering analysis
- Frequency distribution
- Suspicious amount ranges (GTQ 45K-49.9K)
- Recurring vendor-entity pairs

### 3. Risk Scoring Model

**Vendor Risk Score:**
```python
risk_score = (
    anomaly_count * 0.4 +
    splitting_instances * 0.3 +
    price_deviation * 0.2 +
    transaction_frequency * 0.1
)
```

**Entity Risk Assessment:**
- Aggregated vendor risk scores
- Historical compliance patterns
- Geographic risk factors
- Procurement volume analysis

---

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|---------|
| **Python** | Data processing and analysis |
| **Pandas** | Data manipulation |
| **NumPy** | Statistical calculations |
| **Power Query (M)** | Power BI data transformation |
| **DAX** | Power BI calculated measures |

---

## 📊 Data Model

### Key Tables

**Fact Tables:**
- `FactProcurement` - Transaction-level purchases
- `FactAnomalies` - Detected anomalies and flags
- `FactSplitting` - Purchase splitting instances

**Dimension Tables:**
- `DimVendor` - Supplier information (from RGAE)
- `DimEntity` - Healthcare institutions
- `DimProduct` - Medical supply categories
- `DimDate` - Date dimension for time intelligence

**Relationships:**
- Star schema design
- Many-to-one cardinality
- Bidirectional filters where needed

---

## 🔄 Data Pipeline

```
Raw Government Data
    ↓
Data Integration
    ↓
Data Cleaning
    ↓
Feature Engineering
    ↓
Anomaly Detection
    ↓
Risk Scoring
    ↓
Power BI Dashboard
```

---

## 📈 Key Metrics & Calculations

**DAX Measures (Power BI):**

**Price Deviation:**
```dax
Price Deviation % =
DIVIDE(
    [Current Price] - [Historical Baseline],
    [Historical Baseline]
)
```

**Splitting Flag:**
```dax
Is Splitting =
IF(
    [Purchase Amount] < 50000 &&
    [Purchases Last 30 Days] > 1,
    "Yes",
    "No"
)
```

**Risk Score:**
```dax
Risk Score =
[Anomaly Count] * 0.4 +
[Splitting Count] * 0.3 +
[Price Deviation Score] * 0.2 +
[Frequency Score] * 0.1
```

---

## 🚀 Usage

### Running the Analysis

1. **Data Extraction:**
   - Extract data from government systems
   - Save as CSV/Excel format

2. **Data Processing:**
   - Run cleaning scripts
   - Execute feature engineering
   - Generate anomaly flags

3. **Power BI Integration:**
   - Load processed data into Power BI
   - Refresh data model
   - Update visualizations

---

## 📝 Notes

- Scripts designed for reproducibility
- Comments included for methodology transparency
- Optimized for Guatemala government data schemas
- Scalable to other procurement categories

---

For context on the overall project and results, see the main [project README](../README.md).
