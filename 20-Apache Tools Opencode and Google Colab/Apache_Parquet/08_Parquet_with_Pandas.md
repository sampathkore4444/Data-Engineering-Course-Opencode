# Parquet with Pandas

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-financial-reporting-pipeline)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-ml-feature-engineering)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### Pandas and Parquet Integration

Pandas provides seamless Parquet integration through PyArrow:

> **Pandas can read and write Parquet files directly, using PyArrow as the engine. This makes it easy to persist DataFrames to efficient columnar storage.**

### How Pandas Writes Parquet

```python
import pandas as pd

df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "amount": [100.0, 250.0, 500.0],
})

# Write to Parquet (uses PyArrow internally)
df.to_parquet("output.parquet", engine="pyarrow")
```

### How Pandas Reads Parquet

```python
# Read from Parquet
df = pd.read_parquet("output.parquet", engine="pyarrow")

# Read with column selection
df = pd.read_parquet("output.parquet", columns=["id", "amount"])

# Read with filters (PyArrow engine)
df = pd.read_parquet("output.parquet", filters=[("amount", ">", 100)])
```

### Pandas Parquet Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `engine` | Backend ("pyarrow" or "fastparquet") | "pyarrow" |
| `compression` | Compression codec | "snappy" |
| `index` | Write DataFrame index | True |
| `partition_cols` | Columns to partition by | None |
| `schema` | Explicit Arrow schema | None |
| `row_group_size` | Rows per row group | None |

### Writing with Pandas

```python
# Basic write
df.to_parquet("output.parquet")

# With compression
df.to_parquet("output.parquet", compression="zstd")

# Without index (recommended)
df.to_parquet("output.parquet", index=False)

# Partitioned write
df.to_parquet(
    "output/",
    engine="pyarrow",
    partition_cols=["date", "region"],
)

# With explicit schema
import pyarrow as pa
schema = pa.schema([
    ("id", pa.int64()),
    ("amount", pa.decimal128(18, 2)),
])
df.to_parquet("output.parquet", schema=schema)
```

### Reading with Pandas

```python
# Basic read
df = pd.read_parquet("output.parquet")

# Column selection (column pruning)
df = pd.read_parquet("output.parquet", columns=["id", "amount"])

# Filters (predicate pushdown)
df = pd.read_parquet(
    "output.parquet",
    filters=[("amount", ">", 1000), ("status", "=", "COMPLETED")]
)

# Read specific columns with filters
df = pd.read_parquet(
    "output.parquet",
    columns=["id", "amount", "status"],
    filters=[("amount", ">", 1000)]
)

# Read partitioned dataset
df = pd.read_parquet(
    "output/",
    filters=[("date", ">=","2026-08-01")]
)
```

### Pandas Index Handling

By default, Pandas writes the DataFrame index to Parquet:

```python
df = pd.DataFrame({"amount": [100, 200]}, index=[1, 2])
df.to_parquet("with_index.parquet")  # index is saved

# Read back - index is restored
df = pd.read_parquet("with_index.parquet")
print(df.index)  # Int64Index([1, 2])

# To avoid saving index:
df.to_parquet("without_index.parquet", index=False)
```

### Type Mapping: Pandas ↔ Parquet

| Pandas Type | Parquet Type |
|-------------|-------------|
| int64 | INT64 |
| float64 | DOUBLE |
| object (strings) | BYTE_ARRAY (STRING) |
| bool | BOOLEAN |
| datetime64 | TIMESTAMP |
| category | BYTE_ARRAY (with dictionary) |

**Common issues:**
- `object` columns with mixed types → error
- `datetime64` with timezone → TIMESTAMP with timezone
- `int64` with nulls → becomes FLOAT64 (Pandas limitation)

### Large DataFrames

For DataFrames larger than RAM:

```python
# Write in chunks
chunk_size = 1_000_000
for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i+chunk_size]
    chunk.to_parquet(f"chunk_{i}.parquet", index=False)

# Read with filters (PyArrow pushes down to file level)
df = pd.read_parquet("chunks/", filters=[("date", ">=", "2026-08-01")])
```

---

## 2. Example

### Complete Pandas Parquet Workflow

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

# Create sample data
df = pd.DataFrame({
    "transaction_id": range(1, 1000001),
    "account_id": [f"ACC{i:06d}" for i in range(1, 1000001)],
    "amount": np.random.uniform(1.0, 100000.0, 1000000).round(2),
    "currency": np.random.choice(["USD", "EUR", "GBP"], 1000000),
    "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], 1000000),
    "date": pd.date_range("2026-01-01", periods=1000000, freq="s"),
})

# 1. Write to Parquet
df.to_parquet("transactions.parquet", engine="pyarrow", compression="zstd", index=False)
print(f"Written: {len(df):,} rows")

# 2. Read back
df_read = pd.read_parquet("transactions.parquet")
print(f"Read: {len(df_read):,} rows")

# 3. Read with column pruning
df_partial = pd.read_parquet("transactions.parquet", columns=["amount", "status"])
print(f"Partial: {df_partial.shape}")

# 4. Read with filters
df_filtered = pd.read_parquet(
    "transactions.parquet",
    filters=[("amount", ">", 10000), ("status", "=", "COMPLETED")]
)
print(f"Filtered: {len(df_filtered):,} rows")

# 5. Compute summary
summary = df_filtered.groupby("currency").agg(
    total=("amount", "sum"),
    count=("amount", "count"),
    avg=("amount", "mean"),
).round(2)
print(f"\nSummary:\n{summary}")
```

---

## 3. Banking Scenario 1: Financial Reporting Pipeline

### Problem
A bank's finance team generates monthly reports from Parquet data using Pandas:
- P&L statements
- Balance sheets
- Regulatory reports (Basel III, CCAR)
- Customer profitability analysis

Data: 50 GB of Parquet files, reports must complete in 30 minutes.

### Why Pandas + Parquet?
- Finance team knows Pandas
- Parquet provides fast reads with column pruning
- Reports only need 5-10 columns each
- Partitioned Parquet enables date-range filtering

### Architecture
```
Parquet Data Lake (S3)
       |
       v
  Pandas (read_parquet with filters)
       |
       v
  Financial Calculations
       |
       v
  Excel / PDF Reports
```

---

## 4. Python Code - Scenario 1

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile

# ============================================================
# BANKING SCENARIO: Financial Reporting Pipeline
# ============================================================

def generate_financial_data(num_rows=500_000):
    """Generate realistic financial transaction data."""
    random.seed(42)
    np.random.seed(42)

    dates = pd.date_range("2026-01-01", "2026-08-31", freq="D")
    accounts = [f"ACC{random.randint(100000, 999999)}" for _ in range(num_rows)]
    branches = [f"BR{random.randint(100, 999)}" for _ in range(num_rows)]
    products = ["CHECKING", "SAVINGS", "CREDIT_CARD", "LOAN", "MORTGAGE"]
    departments = ["RETAIL", "CORPORATE", "INVESTMENT", "WEALTH_MGMT"]

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "account_id": accounts,
        "branch_id": branches,
        "product": np.random.choice(products, num_rows),
        "department": np.random.choice(departments, num_rows),
        "revenue": np.random.uniform(0.01, 50000.0, num_rows).round(2),
        "cost": np.random.uniform(0.01, 10000.0, num_rows).round(2),
        "fee_income": np.random.uniform(0.0, 500.0, num_rows).round(2),
        "interest_income": np.random.uniform(0.0, 2000.0, num_rows).round(2),
        "date": np.random.choice(dates, num_rows),
        "regulatory_capital": np.random.uniform(100000, 1000000, num_rows).round(2),
    })

    df["profit"] = df["revenue"] - df["cost"] + df["fee_income"] + df["interest_income"]

    return df


def write_financial_data(df, base_path):
    """Write financial data partitioned by date."""
    df.to_parquet(
        base_path,
        engine="pyarrow",
        compression="zstd",
        index=False,
        partition_cols=["date"],
    )
    print(f"Wrote {len(df):,} financial records")


def generate_pnl_report(data_path, start_date, end_date):
    """Generate P&L statement using Pandas + Parquet."""
    start_time = datetime.now()

    # Read with filters (only needed date range)
    df = pd.read_parquet(
        data_path,
        filters=[
            ("date", ">=", pd.Timestamp(start_date)),
            ("date", "<=", pd.Timestamp(end_date)),
        ],
        columns=["product", "department", "revenue", "cost", "fee_income",
                 "interest_income", "profit"],
    )

    read_time = (datetime.now() - start_time).total_seconds()

    # P&L by department
    pnl_by_dept = df.groupby("department").agg(
        total_revenue=("revenue", "sum"),
        total_cost=("cost", "sum"),
        fee_income=("fee_income", "sum"),
        interest_income=("interest_income", "sum"),
        net_profit=("profit", "sum"),
        transaction_count=("revenue", "count"),
    ).round(2)

    pnl_by_dept["profit_margin"] = (pnl_by_dept["net_profit"] / pnl_by_dept["total_revenue"] * 100).round(2)

    # P&L by product
    pnl_by_product = df.groupby("product").agg(
        total_revenue=("revenue", "sum"),
        total_cost=("cost", "sum"),
        net_profit=("profit", "sum"),
    ).round(2)

    print(f"\n=== P&L Report ({start_date} to {end_date}) ===")
    print(f"Data read time: {read_time:.3f}s")
    print(f"Total transactions: {len(df):,}")
    print(f"\n--- By Department ---")
    print(pnl_by_dept.to_string())
    print(f"\n--- By Product ---")
    print(pnl_by_product.to_string())

    # Total P&L
    print(f"\n--- Total P&L ---")
    print(f"Total Revenue: ${df['revenue'].sum():,.2f}")
    print(f"Total Cost: ${df['cost'].sum():,.2f}")
    print(f"Fee Income: ${df['fee_income'].sum():,.2f}")
    print(f"Interest Income: ${df['interest_income'].sum():,.2f}")
    print(f"Net Profit: ${df['profit'].sum():,.2f}")

    return pnl_by_dept


def generate_regulatory_report(data_path):
    """Generate Basel III regulatory capital report."""
    df = pd.read_parquet(
        data_path,
        columns=["branch_id", "product", "regulatory_capital", "revenue", "profit"],
    )

    # Risk-weighted assets (simplified)
    risk_weights = {
        "CHECKING": 0.10,
        "SAVINGS": 0.05,
        "CREDIT_CARD": 1.00,
        "LOAN": 0.75,
        "MORTGAGE": 0.35,
    }

    df["risk_weight"] = df["product"].map(risk_weights)
    df["risk_weighted_assets"] = df["regulatory_capital"] * df["risk_weight"]

    # Capital adequacy ratio
    total_capital = df["regulatory_capital"].sum()
    total_rwa = df["risk_weighted_assets"].sum()
    car = total_capital / total_rwa * 100

    print(f"\n=== Basel III Regulatory Report ===")
    print(f"Total Capital: ${total_capital:,.2f}")
    print(f"Total Risk-Weighted Assets: ${total_rwa:,.2f}")
    print(f"Capital Adequacy Ratio: {car:.2f}%")
    print(f"Minimum required: 8.00%")
    print(f"Status: {'✓ PASS' if car >= 8.0 else '✗ FAIL'}")

    return car


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "financial_data")
    os.makedirs(base_path, exist_ok=True)

    # Generate and write data
    print("Generating financial data...")
    df = generate_financial_data(num_rows=500_000)
    write_financial_data(df, base_path)

    # Generate P&L report
    generate_pnl_report(base_path, "2026-01-01", "2026-08-31")

    # Generate regulatory report
    generate_regulatory_report(base_path)
```

---

## 5. Banking Scenario 2: ML Feature Engineering

### Problem
A bank's data science team builds fraud detection models. They need to:
- Read Parquet transaction data
- Engineer features (rolling averages, velocity, ratios)
- Train models on historical data
- Score new transactions in real-time

Data: 100 GB of Parquet files, feature engineering must complete in 1 hour.

### Why Pandas + Parquet?
- Data scientists use Pandas for feature engineering
- Parquet provides fast column access for specific features
- Partitioned data enables date-range sampling
- Consistent format between training and scoring

### Architecture
```
Parquet Data Lake
       |
       v
  Pandas (feature engineering)
       |
       v
  Feature Store (Parquet)
       |
       v
  ML Training (scikit-learn / XGBoost)
       |
       v
  Model Registry
       |
       v
  Real-time Scoring
```

---

## 6. Python Code - Scenario 2

```python
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile

# ============================================================
# BANKING SCENARIO: ML Feature Engineering for Fraud Detection
# ============================================================

def generate_transaction_data(num_rows=200_000):
    """Generate transaction data for feature engineering."""
    random.seed(42)
    np.random.seed(42)

    timestamps = pd.date_range("2026-01-01", periods=num_rows, freq="2min")

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "card_id": [f"CARD{random.randint(10000, 99999)}" for _ in range(num_rows)],
        "merchant_id": [f"M{random.randint(1000, 9999)}" for _ in range(num_rows)],
        "amount": np.random.lognormal(3, 2, num_rows).round(2),
        "merchant_category": np.random.choice(
            ["GROCERY", "RESTAURANT", "GAS", "ONLINE", "ATM", "HOTEL", "AIRLINE"],
            num_rows
        ),
        "country": np.random.choice(["US", "GB", "DE", "FR", "JP", "CN"], num_rows),
        "channel": np.random.choice(["POS", "ONLINE", "MOBILE", "ATM"], num_rows),
        "is_fraud": np.random.choice([0, 1], num_rows, p=[0.97, 0.03]),
        "timestamp": timestamps,
    })

    return df


def engineer_features(df):
    """Engineer fraud detection features."""
    start_time = datetime.now()

    # Sort by card and timestamp
    df = df.sort_values(["card_id", "timestamp"]).reset_index(drop=True)

    # 1. Transaction velocity (count in last 1 hour, 24 hours, 7 days)
    df["tx_count_1h"] = df.groupby("card_id")["transaction_id"].transform(
        lambda x: x.rolling("1h", on=df.loc[x.index, "timestamp"]).count()
    )
    df["tx_count_24h"] = df.groupby("card_id")["transaction_id"].transform(
        lambda x: x.rolling("24h", on=df.loc[x.index, "timestamp"]).count()
    )

    # 2. Amount statistics
    df["amount_mean_24h"] = df.groupby("card_id")["amount"].transform(
        lambda x: x.rolling("24h", on=df.loc[x.index, "timestamp"]).mean()
    )
    df["amount_std_24h"] = df.groupby("card_id")["amount"].transform(
        lambda x: x.rolling("24h", on=df.loc[x.index, "timestamp"]).std()
    )

    # 3. Amount deviation from average
    df["amount_zscore"] = (df["amount"] - df["amount_mean_24h"]) / df["amount_std_24h"].replace(0, 1)

    # 4. Geographic features
    country_counts = df.groupby("card_id")["country"].transform("nunique")
    df["unique_countries_24h"] = country_counts

    # 5. Time features
    df["hour"] = df["timestamp"].dt.hour
    df["is_night"] = df["hour"].apply(lambda x: 1 if x < 6 or x > 22 else 0)
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].apply(lambda x: 1 if x >= 5 else 0)

    # 6. Merchant category risk
    fraud_by_merchant = df.groupby("merchant_category")["is_fraud"].mean()
    df["merchant_fraud_rate"] = df["merchant_category"].map(fraud_by_merchant)

    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"\n=== Feature Engineering Report ===")
    print(f"Features created: {len(df.columns)}")
    print(f"Rows processed: {len(df):,}")
    print(f"Time: {elapsed:.3f}s")
    print(f"\nFeature columns:")
    for col in df.columns:
        print(f"  - {col}")

    return df


def save_features(df, output_path):
    """Save engineered features to Parquet."""
    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="zstd",
        index=False,
    )

    size = os.path.getsize(output_path)
    print(f"\nFeatures saved: {output_path}")
    print(f"Size: {size / (1024*1024):.1f} MB")


def load_features_for_training(feature_path, sample_fraction=0.1):
    """Load features for ML training with sampling."""
    start_time = datetime.now()

    # Read with column pruning (only feature columns needed)
    feature_columns = [
        "amount", "tx_count_1h", "tx_count_24h", "amount_mean_24h",
        "amount_zscore", "unique_countries_24h", "hour", "is_night",
        "is_weekend", "merchant_fraud_rate", "is_fraud",
    ]

    df = pd.read_parquet(
        feature_path,
        columns=feature_columns,
    )

    # Sample for faster training
    df_sample = df.sample(frac=sample_fraction, random_state=42)

    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"\n=== Training Data Load ===")
    print(f"Rows loaded: {len(df):,}")
    print(f"Sampled: {len(df_sample):,} ({sample_fraction*100:.0f}%)")
    print(f"Load time: {elapsed:.3f}s")
    print(f"Fraud rate: {df_sample['is_fraud'].mean()*100:.2f}%")

    return df_sample


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "ml_features")
    os.makedirs(base_path, exist_ok=True)

    # Generate transaction data
    print("Generating transaction data...")
    df = generate_transaction_data(num_rows=200_000)

    # Save raw data
    raw_path = os.path.join(base_path, "raw_transactions.parquet")
    df.to_parquet(raw_path, engine="pyarrow", index=False)

    # Engineer features
    features_df = engineer_features(df)

    # Save features
    feature_path = os.path.join(base_path, "features.parquet")
    save_features(features_df, feature_path)

    # Load for training
    train_df = load_features_for_training(feature_path, sample_fraction=0.1)

    # Show feature correlations with fraud
    print(f"\n=== Feature Importance (correlation with fraud) ===")
    correlations = train_df.corr()["is_fraud"].drop("is_fraud").sort_values(ascending=False)
    print(correlations.to_string())
```

---

## 7. Interview Questions

### Q1: What is the difference between `pd.read_parquet()` and `pq.read_table()`?

**Answer:**

| Feature | `pd.read_parquet()` | `pq.read_table()` |
|---------|---------------------|-------------------|
| Returns | Pandas DataFrame | Arrow Table |
| Memory | Row-based (NumPy) | Columnar (Arrow) |
| Large data | Limited by RAM | Handles large data better |
| Nested data | Limited support | Full support |
| API | Pandas API | PyArrow API |
| Best for | Quick analysis | Data pipelines |

**When to use which:**
- `pd.read_parquet()`: Exploratory analysis, ML feature engineering, quick reports
- `pq.read_table()`: Production pipelines, large datasets, nested data, schema control

**Example**:
```python
# Quick analysis
df = pd.read_parquet("file.parquet")
df.groupby("status")["amount"].sum()

# Production pipeline
table = pq.read_table("file.parquet", columns=["amount", "status"])
# Process with PyArrow compute functions
```

---

### Q2: How do you handle the Pandas index when writing Parquet?

**Answer:**

**Problem**: By default, Pandas writes the DataFrame index to Parquet. This can:
- Waste storage (index column duplicated)
- Cause confusion when reading back
- Break partitioned writes

**Solution**: Always use `index=False`:

```python
# Bad: writes index
df.to_parquet("output.parquet")

# Good: no index
df.to_parquet("output.parquet", index=False)
```

**When to keep the index:**
- If the index is meaningful (e.g., time series index)
- If you need to preserve the exact DataFrame structure

**Example**:
```python
# Time series data - keep datetime index
df = pd.DataFrame({"amount": [100, 200]}, index=pd.to_datetime(["2026-01-01", "2026-01-02"]))
df.to_parquet("ts.parquet")  # index saved

# Tabular data - drop index
df = pd.DataFrame({"id": [1, 2], "amount": [100, 200]})
df.to_parquet("tabular.parquet", index=False)  # no index
```

---

### Q3: How do you read Parquet files larger than memory in Pandas?

**Answer:**

Pandas loads entire files into memory, so for large files you need strategies:

1. **Column pruning** (read fewer columns):
```python
df = pd.read_parquet("large.parquet", columns=["col1", "col2"])
```

2. **Partitioned reading** (read specific partitions):
```python
df = pd.read_parquet("data/", filters=[("date", ">=", "2026-08-01")])
```

3. **Chunked reading** (process in batches):
```python
import pyarrow.parquet as pq
dataset = pq.ParquetDataset("large.parquet")
reader = dataset.read批次(batch_size=100_000)
for batch in reader:
    df_batch = batch.to_pandas()
    process(df_batch)
```

4. **Use DuckDB** (handles memory automatically):
```python
import duckdb
con = duckdb.connect()
df = con.execute("SELECT * FROM read_parquet('large.parquet') WHERE amount > 1000").df()
```

---

### Q4: What are the common pitfalls when using Pandas with Parquet?

**Answer:**

1. **Saving the index**:
```python
# Bad
df.to_parquet("output.parquet")  # Index saved
# Good
df.to_parquet("output.parquet", index=False)
```

2. **Not specifying compression**:
```python
# Bad (uses snappy by default)
df.to_parquet("output.parquet")
# Good (explicit)
df.to_parquet("output.parquet", compression="zstd")
```

3. **Reading all columns**:
```python
# Bad
df = pd.read_parquet("large.parquet")
# Good
df = pd.read_parquet("large.parquet", columns=["needed_cols"])
```

4. **Ignoring filters**:
```python
# Bad
df = pd.read_parquet("partitioned/")
# Good
df = pd.read_parquet("partitioned/", filters=[("date", ">=", "2026-08-01")])
```

5. **Mixed types in object columns**:
```python
# Bad (will fail)
df["mixed"] = [1, "two", 3.0]
# Good (explicit types)
df["mixed"] = [1, 2, 3]  # all ints
```

---

### Q5: How do you convert between Pandas and PyArrow efficiently?

**Answer:**

**Pandas → Arrow:**
```python
# From DataFrame
table = pa.Table.from_pandas(df)

# From Series
array = pa.array(df["column"])
```

**Arrow → Pandas:**
```python
# From Table
df = table.to_pandas()

# From Array
series = table.column("column").to_pandas()
```

**Efficient conversion (zero-copy when possible):**
```python
# PyArrow to Pandas (uses Arrow's memory)
df = table.to_pandas()

# Pandas to PyArrow (preserves types)
table = pa.Table.from_pandas(df, preserve_index=False)
```

**Best practices:**
- Use `preserve_index=False` when converting Pandas → Arrow
- Use `types_mapper` for custom type mapping
- For large DataFrames, consider keeping data in Arrow format

```python
# Custom type mapping
table = pa.Table.from_pandas(
    df,
    preserve_index=False,
    types_mapper={pa.int64(): pa.decimal128(18, 2)}  # Custom mapping
)
```
