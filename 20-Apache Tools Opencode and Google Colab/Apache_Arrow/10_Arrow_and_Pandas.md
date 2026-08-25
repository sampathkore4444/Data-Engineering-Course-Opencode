# Concept 10: Arrow and Pandas Integration

## 📚 Detailed Explanation

**Arrow and Pandas** integration enables efficient data interchange between two powerful data processing frameworks. Arrow provides the memory format, while Pandas provides the analysis tools.

### Why Arrow + Pandas?

**Without Integration:**
```
Pandas → Serialize → Arrow → Deserialize → Pandas
        (slow)                 (slow)
```

**With Integration:**
```
Pandas ↔ Zero-Copy ↔ Arrow
        (fast)
```

### Key Features

| Feature | Pandas | Arrow |
|---------|--------|-------|
| **Primary Use** | Data analysis | Data interchange |
| **Memory** | Row-based | Columnar |
| **Mutability** | Mutable | Immutable |
| **Performance** | Moderate | Fast |

### Converting Between Pandas and Arrow

**Pandas to Arrow:**
```python
import pandas as pd
import pyarrow as pa

df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
table = pa.Table.from_pandas(df)
```

**Arrow to Pandas:**
```python
df = table.to_pandas()
```

**Zero-Copy:**
```python
# Zero-copy conversion
df = table.to_pandas(timestamp_as_object=True)
```

---

## 💡 Example: Arrow + Pandas in Banking

### Scenario: Data Analysis

```python
import pandas as pd
import pyarrow as pa

# Create Arrow table
table = pa.table({
    "transaction_id": ["TXN-001", "TXN-002", "TXN-003"],
    "amount": [50000.00, 75000.00, 60000.00],
    "status": ["COMPLETED", "PENDING", "COMPLETED"]
})

# Convert to Pandas for analysis
df = table.to_pandas()

# Analyze with Pandas
result = df.groupby("status")["amount"].sum()

# Convert back to Arrow
result_table = pa.Table.from_pandas(result.reset_index())
```

---

## 🏦 Real-World Banking Scenario 1: Data Science Pipeline

### Scenario
A bank's **data science team** needs to:
- Load data from Parquet
- Process with Pandas
- Train ML models
- Save results

### Problem
- Large datasets
- Memory limitations
- Performance needs

### Solution
Arrow + Pandas integration:
- Zero-copy conversion
- Memory efficiency
- Fast processing

### Python Code

```python
"""
Banking Scenario 1: Data Science Pipeline
Using Arrow and Pandas
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import random
import time

# ============================================================
# STEP 1: Generate Dataset
# ============================================================

print("=== DATA SCIENCE PIPELINE WITH ARROW AND PANDAS ===\n")

def generate_ml_dataset(num_records: int) -> pa.Table:
    """Generate dataset for ML training."""
    
    data = {
        "customer_id": [f"CUST-{i:06d}" for i in range(1, num_records + 1)],
        "age": [random.randint(18, 80) for _ in range(num_records)],
        "income": [round(random.uniform(20000, 200000), 2) for _ in range(num_records)],
        "account_balance": [round(random.uniform(1000, 500000), 2) for _ in range(num_records)],
        "num_transactions": [random.randint(1, 100) for _ in range(num_records)],
        "avg_transaction_amount": [round(random.uniform(100, 10000), 2) for _ in range(num_records)],
        "credit_score": [random.randint(300, 850) for _ in range(num_records)],
        "is_high_risk": [random.choice([0, 1]) for _ in range(num_records)],
    }
    
    return pa.table(data)

# Generate 1 million records
print("Generating 1 million records...")
arrow_table = generate_ml_dataset(1000000)
print(f"Generated: {len(arrow_table):,} records")

# ============================================================
# STEP 2: Convert to Pandas
# ============================================================

print("\n--- Converting to Pandas ---")

# Convert Arrow to Pandas
start_time = time.time()
df = arrow_table.to_pandas()
conversion_time = time.time() - start_time

print(f"\nConversion:")
print(f"  Arrow → Pandas: {conversion_time:.3f} seconds")
print(f"  DataFrame shape: {df.shape}")
print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 3: Data Analysis with Pandas
# ============================================================

print("\n--- Data Analysis with Pandas ---")

# Basic statistics
start_time = time.time()
stats = df.describe()
analysis_time = time.time() - start_time

print(f"\nBasic Statistics:")
print(stats.to_string())
print(f"\n  Analysis completed in {analysis_time:.3f} seconds")

# Group by risk
start_time = time.time()
risk_analysis = df.groupby("is_high_risk").agg({
    "income": "mean",
    "account_balance": "mean",
    "credit_score": "mean",
    "num_transactions": "mean"
}).round(2)
group_time = time.time() - start_time

print(f"\nRisk Analysis:")
print(risk_analysis.to_string())
print(f"\n  Group analysis completed in {group_time:.3f} seconds")

# ============================================================
# STEP 4: Feature Engineering
# ============================================================

print("\n--- Feature Engineering ---")

start_time = time.time()

# Create new features
df["income_per_age"] = df["income"] / df["age"]
df["balance_to_income"] = df["account_balance"] / df["income"]
df["transaction_intensity"] = df["num_transactions"] * df["avg_transaction_amount"]

# Create risk categories
df["risk_category"] = pd.cut(
    df["credit_score"],
    bins=[0, 580, 670, 740, 850],
    labels=["Poor", "Fair", "Good", "Excellent"]
)

feature_time = time.time() - start_time

print(f"\nFeature Engineering:")
print(f"  New features created: 4")
print(f"  Time: {feature_time:.3f} seconds")

# ============================================================
# STEP 5: Convert Back to Arrow
# ============================================================

print("\n--- Converting Back to Arrow ---")

# Convert Pandas to Arrow
start_time = time.time()
result_table = pa.Table.from_pandas(df)
back_conversion_time = time.time() - start_time

print(f"\nConversion:")
print(f"  Pandas → Arrow: {back_conversion_time:.3f} seconds")
print(f"  Arrow table size: {result_table.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 6: Save to Parquet
# ============================================================

print("\n--- Saving to Parquet ---")

# Save to Parquet
start_time = time.time()
pq.write_table(result_table, "/tmp/ml_features.parquet", compression="snappy")
save_time = time.time() - start_time

import os
file_size = os.path.getsize("/tmp/ml_features.parquet")

print(f"\nParquet Write:")
print(f"  Time: {save_time:.3f} seconds")
print(f"  File size: {file_size / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 7: Performance Comparison
# ============================================================

print("\n--- Performance Comparison ---")

# Compare Arrow vs Pandas operations
import pyarrow.compute as pc

# Arrow aggregation
start_time = time.time()
arrow_result = arrow_table.group_by("is_high_risk").aggregate({
    "income": "mean",
    "account_balance": "mean"
})
arrow_agg_time = time.time() - start_time

# Pandas aggregation
start_time = time.time()
pandas_result = df.groupby("is_high_risk")[["income", "account_balance"]].mean()
pandas_agg_time = time.time() - start_time

print(f"\nAggregation Comparison:")
print(f"  Arrow: {arrow_agg_time:.3f} seconds")
print(f"  Pandas: {pandas_agg_time:.3f} seconds")
print(f"  Arrow is {pandas_agg_time / arrow_agg_time:.1f}x faster")

# ============================================================
# STEP 8: Benefits Summary
# ============================================================

print("\n--- Benefits Summary ---")

print("""
ARROW + PANDAS BENEFITS:

1. ZERO-COPY CONVERSION
   - No data duplication
   - Fast conversion
   - Memory efficient

2. BEST OF BOTH WORLDS
   - Arrow: Fast I/O, memory efficiency
   - Pandas: Rich analysis, ML support

3. SEAMLESS INTEGRATION
   - Direct conversion
   - Schema preservation
   - Type compatibility

4. PERFORMANCE
   - Fast Arrow operations
   - Rich Pandas ecosystem
   - Optimized workflows

5. USE CASES
   - Data science pipelines
   - ML feature engineering
   - Data analysis
   - ETL transformations

WORKFLOW:
  Parquet → Arrow → Pandas → Analysis → Arrow → Parquet
""")
```

---

## 🏦 Real-World Banking Scenario 2: Reporting Dashboard

### Scenario
A bank's **reporting team** needs to:
- Load transaction data
- Generate reports
- Create visualizations
- Update dashboards

### Problem
- Large datasets
- Real-time updates
- Performance requirements

### Solution
Arrow + Pandas integration:
- Fast data loading
- Efficient processing
- Real-time updates

### Python Code

```python
"""
Banking Scenario 2: Reporting Dashboard
Using Arrow and Pandas
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import random
from datetime import datetime, timedelta
import time

# ============================================================
# STEP 1: Generate Transaction Data
# ============================================================

print("=== REPORTING DASHBOARD WITH ARROW AND PANDAS ===\n")

def generate_transaction_data(num_records: int) -> pa.Table:
    """Generate transaction data for reporting."""
    
    base_date = datetime(2026, 1, 1)
    
    data = {
        "transaction_id": [f"TXN-{i:010d}" for i in range(1, num_records + 1)],
        "date": [(base_date + timedelta(days=random.randint(0, 365))).date() for _ in range(num_records)],
        "branch": [random.choice(["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata"]) for _ in range(num_records)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_records)],
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(num_records)],
        "status": [random.choice(["COMPLETED", "PENDING", "FAILED"]) for _ in range(num_records)],
    }
    
    return pa.table(data)

# Generate 5 million transactions
print("Generating 5 million transactions...")
arrow_table = generate_transaction_data(5000000)
print(f"Generated: {len(arrow_table):,} transactions")

# ============================================================
# STEP 2: Convert to Pandas for Analysis
# ============================================================

print("\n--- Converting to Pandas ---")

start_time = time.time()
df = arrow_table.to_pandas()
conversion_time = time.time() - start_time

print(f"\nConversion: {conversion_time:.3f} seconds")

# ============================================================
# STEP 3: Generate Reports
# ============================================================

print("\n--- Generating Reports ---")

# Report 1: Daily Transaction Summary
start_time = time.time()
daily_summary = df.groupby("date").agg({
    "transaction_id": "count",
    "amount": ["sum", "mean"]
}).round(2)
report1_time = time.time() - start_time

print(f"\nReport 1: Daily Transaction Summary")
print(f"  Rows: {len(daily_summary)}")
print(f"  Time: {report1_time:.3f} seconds")
print(f"\nSample:")
print(daily_summary.head().to_string())

# Report 2: Branch Performance
start_time = time.time()
branch_performance = df.groupby("branch").agg({
    "transaction_id": "count",
    "amount": ["sum", "mean", "max"]
}).round(2)
report2_time = time.time() - start_time

print(f"\nReport 2: Branch Performance")
print(f"  Rows: {len(branch_performance)}")
print(f"  Time: {report2_time:.3f} seconds")
print(f"\nSample:")
print(branch_performance.to_string())

# Report 3: Transaction Type Analysis
start_time = time.time()
type_analysis = df.groupby("transaction_type").agg({
    "transaction_id": "count",
    "amount": ["sum", "mean"]
}).round(2)
report3_time = time.time() - start_time

print(f"\nReport 3: Transaction Type Analysis")
print(f"  Rows: {len(type_analysis)}")
print(f"  Time: {report3_time:.3f} seconds")
print(f"\nSample:")
print(type_analysis.to_string())

# ============================================================
# STEP 4: Create Visualizations Data
# ============================================================

print("\n--- Creating Visualizations Data ---")

start_time = time.time()

# Prepare data for charts
chart_data = {
    "daily_trend": daily_summary.reset_index().to_dict("records"),
    "branch_comparison": branch_performance.reset_index().to_dict("records"),
    "type_distribution": type_analysis.reset_index().to_dict("records"),
}

viz_time = time.time() - start_time

print(f"\nVisualization Data:")
print(f"  Daily trend points: {len(chart_data['daily_trend'])}")
print(f"  Branch comparison points: {len(chart_data['branch_comparison'])}")
print(f"  Type distribution points: {len(chart_data['type_distribution'])}")
print(f"  Time: {viz_time:.3f} seconds")

# ============================================================
# STEP 5: Update Dashboard Data
# ============================================================ print("\n--- Updating Dashboard Data ---")

start_time = time.time()

# Save reports to Parquet for dashboard
pq.write_table(
    pa.Table.from_pandas(daily_summary.reset_index()),
    "/tmp/dashboard_daily.parquet"
)

pq.write_table(
    pa.Table.from_pandas(branch_performance.reset_index()),
    "/tmp/dashboard_branch.parquet"
)

pq.write_table(
    pa.Table.from_pandas(type_analysis.reset_index()),
    "/tmp/dashboard_type.parquet"
)

dashboard_time = time.time() - start_time

print(f"\nDashboard Update:")
print(f"  Reports saved: 3")
print(f"  Time: {dashboard_time:.3f} seconds")

# ============================================================
# STEP 6: Performance Summary
# =================================================

print("\n--- Performance Summary ---")

print("""
REPORTING DASHBOARD PERFORMANCE:

Dataset: 5 million transactions

Operations:
  - Conversion: {conversion_time:.3f} seconds
  - Report 1 (Daily): {report1_time:.3f} seconds
  - Report 2 (Branch): {report2_time:.3f} seconds
  - Report 3 (Type): {report3_time:.3f} seconds
  - Visualization: {viz_time:.3f} seconds
  - Dashboard Update: {dashboard_time:.3f} seconds

Total Time: {total_time:.3f} seconds

PERFORMANCE CHARACTERISTICS:
  ✓ Fast Arrow → Pandas conversion
  ✓ Efficient Pandas aggregations
  ✓ Quick Parquet writes
  ✓ Real-time capable

USE CASES:
  ✓ Real-time dashboards
  ✓ Daily reports
  ✓ Branch analytics
  ✓ Transaction monitoring
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: How do you convert between Pandas DataFrames and Arrow Tables?

**Answer:**

**Pandas to Arrow:**
```python
import pandas as pd
import pyarrow as pa

df = pd.DataFrame({"a": [1, 2, 3]})
table = pa.Table.from_pandas(df)
```

**Arrow to Pandas:**
```python
df = table.to_pandas()
```

**Zero-Copy:**
```python
# Zero-copy conversion
df = table.to_pandas(timestamp_as_object=True)
```

---

### Question 2: What are the performance benefits of using Arrow with Pandas?

**Answer:**

**Performance Benefits:**

1. **Zero-Copy Conversion:**
   - No data duplication
   - Fast conversion
   - Memory efficient

2. **Arrow Operations:**
   - Fast aggregations
   - Vectorized operations
   - Cache-efficient

3. **Parquet I/O:**
   - Fast reads/writes
   - Compression
   - Predicate pushdown

**Example:**
```python
# Arrow aggregation (fast)
result = table.group_by("category").aggregate({"amount": "sum"})

# Pandas aggregation (slower)
result = df.groupby("category")["amount"].sum()
```

---

### Question 3: When would you use Arrow vs Pandas?

**Answer:**

**Use Arrow When:**
- Reading/writing Parquet
- Data interchange
- Memory efficiency critical
- Large datasets

**Use Pandas When:**
- Data analysis
- Feature engineering
- ML preprocessing
- Rich operations

**Best Practice:**
```
Parquet → Arrow → Pandas → Analysis → Arrow → Parquet
```

---

### Question 4: How do you handle data type differences between Arrow and Pandas?

**Answer:**

**Type Mapping:**

| Arrow Type | Pandas Type |
|------------|-------------|
| int64 | int64 |
| float64 | float64 |
| string | object |
| date32 | datetime64 |
| timestamp | datetime64 |

**Example:**
```python
import pyarrow as pa
import pandas as pd

# Arrow table with date
table = pa.table({"date": [pa.scalar("2026-08-24").cast(pa.date32())]})

# Convert to Pandas
df = table.to_pandas()
# date column becomes datetime64
```

---

### Question 5: How do you optimize memory when using Arrow and Pandas together?

**Answer:**

**Memory Optimization:**

1. **Use Zero-Copy:**
```python
df = table.to_pandas(timestamp_as_object=True)
```

2. **Process in Batches:**
```python
for batch in table.to_batches(max_chunksize=10000):
    df = batch.to_pandas()
    process(df)
```

3. **Use Appropriate Types:**
```python
# Use smallest appropriate type
df["category"] = df["category"].astype("category")
```

4. **Release Memory:**
```python
del df
import gc
gc.collect()
```

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Seamless integration for data processing |
| **Conversion** | Zero-copy, fast |
| **Benefits** | Best of both worlds |
| **Use Cases** | Data science, reporting, ML |
| **Performance** | Fast I/O, efficient processing |
| **Memory** | Zero-copy, batch processing |
