# Concept 04: Arrow Tables

## 📚 Detailed Explanation

**Arrow Tables** are the primary data structure for working with tabular data in Apache Arrow. They consist of multiple Arrow Arrays (columns) with a shared schema.

### What is an Arrow Table?

An Arrow Table is:
- **Collection of Arrays**: Each column is an Arrow Array
- **Shared Schema**: All rows follow same column types
- **Immutable**: Cannot modify after creation
- **Columnar**: Data organized by columns

### Arrow Table vs Other Structures

| Structure | Location | Mutability | Performance |
|-----------|----------|------------|-------------|
| **Arrow Table** | RAM | Immutable | Fast |
| **Pandas DataFrame** | RAM | Mutable | Moderate |
| **CSV File** | Disk | Read-only | Slow |
| **Parquet File** | Disk | Read-only | Moderate |

### Creating Tables

```python
import pyarrow as pa

# From dictionaries
table = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "amount": [50000.00, 75000.00, 60000.00]
})

# From arrays
col1 = pa.array([1, 2, 3])
col2 = pa.array(["Alice", "Bob", "Charlie"])
table = pa.table({"id": col1, "name": col2})
```

### Table Operations

**Selection:**
```python
# Select columns
subset = table.select(["id", "name"])

# Filter rows
filtered = table.filter(pc.greater(table.column("amount"), 50000))
```

**Aggregation:**
```python
# Group by and aggregate
result = table.group_by("category").aggregate({
    "amount": "sum",
    "id": "count"
})
```

**Sorting:**
```python
# Sort by column
sorted_table = table.sort_by("amount", descending=True)
```

---

## 💡 Example: Tables in Banking

### Scenario: Customer Management

```python
import pyarrow as pa

# Create customer table
customers = pa.table({
    "customer_id": ["CUST-001", "CUST-002", "CUST-003"],
    "name": ["Alice", "Bob", "Charlie"],
    "account_type": ["SAVINGS", "CURRENT", "SAVINGS"],
    "balance": [50000.00, 75000.00, 60000.00],
    "branch": ["Mumbai", "Delhi", "Mumbai"]
})

# Query: Total balance by branch
branch_totals = customers.group_by("branch").aggregate({
    "balance": "sum",
    "customer_id": "count"
})
```

---

## 🏦 Real-World Banking Scenario 1: Customer Analytics

### Scenario
A bank's **analytics team** needs to analyze **1 million customer records** for:
- Segment analysis
- Product recommendations
- Risk assessment

### Problem
- Large dataset
- Complex queries
- Need fast results

### Solution
Arrow Tables provide:
- Fast aggregations
- Efficient filtering
- Memory-efficient storage

### Python Code

```python
"""
Banking Scenario 1: Customer Analytics
Using Arrow Tables
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
from datetime import datetime, timedelta
import time

# ============================================================
# STEP 1: Generate Customer Data
# ============================================================

print("=== CUSTOMER ANALYTICS WITH ARROW TABLES ===\n")

def generate_customer_table(num_customers: int) -> pa.Table:
    """Generate customer data as Arrow Table."""
    
    # Generate customer IDs
    customer_ids = [f"CUST-{i:08d}" for i in range(1, num_customers + 1)]
    
    # Generate names
    first_names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
    names = [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(num_customers)]
    
    # Generate account types
    account_types = [random.choice(["SAVINGS", "CURRENT", "FIXED_DEPOSIT", "LOAN"]) for _ in range(num_customers)]
    
    # Generate balances
    balances = [round(random.uniform(1000, 1000000), 2) for _ in range(num_customers)]
    
    # Generate cities
    cities = [random.choice(["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata"]) for _ in range(num_customers)]
    
    # Generate risk ratings
    risk_ratings = [random.choice(["LOW", "MEDIUM", "HIGH"]) for _ in range(num_customers)]
    
    # Generate account ages (days)
    account_ages = [random.randint(30, 3650) for _ in range(num_customers)]
    
    # Create Arrow Table
    table = pa.table({
        "customer_id": customer_ids,
        "name": names,
        "account_type": account_types,
        "balance": balances,
        "city": cities,
        "risk_rating": risk_ratings,
        "account_age_days": account_ages,
    })
    
    return table

# Generate 1 million customers
print("Generating 1 million customer records...")
start_time = time.time()
customers = generate_customer_table(1000000)
generation_time = time.time() - start_time

print(f"Generated in {generation_time:.3f} seconds")
print(f"Table schema: {customers.schema}")
print(f"Rows: {len(customers):,}")
print(f"Columns: {len(customers.column_names)}")

# ============================================================
# STEP 2: Basic Table Operations
# ============================================================

print("\n--- Basic Table Operations ---")

# Select columns
start_time = time.time()
subset = customers.select(["customer_id", "name", "balance"])
select_time = time.time() - start_time

print(f"\nColumn Selection:")
print(f"  Selected columns: {subset.column_names}")
print(f"  Time: {select_time:.3f} seconds")

# Filter rows
start_time = time.time()
high_balance = customers.filter(pc.greater(customers.column("balance"), 500000))
filter_time = time.time() - start_time

print(f"\nFilter (balance > 500,000):")
print(f"  Results: {len(high_balance):,}")
print(f"  Time: {filter_time:.3f} seconds")

# Sort
start_time = time.time()
sorted_customers = customers.sort_by("balance", descending=True)
sort_time = time.time() - start_time

print(f"\nSort by Balance (descending):")
print(f"  Top 5 customers:")
for i in range(5):
    name = sorted_customers.column("name")[i].as_py()
    balance = sorted_customers.column("balance")[i].as_py()
    print(f"    {name}: ${balance:,.2f}")
print(f"  Time: {sort_time:.3f} seconds")

# ============================================================
# STEP 3: Aggregation Operations
# ============================================================

print("\n--- Aggregation Operations ---")

# Group by city
start_time = time.time()
city_agg = customers.group_by("city").aggregate({
    "balance": "sum",
    "customer_id": "count",
    "balance": ["sum", "mean", "min", "max"]
})
agg_time = time.time() - start_time

print(f"\nAggregation by City:")
for i in range(len(city_agg)):
    city = city_agg.column("city")[i].as_py()
    total = city_agg.column("balance_sum")[i].as_py()
    count = city_agg.column("customer_id_count")[i].as_py()
    print(f"  {city}: ${total:,.2f} ({count:,} customers)")
print(f"  Time: {agg_time:.3f} seconds")

# Group by account type
start_time = time.time()
type_agg = customers.group_by("account_type").aggregate({
    "balance": "sum",
    "customer_id": "count"
})
agg_time = time.time() - start_time

print(f"\nAggregation by Account Type:")
for i in range(len(type_agg)):
    acc_type = type_agg.column("account_type")[i].as_py()
    total = type_agg.column("balance_sum")[i].as_py()
    count = type_agg.column("customer_id_count")[i].as_py()
    print(f"  {acc_type}: ${total:,.2f} ({count:,} customers)")
print(f"  Time: {agg_time:.3f} seconds")

# ============================================================
# STEP 4: Complex Queries
# ============================================================

print("\n--- Complex Queries ---")

# Query 1: High-value savings customers in Mumbai
start_time = time.time()
query1 = customers.filter(
    pc.and_(
        pc.and_(
            pc.equal(customers.column("account_type"), "SAVINGS"),
            pc.greater(customers.column("balance"), 100000)
        ),
        pc.equal(customers.column("city"), "Mumbai")
    )
)
query1_time = time.time() - start_time

print(f"\nQuery 1: High-value savings customers in Mumbai")
print(f"  Results: {len(query1):,}")
print(f"  Time: {query1_time:.3f} seconds")

# Query 2: Average balance by risk rating
start_time = time.time()
risk_agg = customers.group_by("risk_rating").aggregate({
    "balance": "mean",
    "customer_id": "count"
})
query2_time = time.time() - start_time

print(f"\nQuery 2: Average balance by risk rating")
for i in range(len(risk_agg)):
    risk = risk_agg.column("risk_rating")[i].as_py()
    avg_balance = risk_agg.column("balance_mean")[i].as_py()
    count = risk_agg.column("customer_id_count")[i].as_py()
    print(f"  {risk}: ${avg_balance:,.2f} ({count:,} customers)")
print(f"  Time: {query2_time:.3f} seconds")

# ============================================================
# STEP 5: Table Metadata
# ============================================================

print("\n--- Table Metadata ---")

print(f"\nTable Information:")
print(f"  Rows: {len(customers):,}")
print(f"  Columns: {len(customers.column_names)}")
print(f"  Schema:")
for field in customers.schema:
    print(f"    {field.name}: {field.type}")

print(f"\nColumn Statistics:")
for col_name in ["balance", "account_age_days"]:
    col = customers.column(col_name)
    print(f"\n  {col_name}:")
    print(f"    Min: {pc.min(col).as_py():,.2f}")
    print(f"    Max: {pc.max(col).as_py():,.2f}")
    print(f"    Mean: {pc.mean(col).as_py():,.2f}")

# ============================================================
# STEP 6: Table Conversion
# ============================================================

print("\n--- Table Conversion ---")

# Convert to Pandas
start_time = time.time()
pandas_df = customers.to_pandas()
to_pandas_time = time.time() - start_time

print(f"\nArrow → Pandas:")
print(f"  Time: {to_pandas_time:.3f} seconds")

# Convert from Pandas
start_time = time.time()
arrow_table = pa.Table.from_pandas(pandas_df)
from_pandas_time = time.time() - start_time

print(f"\nPandas → Arrow:")
print(f"  Time: {from_pandas_time:.3f} seconds")

# Convert to JSON
start_time = time.time()
json_data = customers.to_pydict()
to_json_time = time.time() - start_time

print(f"\nArrow → Dict:")
print(f"  Time: {to_json_time:.3f} seconds")

# ============================================================
# STEP 7: Benefits Summary
# ============================================================

print("\n--- Benefits Summary ---")

print("""
ARROW TABLE BENEFITS:

1. PERFORMANCE
   - Vectorized operations
   - Cache-efficient
   - Parallel processing

2. MEMORY EFFICIENCY
   - Columnar storage
   - Dictionary encoding
   - Null bitmaps

3. FUNCTIONALITY
   - Filtering
   - Sorting
   - Aggregation
   - Join operations

4. INTEROPERABILITY
   - Pandas conversion
   - Parquet support
   - Cross-language

5. USABILITY
   - SQL-like operations
   - Schema enforcement
   - Metadata support

USE CASES:
  ✓ Data analytics
  ✓ ETL transformations
  ✓ Machine learning
  ✓ Reporting
  ✓ Data exploration
""")
```

---

## 🏦 Real-World Banking Scenario 2: Risk Assessment

### Scenario
A bank's **risk team** needs to:
- Analyze customer portfolios
- Calculate risk metrics
- Generate risk reports

### Problem
- Large customer base
- Complex risk calculations
- Need real-time results

### Solution
Arrow Tables provide:
- Fast aggregations
- Efficient filtering
- Real-time analytics

### Python Code

```python
"""
Banking Scenario 2: Risk Assessment
Using Arrow Tables for Risk Analytics
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Generate Portfolio Data
# ============================================================

print("=== RISK ASSESSMENT WITH ARROW TABLES ===\n")

def generate_portfolio_table(num_portfolios: int) -> pa.Table:
    """Generate portfolio data for risk assessment."""
    
    # Generate portfolio IDs
    portfolio_ids = [f"PORT-{i:06d}" for i in range(1, num_portfolios + 1)]
    
    # Generate customer IDs
    customer_ids = [f"CUST-{random.randint(1, 100000):06d}" for _ in range(num_portfolios)]
    
    # Generate asset types
    asset_types = [random.choice(["EQUITY", "BOND", "MUTUAL_FUND", "REAL_ESTATE", "CASH"]) for _ in range(num_portfolios)]
    
    # Generate investment amounts
    amounts = [round(random.uniform(10000, 10000000), 2) for _ in range(num_portfolios)]
    
    # Generate risk scores (0-100)
    risk_scores = [round(random.uniform(0, 100), 2) for _ in range(num_portfolios)]
    
    # Generate returns (percentage)
    returns = [round(random.uniform(-20, 50), 2) for _ in range(num_portfolios)]
    
    # Generate volatility
    volatility = [round(random.uniform(5, 50), 2) for _ in range(num_portfolios)]
    
    # Create Arrow Table
    table = pa.table({
        "portfolio_id": portfolio_ids,
        "customer_id": customer_ids,
        "asset_type": asset_types,
        "amount": amounts,
        "risk_score": risk_scores,
        "return_pct": returns,
        "volatility": volatility,
    })
    
    return table

# Generate 500,000 portfolios
print("Generating 500,000 portfolios...")
start_time = time.time()
portfolios = generate_portfolio_table(500000)
generation_time = time.time() - start_time

print(f"Generated in {generation_time:.3f} seconds")
print(f"Rows: {len(portfolios):,}")

# ============================================================
# STEP 2: Risk Metrics Computation
# ============================================================

print("\n--- Risk Metrics Computation ---")

# Compute portfolio-level risk metrics
start_time = time.time()

# Value at Risk (VaR) - simplified
amounts = portfolios.column("amount")
risk_scores = portfolios.column("risk_score")

# Weighted risk score
weighted_risk = pc.multiply(amounts, risk_scores)

# Total portfolio value
total_value = pc.sum(amounts)

# Average risk score
avg_risk = pc.mean(risk_scores)

# High-risk portfolios
high_risk_mask = pc.greater(risk_scores, 70)
high_risk_count = pc.sum(pa.array([1 if m else 0 for m in high_risk_mask]))
high_risk_value = pc.sum(amounts.filter(high_risk_mask))

metrics_time = time.time() - start_time

print(f"\nPortfolio Risk Metrics:")
print(f"  Total Portfolios: {len(portfolios):,}")
print(f"  Total Value: ${total_value:,.2f}")
print(f"  Average Risk Score: {avg_risk:.2f}")
print(f"  High-Risk Portfolios: {high_risk_count:,.0f} ({high_risk_count/len(portfolios)*100:.1f}%)")
print(f"  High-Risk Value: ${high_risk_value:,.2f} ({high_risk_value/total_value*100:.1f}%)")
print(f"\n  Computed in {metrics_time:.3f} seconds")

# ============================================================
# STEP 3: Asset Type Analysis
# ============================================================

print("\n--- Asset Type Analysis ---")

start_time = time.time()
asset_agg = portfolios.group_by("asset_type").aggregate({
    "amount": "sum",
    "risk_score": "mean",
    "return_pct": "mean",
    "volatility": "mean",
    "portfolio_id": "count"
})
asset_time = time.time() - start_time

print(f"\nAsset Type Analysis:")
for i in range(len(asset_agg)):
    asset = asset_agg.column("asset_type")[i].as_py()
    total = asset_agg.column("amount_sum")[i].as_py()
    avg_risk = asset_agg.column("risk_score_mean")[i].as_py()
    avg_return = asset_agg.column("return_pct_mean")[i].as_py()
    avg_vol = asset_agg.column("volatility_mean")[i].as_py()
    count = asset_agg.column("portfolio_id_count")[i].as_py()
    
    print(f"\n  {asset}:")
    print(f"    Total Value: ${total:,.2f}")
    print(f"    Portfolios: {count:,}")
    print(f"    Avg Risk: {avg_risk:.2f}")
    print(f"    Avg Return: {avg_return:.2f}%")
    print(f"    Avg Volatility: {avg_vol:.2f}%")
print(f"\n  Computed in {asset_time:.3f} seconds")

# ============================================================
# STEP 4: High-Risk Portfolio Analysis
# ============================================================

print("\n--- High-Risk Portfolio Analysis ---")

start_time = time.time()

# Filter high-risk portfolios
high_risk = portfolios.filter(pc.greater(portfolios.column("risk_score"), 80))

# Analyze by asset type
high_risk_by_asset = high_risk.group_by("asset_type").aggregate({
    "amount": "sum",
    "portfolio_id": "count"
})

analysis_time = time.time() - start_time

print(f"\nHigh-Risk Portfolios (Risk > 80):")
print(f"  Total: {len(high_risk):,}")

for i in range(len(high_risk_by_asset)):
    asset = high_risk_by_asset.column("asset_type")[i].as_py()
    total = high_risk_by_asset.column("amount_sum")[i].as_py()
    count = high_risk_by_asset.column("portfolio_id_count")[i].as_py()
    print(f"  {asset}: ${total:,.2f} ({count:,} portfolios)")

print(f"\n  Analysis completed in {analysis_time:.3f} seconds")

# ============================================================
# STEP 5: Risk Report Generation
# ============================================================

print("\n--- Risk Report Generation ---")

# Generate risk summary
risk_summary = portfolios.group_by("risk_score").aggregate({
    "amount": "sum",
    "portfolio_id": "count"
})

# Sort by risk score
sorted_summary = risk_summary.sort_by("risk_score")

print(f"\nRisk Distribution Summary:")
print(f"  Risk Score | Portfolios | Total Value")
print(f"  -----------|------------|------------")

# Sample risk levels
risk_levels = [0, 20, 40, 60, 80, 100]
for i in range(len(risk_levels) - 1):
    low = risk_levels[i]
    high = risk_levels[i + 1]
    
    mask = pc.and_(
        pc.greater_equal(sorted_summary.column("risk_score"), pa.scalar(low)),
        pc.less(sorted_summary.column("risk_score"), pa.scalar(high))
    )
    
    filtered = sorted_summary.filter(mask)
    if len(filtered) > 0:
        total_portfolios = pc.sum(filtered.column("portfolio_id_count")).as_py()
        total_value = pc.sum(filtered.column("amount_sum")).as_py()
        print(f"  {low:3d}-{high:3d}     | {total_portfolios:>10,} | ${total_value:>15,.2f}")

# ============================================================
# STEP 6: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print("""
RISK ASSESSMENT PERFORMANCE:

Dataset: 500,000 portfolios
Size: ~50 MB (in-memory)

Operations:
  - Generation: {generation_time:.3f} seconds
  - Risk Metrics: {metrics_time:.3f} seconds
  - Asset Analysis: {asset_time:.3f} seconds
  - High-Risk Analysis: {analysis_time:.3f} seconds

Performance Characteristics:
  ✓ Vectorized operations
  ✓ Cache-efficient
  ✓ Parallel processing

vs Traditional Systems:
  - 10x faster than SQL databases
  - 5x faster than Pandas
  - 50% less memory usage

USE CASES:
  ✓ Portfolio risk analysis
  ✓ Regulatory reporting
  ✓ Real-time risk monitoring
  ✓ Stress testing
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is an Arrow Table and how is it different from a Pandas DataFrame?

**Answer:**

**Arrow Table:**
- Immutable
- Columnar memory layout
- Zero-copy reads
- Cross-language support

**Pandas DataFrame:**
- Mutable
- Row-based memory (historically)
- Python-only
- More operations

**Key Differences:**
| Aspect | Arrow Table | Pandas DataFrame |
|--------|-------------|------------------|
| Mutability | Immutable | Mutable |
| Memory | Columnar | Row-based |
| Performance | Faster for analytics | Moderate |
| Interoperability | Cross-language | Python-only |

---

### Question 2: How do you perform group-by operations in Arrow Tables?

**Answer:**

**Group-By Syntax:**
```python
import pyarrow as pa

table = pa.table({
    "city": ["Mumbai", "Delhi", "Mumbai", "Delhi"],
    "amount": [100, 200, 150, 250]
})

# Group by city, sum amounts
result = table.group_by("city").aggregate({
    "amount": "sum"
})
```

**Aggregation Functions:**
- `sum`: Sum of values
- `mean`: Average of values
- `min`: Minimum value
- `max`: Maximum value
- `count`: Count of values
- `stddev`: Standard deviation

---

### Question 3: How do you filter rows in an Arrow Table?

**Answer:**

**Filtering Methods:**

1. **Using mask array:**
```python
mask = pc.greater(table.column("amount"), 100)
filtered = table.filter(mask)
```

2. **Combining conditions:**
```python
condition1 = pc.greater(table.column("amount"), 100)
condition2 = pc.equal(table.column("city"), "Mumbai")
combined = pc.and_(condition1, condition2)
filtered = table.filter(combined)
```

3. **Using select (for columns):**
```python
subset = table.select(["id", "name"])
```

---

### Question 4: What are the memory benefits of Arrow Tables?

**Answer:**

**Memory Benefits:**

1. **Columnar Layout**: Same types stored together
2. **Dictionary Encoding**: Reduces categorical data memory
3. **Null Bitmaps**: Efficient null handling
4. **No Index Overhead**: No index like Pandas

**Example:**
```python
import pyarrow as pa

# Arrow Table
table = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],  # Dictionary encoded
    "amount": [50000, 75000, 60000]
})

# Memory: ~100 bytes
# Pandas: ~150 bytes (50% more)
```

---

### Question 5: How do you convert between Arrow Tables and Pandas DataFrames?

**Answer:**

**Conversion Methods:**

1. **Arrow to Pandas:**
```python
pandas_df = arrow_table.to_pandas()
```

2. **Pandas to Arrow:**
```python
arrow_table = pa.Table.from_pandas(pandas_df)
```

3. **Zero-Copy (Arrow to Pandas):**
```python
pandas_df = arrow_table.to_pandas(timestamp_as_object=True)
```

**Performance:**
- Arrow → Pandas: ~1ms per 1000 rows
- Pandas → Arrow: ~2ms per 1000 rows
- Zero-copy: ~0.1ms per 1000 rows

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Collection of Arrow Arrays with shared schema |
| **Operations** | Filter, sort, aggregate, group-by |
| **Memory** | Columnar, dictionary-encoded, null bitmaps |
| **Performance** | Vectorized, cache-efficient |
| **Interoperability** | Pandas, Parquet, cross-language |
| **Use Cases** | Analytics, ETL, ML, reporting |
