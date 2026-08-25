# Concept 08: Arrow Compute Functions

## 📚 Detailed Explanation

**Arrow Compute Functions** provide vectorized operations for efficient data processing. They leverage SIMD instructions and cache-efficient memory layout for high performance.

### What are Compute Functions?

Compute functions are:
- **Vectorized**: Process multiple values simultaneously
- **SIMD-Optimized**: Use CPU vector instructions
- **Null-Aware**: Handle missing values automatically
- **Type-Safe**: Enforce type consistency

### Compute Function Categories

| Category | Functions | Description |
|----------|-----------|-------------|
| **Arithmetic** | add, subtract, multiply, divide | Math operations |
| **Comparison** | equal, greater, less, etc. | Comparisons |
| **Logical** | and, or, not, xor | Logical operations |
| **String** | utf8_upper, utf8_lower, etc. | String operations |
| **Temporal** | year, month, day, etc. | Date/time operations |
| **Aggregation** | sum, mean, min, max, etc. | Aggregate values |
| **Statistical** | stddev, variance, etc. | Statistics |

### Using Compute Functions

```python
import pyarrow as pa
import pyarrow.compute as pc

# Create array
array = pa.array([1, 2, 3, 4, 5])

# Arithmetic
result = pc.add(array, 10)  # [11, 12, 13, 14, 15]

# Comparison
mask = pc.greater(array, 3)  # [False, False, False, True, True]

# Aggregation
total = pc.sum(array)  # 15
mean = pc.mean(array)  # 3.0
```

---

## 💡 Example: Compute Functions in Banking

### Scenario: Transaction Analysis

```python
import pyarrow as pa
import pyarrow.compute as pc

# Transaction amounts
amounts = pa.array([50000, 75000, 60000, 80000, 55000])

# Compute statistics
total = pc.sum(amounts)
average = pc.mean(amounts)
max_amount = pc.max(amounts)

# Apply discount
discounted = pc.multiply(amounts, pa.scalar(0.9))

# Filter high-value
high_value = amounts.filter(pc.greater(amounts, 60000))
```

---

## 🏦 Real-World Banking Scenario 1: Financial Calculations

### Scenario
A bank needs to:
- Calculate interest
- Apply fees
- Compute taxes
- Generate statements

### Problem
- High volume calculations
- Precision requirements
- Performance needs

### Solution
Arrow compute functions provide:
- Vectorized operations
- Decimal precision
- Fast processing

### Python Code

```python
"""
Banking Scenario 1: Financial Calculations
Using Arrow Compute Functions
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Generate Account Data
# ============================================================

print("=== FINANCIAL CALCULATIONS WITH ARROW COMPUTE ===\n")

def generate_account_data(num_accounts: int) -> pa.Table:
    """Generate account data for calculations."""
    
    data = {
        "account_id": [f"ACC-{i:06d}" for i in range(1, num_accounts + 1)],
        "balance": [round(random.uniform(1000, 1000000), 2) for _ in range(num_accounts)],
        "interest_rate": [round(random.uniform(0.02, 0.08), 4) for _ in range(num_accounts)],
        "fee_rate": [round(random.uniform(0.001, 0.01), 4) for _ in range(num_accounts)],
        "tax_rate": [round(random.uniform(0.1, 0.3), 4) for _ in range(num_accounts)],
    }
    
    return pa.table(data)

# Generate 1 million accounts
print("Generating 1 million accounts...")
accounts = generate_account_data(1000000)
print(f"Generated: {len(accounts):,} accounts")

# ============================================================
# STEP 2: Interest Calculation
# ============================================================

print("\n--- Interest Calculation ---")

balances = accounts.column("balance")
interest_rates = accounts.column("interest_rate")

# Calculate interest (vectorized)
start_time = time.time()
interest = pc.multiply(balances, interest_rates)
interest_time = time.time() - start_time

print(f"\nInterest Calculation:")
print(f"  Total interest: ${pc.sum(interest):,.2f}")
print(f"  Average interest: ${pc.mean(interest):,.2f}")
print(f"  Time: {interest_time:.3f} seconds")

# ============================================================
# STEP 3: Fee Calculation
# ============================================================

print("\n--- Fee Calculation ---")

fee_rates = accounts.column("fee_rate")

# Calculate fees (vectorized)
start_time = time.time()
fees = pc.multiply(balances, fee_rates)
fee_time = time.time() - start_time

print(f"\nFee Calculation:")
print(f"  Total fees: ${pc.sum(fees):,.2f}")
print(f"  Average fee: ${pc.mean(fees):,.2f}")
print(f"  Time: {fee_time:.3f} seconds")

# ============================================================
# STEP 4: Tax Calculation
# ============================================================

print("\n--- Tax Calculation ---")

tax_rates = accounts.column("tax_rate")

# Calculate tax on interest (vectorized)
start_time = time.time()
tax = pc.multiply(interest, tax_rates)
tax_time = time.time() - start_time

print(f"\nTax Calculation:")
print(f"  Total tax: ${pc.sum(tax):,.2f}")
print(f"  Average tax: ${pc.mean(tax):,.2f}")
print(f"  Time: {tax_time:.3f} seconds")

# ============================================================
# STEP 5: Net Amount Calculation
# ============================================================

print("\n--- Net Amount Calculation ---")

# Calculate net amount (vectorized)
start_time = time.time()
net_amount = pc.add(pc.subtract(balances, fees), pc.subtract(interest, tax))
net_time = time.time() - start_time

print(f"\nNet Amount Calculation:")
print(f"  Total net amount: ${pc.sum(net_amount):,.2f}")
print(f"  Average net amount: ${pc.mean(net_amount):,.2f}")
print(f"  Time: {net_time:.3f} seconds")

# ============================================================
# STEP 6: Statistical Analysis
# ============================================================

print("\n--- Statistical Analysis ---")

start_time = time.time()

# Balance statistics
balance_stats = {
    "min": pc.min(balances).as_py(),
    "max": pc.max(balances).as_py(),
    "mean": pc.mean(balances).as_py(),
    "std": pc.stddev(balances).as_py(),
    "median": pc.median(balances).as_py(),
}

# Interest statistics
interest_stats = {
    "min": pc.min(interest).as_py(),
    "max": pc.max(interest).as_py(),
    "mean": pc.mean(interest).as_py(),
    "std": pc.stddev(interest).as_py(),
}

stats_time = time.time() - start_time

print(f"\nBalance Statistics:")
for stat, value in balance_stats.items():
    print(f"  {stat}: ${value:,.2f}")

print(f"\nInterest Statistics:")
for stat, value in interest_stats.items():
    print(f"  {stat}: ${value:,.2f}")

print(f"\n  Statistics computed in {stats_time:.3f} seconds")

# ============================================================
# STEP 7: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print("""
FINANCIAL CALCULATIONS PERFORMANCE:

Dataset: 1 million accounts

Operations:
  - Interest: {interest_time:.3f} seconds
  - Fees: {fee_time:.3f} seconds
  - Tax: {tax_time:.3f} seconds
  - Net Amount: {net_time:.3f} seconds
  - Statistics: {stats_time:.3f} seconds

Total Time: {total_time:.3f} seconds

Performance Characteristics:
  ✓ Vectorized operations (SIMD)
  ✓ Parallel processing
  ✓ Cache-efficient
  ✓ Null-aware

vs Traditional Methods:
  - 10-100x faster than loops
  - 50% less memory
  - Better precision
""")
```

---

## 🏦 Real-World Banking Scenario 2: Risk Analysis

### Scenario
A bank's **risk team** needs to:
- Calculate portfolio risk
- Compute Value at Risk (VaR)
- Analyze volatility

### Problem
- Complex calculations
- Large datasets
- Real-time requirements

### Solution
Arrow compute functions provide:
- Vectorized risk calculations
- Fast aggregations
- Statistical functions

### Python Code

```python
"""
Banking Scenario 2: Risk Analysis
Using Arrow Compute Functions
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Generate Portfolio Data
# ============================================================

print("=== RISK ANALYSIS WITH ARROW COMPUTE ===\n")

def generate_portfolio_data(num_portfolios: int) -> pa.Table:
    """Generate portfolio data for risk analysis."""
    
    data = {
        "portfolio_id": [f"PORT-{i:06d}" for i in range(1, num_portfolios + 1)],
        "value": [round(random.uniform(100000, 10000000), 2) for _ in range(num_portfolios)],
        "returns": [round(random.uniform(-0.2, 0.3), 4) for _ in range(num_portfolios)],
        "volatility": [round(random.uniform(0.05, 0.5), 4) for _ in range(num_portfolios)],
        "beta": [round(random.uniform(0.5, 2.0), 4) for _ in range(num_portfolios)],
        "sharpe_ratio": [round(random.uniform(0.5, 3.0), 4) for _ in range(num_portfolios)],
    }
    
    return pa.table(data)

# Generate 500,000 portfolios
print("Generating 500,000 portfolios...")
portfolios = generate_portfolio_data(500000)
print(f"Generated: {len(portfolios):,} portfolios")

# ============================================================
# STEP 2: Value at Risk (VaR) Calculation
# ============================================================

print("\n--- Value at Risk (VaR) Calculation ---")

values = portfolios.column("value")
returns = portfolios.column("returns")
volatilities = portfolios.column("volatility")

# Calculate VaR (95% confidence)
start_time = time.time()

# VaR = Portfolio Value × Volatility × Z-score (1.645 for 95%)
z_score = 1.645
var_95 = pc.multiply(pc.multiply(values, volatilities), pa.scalar(z_score))

var_time = time.time() - start_time

print(f"\nValue at Risk (95% confidence):")
print(f"  Total VaR: ${pc.sum(var_95):,.2f}")
print(f"  Average VaR: ${pc.mean(var_95):,.2f}")
print(f"  Max VaR: ${pc.max(var_95):,.2f}")
print(f"\n  Calculated in {var_time:.3f} seconds")

# ============================================================
# STEP 3: Portfolio Performance Metrics
# ============================================================

print("\n--- Portfolio Performance Metrics ---")

start_time = time.time()

# Calculate performance metrics
sharpe_ratios = portfolios.column("sharpe_ratio")
betas = portfolios.column("beta")

# High Sharpe ratio portfolios
high_sharpe_mask = pc.greater(sharpe_ratios, pa.scalar(2.0))
high_sharpe_count = pc.sum(pa.array([1 if m else 0 for m in high_sharpe_mask]))

# High beta portfolios (more volatile)
high_beta_mask = pc.greater(betas, pa.scalar(1.5))
high_beta_count = pc.sum(pa.array([1 if m else 0 for m in high_beta_mask]))

metrics_time = time.time() - start_time

print(f"\nPortfolio Metrics:")
print(f"  High Sharpe ratio (>2.0): {high_sharpe_count:,.0f} portfolios")
print(f"  High beta (>1.5): {high_beta_count:,.0f} portfolios")
print(f"  Average Sharpe ratio: {pc.mean(sharpe_ratios):.4f}")
print(f"  Average beta: {pc.mean(betas):.4f}")
print(f"\n  Metrics computed in {metrics_time:.3f} seconds")

# ============================================================
# STEP 4: Risk Segmentation
# ============================================================

print("\n--- Risk Segmentation ---")

start_time = time.time()

# Segment by volatility
low_vol_mask = pc.less(volatilities, pa.scalar(0.2))
medium_vol_mask = pc.and_(pc.greater_equal(volatilities, pa.scalar(0.2)), 
                          pc.less(volatilities, pa.scalar(0.35)))
high_vol_mask = pc.greater_equal(volatilities, pa.scalar(0.35))

low_vol_count = pc.sum(pa.array([1 if m else 0 for m in low_vol_mask]))
medium_vol_count = pc.sum(pa.array([1 if m else 0 for m in medium_vol_mask]))
high_vol_count = pc.sum(pa.array([1 if m else 0 for m in high_vol_mask]))

segmentation_time = time.time() - start_time

print(f"\nRisk Segmentation:")
print(f"  Low Volatility (<20%): {low_vol_count:,.0f} portfolios")
print(f"  Medium Volatility (20-35%): {medium_vol_count:,.0f} portfolios")
print(f"  High Volatility (>35%): {high_vol_count:,.0f} portfolios")
print(f"\n  Segmentation completed in {segmentation_time:.3f} seconds")

# ============================================================
# STEP 5: Correlation Analysis
# ============================================================

print("\n--- Correlation Analysis ---")

start_time = time.time()

# Calculate correlation between returns and volatility
# (Simplified - in production use proper correlation)
returns_mean = pc.mean(returns)
volatilities_mean = pc.mean(volatilities)

# Covariance (simplified)
covariance = pc.mean(pc.multiply(
    pc.subtract(returns, returns_mean),
    pc.subtract(volatilities, volatilities_mean)
))

correlation_time = time.time() - start_time

print(f"\nCorrelation Analysis:")
print(f"  Returns mean: {returns_mean.as_py():.4f}")
print(f"  Volatility mean: {volatilities_mean.as_py():.4f}")
print(f"  Covariance: {covariance.as_py():.6f}")
print(f"\n  Analysis completed in {correlation_time:.3f} seconds")

# ============================================================
# STEP 6: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print("""
RISK ANALYSIS PERFORMANCE:

Dataset: 500,000 portfolios

Operations:
  - VaR Calculation: {var_time:.3f} seconds
  - Performance Metrics: {metrics_time:.3f} seconds
  - Risk Segmentation: {segmentation_time:.3f} seconds
  - Correlation: {correlation_time:.3f} seconds

Total Time: {total_time:.3f} seconds

Performance Characteristics:
  ✓ Vectorized risk calculations
  ✓ Fast aggregations
  ✓ Statistical functions
  ✓ Real-time capable

USE CASES:
  ✓ Portfolio risk analysis
  ✓ Regulatory reporting
  ✓ Stress testing
  ✓ Real-time monitoring
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What are Arrow compute functions and why are they important?

**Answer:**

**Arrow Compute Functions:**
- Vectorized operations
- SIMD-optimized
- Null-aware
- Type-safe

**Importance:**
1. **Performance**: 10-100x faster than loops
2. **Memory Efficiency**: No intermediate arrays
3. **Null Handling**: Automatic skip
4. **Type Safety**: Guaranteed types

**Example:**
```python
import pyarrow as pa
import pyarrow.compute as pc

array = pa.array([1, 2, 3, 4, 5])
result = pc.add(array, 10)  # Vectorized
```

---

### Question 2: How do Arrow compute functions achieve high performance?

**Answer:**

**Performance Mechanisms:**

1. **SIMD Instructions:**
   - Process multiple values simultaneously
   - CPU vector instructions
   - Parallel computation

2. **Cache Efficiency:**
   - Sequential memory access
   - Predictable patterns
   - Prefetch optimization

3. **Null Skipping:**
   - Skip null values
   - No conditional checks
   - Efficient processing

**Example:**
```python
# SIMD processes 8 values at once (AVX2)
# [1, 2, 3, 4, 5, 6, 7, 8] + [10, 10, 10, 10, 10, 10, 10, 10]
# = [11, 12, 13, 14, 15, 16, 17, 18]
```

---

### Question 3: What compute functions are available in Arrow?

**Answer:**

**Function Categories:**

1. **Arithmetic:**
```python
pc.add(a, b)      # Addition
pc.subtract(a, b)  # Subtraction
pc.multiply(a, b)  # Multiplication
pc.divide(a, b)    # Division
```

2. **Comparison:**
```python
pc.equal(a, b)
pc.greater(a, b)
pc.less(a, b)
pc.and_(a, b)
```

3. **Aggregation:**
```python
pc.sum(array)
pc.mean(array)
pc.min(array)
pc.max(array)
pc.stddev(array)
```

4. **String:**
```python
pc.utf8_upper(array)
pc.utf8_lower(array)
pc.utf8_length(array)
```

---

### Question 4: How do you handle null values in compute functions?

**Answer:**

**Null Handling:**

1. **Automatic Skipping:**
```python
array = pa.array([1, None, 3, None, 5])
result = pc.sum(array)  # Returns 9 (ignores nulls)
```

2. **Null-Aware Operations:**
```python
# Result is null if any input is null
result = pc.add(pa.array([1, None]), pa.array([2, 3]))
# Returns [3, None]
```

3. **Fill Nulls:**
```python
filled = pc.fill_null(array, 0)  # Replace nulls with 0
```

---

### Question 5: How do compute functions integrate with Arrow Tables?

**Answer:**

**Table Integration:**

1. **Column Operations:**
```python
table = pa.table({"amount": [100, 200, 300]})
discounted = pc.multiply(table.column("amount"), pa.scalar(0.9))
table = table.append_column("discounted", discounted)
```

2. **Aggregations:**
```python
result = table.group_by("category").aggregate({
    "amount": "sum"
})
```

3. **Filtering:**
```python
mask = pc.greater(table.column("amount"), 150)
filtered = table.filter(mask)
```

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Vectorized operations for Arrow data |
| **Categories** | Arithmetic, Comparison, Aggregation, String |
| **Performance** | SIMD-optimized, cache-efficient |
| **Null Handling** | Automatic skip, null-aware |
| **Use Cases** | Financial calculations, Risk analysis |
| **vs Loops** | 10-100x faster |
