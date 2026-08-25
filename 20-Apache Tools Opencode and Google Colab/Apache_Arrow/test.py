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
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
    ]
    names = [
        f"{random.choice(first_names)} {random.choice(last_names)}"
        for _ in range(num_customers)
    ]

    # Generate account types
    account_types = [
        random.choice(["SAVINGS", "CURRENT", "FIXED_DEPOSIT", "LOAN"])
        for _ in range(num_customers)
    ]

    # Generate balances
    balances = [round(random.uniform(1000, 1000000), 2) for _ in range(num_customers)]

    # Generate cities
    cities = [
        random.choice(["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata"])
        for _ in range(num_customers)
    ]

    # Generate risk ratings
    risk_ratings = [
        random.choice(["LOW", "MEDIUM", "HIGH"]) for _ in range(num_customers)
    ]

    # Generate account ages (days)
    account_ages = [random.randint(30, 3650) for _ in range(num_customers)]

    # Create Arrow Table
    table = pa.table(
        {
            "customer_id": customer_ids,
            "name": names,
            "account_type": account_types,
            "balance": balances,
            "city": cities,
            "risk_rating": risk_ratings,
            "account_age_days": account_ages,
        }
    )

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
city_agg = customers.group_by("city").aggregate(
    {"balance": "sum", "customer_id": "count", "balance": ["sum", "mean", "min", "max"]}
)
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
type_agg = customers.group_by("account_type").aggregate(
    {"balance": "sum", "customer_id": "count"}
)
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
            pc.greater(customers.column("balance"), 100000),
        ),
        pc.equal(customers.column("city"), "Mumbai"),
    )
)
query1_time = time.time() - start_time

print(f"\nQuery 1: High-value savings customers in Mumbai")
print(f"  Results: {len(query1):,}")
print(f"  Time: {query1_time:.3f} seconds")

# Query 2: Average balance by risk rating
start_time = time.time()
risk_agg = customers.group_by("risk_rating").aggregate(
    {"balance": "mean", "customer_id": "count"}
)
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
