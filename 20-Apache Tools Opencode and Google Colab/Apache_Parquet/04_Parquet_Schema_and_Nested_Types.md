# Parquet Schema & Nested Types

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-loan-application-document)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-customer-360-profile)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### What is a Parquet Schema?

A Parquet schema defines the **structure, data types, and organization** of columns in a Parquet file. Unlike CSV (which has no schema), Parquet embeds its schema directly in the file footer.

The schema supports three structural types:

```
Parquet Schema Types
    |
    +-- Primitives (leaf nodes)
    |     +-- INT32, INT64, INT96
    |     +-- FLOAT, DOUBLE
    |     +-- BOOLEAN
    |     +-- BYTE_ARRAY (string, binary)
    |     +-- FIXED_LEN_BYTE_ARRAY
    |     +-- DATE, TIMESTAMP, TIME
    |
    +-- Nested Types (group nodes)
          +-- STRUCT (record)
          +-- LIST (repeated group)
          +-- MAP (key-value)
```

### Primitive Types

| Parquet Type | Python Type | Description | Example |
|-------------|-------------|-------------|---------|
| INT32 | int | 32-bit integer | age, count |
| INT64 | int | 64-bit integer | transaction_id, amount_cents |
| FLOAT | float | 32-bit float | exchange_rate |
| DOUBLE | float | 64-bit float | amount, balance |
| BOOLEAN | bool | True/False | is_active, is_fraud |
| BYTE_ARRAY | str/bytes | Variable-length bytes | name, email, status |
| FIXED_LEN_BYTE_ARRAY | bytes | Fixed-length bytes | UUID (16 bytes) |
| DATE | date | Days since epoch | transaction_date |
| TIMESTAMP | datetime | Microsecond precision | created_at |
| DECIMAL | Decimal | Arbitrary precision | amount (financial) |

### Nested Types

#### STRUCT (Record/Group)

A struct groups related columns together:

```json
{
  "name": "address",
  "type": "struct",
  "fields": [
    {"name": "street", "type": "string"},
    {"name": "city", "type": "string"},
    {"name": "state", "type": "string"},
    {"name": "zip", "type": "string"}
  ]
}
```

**Flat equivalent:**
```
address_street, address_city, address_state, address_zip
```

#### LIST

A list stores multiple values of the same type:

```json
{
  "name": "phone_numbers",
  "type": "list",
  "element": {"type": "string"}
}
```

Example: `["555-1234", "555-5678", "555-9012"]`

#### MAP

A map stores key-value pairs:

```json
{
  "name": "account_balances",
  "type": "map",
  "key": {"type": "string"},
  "value": {"type": "double"}
}
```

Example: `{"checking": 15000.00, "savings": 50000.00, "investment": 120000.00}`

### How Parquet Stores Nested Data

Parquet uses a **Dremel encoding** (repetition and definition levels) to store nested data in a flat columnar format:

```
Record 1: {name: "John", phones: ["555-1234", "555-5678"]}
Record 2: {name: "Jane", phones: []}
Record 3: {name: "Bob", phones: ["555-9012"]}

Storage:
  name:          ["John", "Jane", "Bob"]
  phone_numbers: ["555-1234", "555-5678", "555-9012"]
  rep_level:     [1, 2, 1]          ← 1=new record, 2=continuation
  def_level:     [1, 1, 0]          ← 1=present, 0=null/empty
```

### Schema Evolution in Parquet

Parquet supports limited schema evolution:

1. **Add columns**: New columns appear as NULLs in old files
2. **Widen types**: INT32 → INT64, FLOAT → DOUBLE
3. **Reorder columns**: Physical order doesn't affect logical reads

**Cannot:**
- Remove columns (they persist in old files)
- Rename columns (name is embedded in the file)
- Change types arbitrarily (INT → STRING not supported)

### Schema Best Practices

```
DO:
  + Use descriptive, consistent column names
  + Use appropriate types (DATE not STRING for dates)
  + Use DECIMAL for financial amounts (not FLOAT)
  + Flatten nested structures when possible
  + Use INT32 for small numbers, INT64 for large

DON'T:
  - Use STRING for dates/numbers
  - Use FLOAT for financial amounts (precision loss)
  - Create deeply nested structures (>3 levels)
  - Use overly generic names (col1, col2)
```

---

## 2. Example

### Flat Schema
```python
schema = pa.schema([
    ("transaction_id", pa.int64()),
    ("account_id", pa.string()),
    ("amount", pa.decimal128(18, 2)),    # 18 digits, 2 decimal places
    ("currency", pa.string()),
    ("transaction_date", pa.date32()),
    ("is_fraud", pa.bool_()),
])
```

### Nested Schema
```python
schema = pa.schema([
    ("transaction_id", pa.int64()),
    ("account", pa.struct([
        ("account_id", pa.string()),
        ("account_type", pa.string()),
        ("branch_id", pa.string()),
    ])),
    ("amount", pa.struct([
        ("value", pa.decimal128(18, 2)),
        ("currency", pa.string()),
        ("usd_equivalent", pa.decimal128(18, 2)),
    ])),
    ("merchant", pa.struct([
        ("name", pa.string()),
        ("category", pa.string()),
        ("country", pa.string()),
    ])),
    ("tags", pa.list_(pa.string())),
])
```

---

## 3. Banking Scenario 1: Loan Application Document

### Problem
A bank's loan origination system captures complex loan applications with deeply nested data:
- Applicant information (name, address, employment, income)
- Co-applicant (optional)
- Loan details (type, amount, term, rate)
- Property information (for mortgage)
- Documents checklist (list of uploaded documents)
- Credit check results (multiple bureau scores)

The data must be stored in Parquet for the analytics team to build risk models.

### Why Schema Design Matters?
- Nested structures map naturally to the application JSON
- Co-applicant is optional → nullable struct
- Documents are variable-length → list type
- Credit scores from multiple bureaus → list of structs
- Financial amounts must use DECIMAL (not FLOAT) for precision

### Architecture
```
Loan Origination System
       |
       v
  Application JSON
       |
       v
  Schema Mapping (JSON → Parquet)
       |
       v
  Parquet Files (nested schema)
       |
       v
  Risk Analytics (DuckDB / Spark)
       |
       v
  Loan Decision Engine
```

---

## 4. Python Code - Scenario 1

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datetime import datetime, date
import random
import os
import tempfile
from decimal import Decimal

# ============================================================
# BANKING SCENARIO: Loan Application Document Storage
# ============================================================

def create_loan_application_schema():
    """Define the Parquet schema for loan applications."""
    return pa.schema([
        # Application metadata
        ("application_id", pa.int64()),
        ("application_date", pa.date32()),
        ("loan_type", pa.string()),          # MORTGAGE, PERSONAL, AUTO, BUSINESS
        ("status", pa.string()),             # PENDING, APPROVED, DENIED, WITHDRAWN

        # Primary applicant (struct)
        ("applicant", pa.struct([
            ("first_name", pa.string()),
            ("last_name", pa.string()),
            ("date_of_birth", pa.date32()),
            ("ssn_last4", pa.string()),
            ("phone", pa.string()),
            ("email", pa.string()),
            ("address", pa.struct([
                ("street", pa.string()),
                ("city", pa.string()),
                ("state", pa.string()),
                ("zip_code", pa.string()),
                ("country", pa.string()),
            ])),
            ("employment", pa.struct([
                ("employer", pa.string()),
                ("job_title", pa.string()),
                ("years_employed", pa.int32()),
                ("annual_income", pa.decimal128(12, 2)),
                ("income_type", pa.string()),   # WAGE, SELF_EMPLOYED, RETIRED
            ])),
        ])),

        # Co-applicant (nullable struct)
        ("co_applicant", pa.struct([
            ("first_name", pa.string()),
            ("last_name", pa.string()),
            ("annual_income", pa.decimal128(12, 2)),
            ("relationship", pa.string()),      # SPOUSE, PARTNER, CO_BORROWER
        ])),

        # Loan details
        ("loan_amount", pa.decimal128(14, 2)),
        ("loan_term_months", pa.int32()),
        ("interest_rate", pa.decimal128(6, 4)),
        ("monthly_payment", pa.decimal128(12, 2)),

        # Property (for mortgage, nullable)
        ("property", pa.struct([
            ("address", pa.string()),
            ("city", pa.string()),
            ("state", pa.string()),
            ("property_type", pa.string()),
            ("appraised_value", pa.decimal128(14, 2)),
            ("year_built", pa.int32()),
        ])),

        # Credit scores (list of structs)
        ("credit_scores", pa.list_(pa.struct([
            ("bureau", pa.string()),        # EQUIFAX, TRANSUNION, EXPERIAN
            ("score", pa.int32()),
            ("report_date", pa.date32()),
        ]))),

        # Supporting documents (list of strings)
        ("documents", pa.list_(pa.string())),
    ])


def generate_loan_applications(num_applications=1000):
    """Generate realistic loan application data."""
    random.seed(42)

    loan_types = ["MORTGAGE", "PERSONAL", "AUTO", "BUSINESS"]
    statuses = ["PENDING", "APPROVED", "DENIED", "WITHDRAWN"]
    states = ["NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
    bureaus = ["EQUIFAX", "TRANSUNION", "EXPERIAN"]
    income_types = ["WAGE", "SELF_EMPLOYED", "RETIRED"]
    doc_types = ["ID_PROOF", "INCOME_TAX_RETURN", "BANK_STATEMENT", "EMPLOYMENT_LETTER",
                 "PROPERTY_APPRAISAL", "DEED", "INSURANCE", "PAY_STUB"]

    applications = []

    for i in range(num_applications):
        # Primary applicant
        applicant_income = Decimal(str(random.uniform(30000, 250000))).quantize(Decimal("0.01"))
        loan_amount = Decimal(str(random.uniform(10000, 1000000))).quantize(Decimal("0.01"))
        interest_rate = Decimal(str(random.uniform(3.0, 12.0))).quantize(Decimal("0.0001"))
        loan_term = random.choice([12, 24, 36, 60, 120, 180, 240, 360])
        monthly_payment = (loan_amount * (1 + interest_rate / 100) / loan_term).quantize(Decimal("0.01"))

        # Co-applicant (60% chance)
        co_applicant = None
        if random.random() < 0.6:
            co_applicant = {
                "first_name": f"CoFirst{i}",
                "last_name": f"CoLast{i}",
                "annual_income": Decimal(str(random.uniform(30000, 150000))).quantize(Decimal("0.01")),
                "relationship": random.choice(["SPOUSE", "PARTNER", "CO_BORROWER"]),
            }

        # Property (for mortgage)
        prop = None
        loan_type = random.choice(loan_types)
        if loan_type == "MORTGAGE":
            prop = {
                "address": f"{random.randint(100, 9999)} Main St",
                "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
                "state": random.choice(states),
                "property_type": random.choice(["SINGLE_FAMILY", "CONDO", "TOWNHOUSE", "MULTI_FAMILY"]),
                "appraised_value": Decimal(str(random.uniform(150000, 800000))).quantize(Decimal("0.01")),
                "year_built": random.randint(1950, 2025),
            }

        # Credit scores
        credit_scores = []
        base_score = random.randint(580, 850)
        for bureau in bureaus:
            credit_scores.append({
                "bureau": bureau,
                "score": base_score + random.randint(-20, 20),
                "report_date": date(2026, 8, random.randint(1, 24)),
            })

        # Documents (random subset)
        num_docs = random.randint(2, len(doc_types))
        documents = random.sample(doc_types, num_docs)

        app = {
            "application_id": i + 1,
            "application_date": date(2026, random.randint(1, 8), random.randint(1, 28)),
            "loan_type": loan_type,
            "status": random.choice(statuses),
            "applicant": {
                "first_name": f"First{i}",
                "last_name": f"Last{i}",
                "date_of_birth": date(random.randint(1960, 2000), random.randint(1, 12), random.randint(1, 28)),
                "ssn_last4": f"{random.randint(1000, 9999)}",
                "phone": f"555-{random.randint(1000, 9999)}",
                "email": f"applicant{i}@email.com",
                "address": {
                    "street": f"{random.randint(100, 9999)} Oak Ave",
                    "city": random.choice(["New York", "Los Angeles", "Chicago"]),
                    "state": random.choice(states),
                    "zip_code": f"{random.randint(10000, 99999)}",
                    "country": "US",
                },
                "employment": {
                    "employer": random.choice(["Google", "Apple", "Microsoft", "JPMorgan", "Self-Employed"]),
                    "job_title": random.choice(["Engineer", "Manager", "Director", "Analyst", "Consultant"]),
                    "years_employed": random.randint(0, 30),
                    "annual_income": applicant_income,
                    "income_type": random.choice(income_types),
                },
            },
            "co_applicant": co_applicant,
            "loan_amount": loan_amount,
            "loan_term_months": loan_term,
            "interest_rate": interest_rate,
            "monthly_payment": monthly_payment,
            "property": prop,
            "credit_scores": credit_scores,
            "documents": documents,
        }
        applications.append(app)

    return applications


def applications_to_arrow(applications):
    """Convert loan applications to Arrow table with nested schema."""
    # Convert to Arrow using PyArrow's automatic conversion
    table = pa.Table.from_pylist(applications, schema=create_loan_application_schema())
    return table


def store_loan_applications(table, base_path):
    """Store loan applications in Parquet."""
    path = os.path.join(base_path, "loan_applications.parquet")

    pq.write_table(
        table,
        path,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
    )

    size = os.path.getsize(path)
    print(f"Stored {table.num_rows:,} loan applications")
    print(f"Schema fields: {len(table.schema)}")
    print(f"Nested fields: applicant (struct), co_applicant (struct), credit_scores (list), documents (list)")
    print(f"File size: {size / (1024*1024):.1f} MB")


def query_nested_data(base_path):
    """Query nested data using PyArrow's nested field access."""
    table = pq.read_table(base_path)

    # Query 1: Average income by employment type
    print("\n=== Average Income by Employment Type ===")
    df = table.to_pandas()
    # Access nested field
    incomes = []
    emp_types = []
    for row in df.itertuples():
        incomes.append(float(row.applicant["employment"]["annual_income"]))
        emp_types.append(row.applicant["employment"]["income_type"])

    import pandas as pd
    result = pd.DataFrame({"income_type": emp_types, "income": incomes})
    print(result.groupby("income_type")["income"].mean().to_string())

    # Query 2: Average credit score by bureau
    print("\n=== Average Credit Score by Bureau ===")
    all_scores = []
    for row in df.itertuples():
        for score_entry in row.credit_scores:
            all_scores.append({
                "bureau": score_entry["bureau"],
                "score": score_entry["score"],
            })

    scores_df = pd.DataFrame(all_scores)
    print(scores_df.groupby("bureau")["score"].mean().to_string())

    # Query 3: Loan amount distribution by type
    print("\n=== Loan Amount Distribution ===")
    print(df.groupby("loan_type")["loan_amount"].describe().to_string())

    # Query 4: Document frequency
    print("\n=== Most Common Documents ===")
    all_docs = []
    for row in df.itertuples():
        all_docs.extend(row.documents)
    doc_counts = pd.Series(all_docs).value_counts()
    print(doc_counts.to_string())


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "loan_applications")
    os.makedirs(base_path, exist_ok=True)

    # Generate loan applications
    print("Generating loan application data...")
    applications = generate_loan_applications(num_applications=1000)

    # Convert to Arrow and store
    table = applications_to_arrow(applications)
    store_loan_applications(table, base_path)

    # Query nested data
    query_nested_data(base_path)

    # Print schema
    print(f"\n=== Parquet Schema ===")
    print(table.schema)
```

---

## 5. Banking Scenario 2: Customer 360 Profile

### Problem
A bank wants a **Customer 360** view that consolidates all customer information into a single Parquet dataset:
- Personal info (name, DOB, address)
- Multiple accounts (checking, savings, credit card, loan)
- Transaction summaries per account
- Risk profile (credit score, risk rating)
- Communication preferences (list of channels)
- Family relationships (list of related customers)

This data feeds into ML models for churn prediction and personalized offers.

### Why Schema Design Matters?
- One customer → multiple accounts → natural LIST of STRUCTs
- Each account → transaction summary → nested STRUCT
- Communication preferences → LIST of strings
- Family relationships → LIST of STRUCTs with back-references
- Must be queryable by both analysts and ML pipelines

### Architecture
```
Core Banking System
       |
       v
  Customer Data Platform
       |
       v
  Parquet Files (nested Customer 360 schema)
       |
       +-- DuckDB (analyst queries)
       +-- Spark (ML feature engineering)
       +-- BI Dashboard
```

---

## 6. Python Code - Scenario 2

```python
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date
import random
import os
import tempfile
from decimal import Decimal

# ============================================================
# BANKING SCENARIO: Customer 360 Profile
# ============================================================

def create_customer360_schema():
    """Define the Customer 360 Parquet schema."""
    return pa.schema([
        ("customer_id", pa.int64()),
        ("customer_segment", pa.string()),      # PREMIUM, STANDARD, BASIC

        # Personal info (struct)
        ("personal_info", pa.struct([
            ("first_name", pa.string()),
            ("last_name", pa.string()),
            ("date_of_birth", pa.date32()),
            ("email", pa.string()),
            ("phone", pa.string()),
            ("address", pa.struct([
                ("street", pa.string()),
                ("city", pa.string()),
                ("state", pa.string()),
                ("zip_code", pa.string()),
            ])),
        ])),

        # Accounts (list of structs)
        ("accounts", pa.list_(pa.struct([
            ("account_id", pa.string()),
            ("account_type", pa.string()),
            ("balance", pa.decimal128(14, 2)),
            ("opened_date", pa.date32()),
            ("status", pa.string()),
            ("monthly_transactions", pa.int32()),
            ("avg_monthly_balance", pa.decimal128(14, 2)),
        ]))),

        # Risk profile (struct)
        ("risk_profile", pa.struct([
            ("credit_score", pa.int32()),
            ("risk_rating", pa.string()),      # LOW, MEDIUM, HIGH
            ("credit_limit", pa.decimal128(14, 2)),
            ("total_debt", pa.decimal128(14, 2)),
            ("debt_to_income_ratio", pa.decimal128(6, 4)),
        ])),

        # Communication preferences (list of strings)
        ("preferences", pa.list_(pa.string())),

        # Family relationships (list of structs)
        ("family", pa.list_(pa.struct([
            ("related_customer_id", pa.int64()),
            ("relationship", pa.string()),    # SPOUSE, CHILD, PARENT
        ]))),
    ])


def generate_customer360_data(num_customers=500):
    """Generate Customer 360 data."""
    random.seed(42)

    segments = ["PREMIUM", "STANDARD", "BASIC"]
    account_types = ["CHECKING", "SAVINGS", "CREDIT_CARD", "MORTGAGE", "AUTO_LOAN"]
    risk_ratings = ["LOW", "MEDIUM", "HIGH"]
    preferences = ["EMAIL", "SMS", "PUSH_NOTIFICATION", "PHONE", "MAIL", "IN_APP"]
    relationships = ["SPOUSE", "CHILD", "PARENT", "SIBLING"]

    customers = []

    for i in range(num_customers):
        # Generate accounts (1-5 per customer)
        num_accounts = random.randint(1, 5)
        accounts = []
        for _ in range(num_accounts):
            acc_type = random.choice(account_types)
            accounts.append({
                "account_id": f"ACC{random.randint(1000000, 9999999)}",
                "account_type": acc_type,
                "balance": Decimal(str(random.uniform(100, 500000))).quantize(Decimal("0.01")),
                "opened_date": date(random.randint(2015, 2026), random.randint(1, 12), random.randint(1, 28)),
                "status": random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "DORMANT"]),
                "monthly_transactions": random.randint(0, 200),
                "avg_monthly_balance": Decimal(str(random.uniform(1000, 100000))).quantize(Decimal("0.01")),
            })

        # Risk profile
        credit_score = random.randint(580, 850)
        risk_rating = "LOW" if credit_score > 700 else ("MEDIUM" if credit_score > 600 else "HIGH")

        # Family relationships (0-3)
        family = []
        if random.random() < 0.7:
            num_family = random.randint(1, 3)
            for _ in range(num_family):
                family.append({
                    "related_customer_id": random.randint(1, num_customers * 10),
                    "relationship": random.choice(relationships),
                })

        # Communication preferences (1-3)
        num_prefs = random.randint(1, 3)
        prefs = random.sample(preferences, num_prefs)

        customer = {
            "customer_id": i + 1,
            "customer_segment": random.choice(segments),
            "personal_info": {
                "first_name": f"Customer{i}",
                "last_name": f"Family{i}",
                "date_of_birth": date(random.randint(1955, 2000), random.randint(1, 12), random.randint(1, 28)),
                "email": f"customer{i}@bank.com",
                "phone": f"555-{random.randint(1000, 9999)}",
                "address": {
                    "street": f"{random.randint(100, 9999)} Main St",
                    "city": random.choice(["New York", "Boston", "Chicago", "Miami", "Seattle"]),
                    "state": random.choice(["NY", "MA", "IL", "FL", "WA"]),
                    "zip_code": f"{random.randint(10000, 99999)}",
                },
            },
            "accounts": accounts,
            "risk_profile": {
                "credit_score": credit_score,
                "risk_rating": risk_rating,
                "credit_limit": Decimal(str(random.uniform(5000, 100000))).quantize(Decimal("0.01")),
                "total_debt": Decimal(str(random.uniform(0, 200000))).quantize(Decimal("0.01")),
                "debt_to_income_ratio": Decimal(str(random.uniform(0.05, 0.60))).quantize(Decimal("0.0001")),
            },
            "preferences": prefs,
            "family": family,
        }
        customers.append(customer)

    return customers


def store_customer360(customers, base_path):
    """Store Customer 360 data in Parquet."""
    table = pa.Table.from_pylist(customers, schema=create_customer360_schema())

    path = os.path.join(base_path, "customer_360.parquet")
    pq.write_table(
        table,
        path,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    )

    size = os.path.getsize(path)
    print(f"Stored {table.num_rows:,} customer profiles")
    print(f"Schema: {len(table.schema)} top-level fields")
    print(f"File size: {size / (1024*1024):.1f} MB")

    return table


def query_customer360(base_path):
    """Query Customer 360 with nested data access."""
    table = pq.read_table(base_path)
    df = table.to_pandas()

    # Query 1: Average credit score by segment
    print("\n=== Credit Score by Segment ===")
    scores = []
    segments = []
    for row in df.itertuples():
        segments.append(row.customer_segment)
        scores.append(row.risk_profile["credit_score"])

    import pandas as pd
    result = pd.DataFrame({"segment": segments, "credit_score": scores})
    print(result.groupby("segment")["credit_score"].mean().to_string())

    # Query 2: Average number of accounts per segment
    print("\n=== Accounts per Segment ===")
    acct_counts = []
    for row in df.itertuples():
        acct_counts.append({
            "segment": row.customer_segment,
            "num_accounts": len(row.accounts),
        })
    acct_df = pd.DataFrame(acct_counts)
    print(acct_df.groupby("segment")["num_accounts"].mean().to_string())

    # Query 3: Total balance across all accounts
    print("\n=== Total Balance Distribution ===")
    total_balances = []
    for row in df.itertuples():
        total = sum(float(acc["balance"]) for acc in row.accounts)
        total_balances.append(total)
    balance_df = pd.DataFrame({"segment": segments, "total_balance": total_balances})
    print(balance_df.groupby("segment")["total_balance"].agg(["mean", "median", "max"]).to_string())

    # Query 4: Communication preferences
    print("\n=== Communication Preferences ===")
    all_prefs = []
    for row in df.itertuples():
        all_prefs.extend(row.preferences)
    pref_counts = pd.Series(all_prefs).value_counts()
    print(pref_counts.to_string())


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "customer360")
    os.makedirs(base_path, exist_ok=True)

    # Generate and store Customer 360 data
    print("Generating Customer 360 data...")
    customers = generate_customer360_data(num_customers=500)
    table = store_customer360(customers, base_path)

    # Query the data
    query_customer360(base_path)

    # Print schema
    print(f"\n=== Full Schema ===")
    print(table.schema)
```

---

## 7. Interview Questions

### Q1: How does Parquet handle nested data structures?

**Answer:**
Parquet uses the **Dremel encoding** with **repetition and definition levels** to flatten nested structures into columnar storage:

**Repetition Level**: Indicates when a new value starts within a repeated field (list/map).
**Definition Level**: Indicates whether a value is present or null.

**Example**:
```json
{"name": "John", "phones": ["555-1234", "555-5678"]}
{"name": "Jane", "phones": []}
{"name": "Bob", "phones": ["555-9012"]}
```

Stored as:
```
name:          ["John", "Jane", "Bob"]
phones:        ["555-1234", "555-5678", "555-9012"]
repetition:    [1, 2, 1]       (1=new record, 2=list continuation)
definition:    [1, 1, 0]       (1=present, 0=empty list)
```

This allows efficient columnar storage while preserving nested structure.

---

### Q2: Why should you use DECIMAL instead of FLOAT for financial amounts in Parquet?

**Answer:**
FLOAT/DOUBLE uses IEEE 754 binary representation, which **cannot exactly represent decimal fractions**:

```
0.1 + 0.2 = 0.30000000000000004  (FLOAT/DOUBLE)
0.1 + 0.2 = 0.3                   (DECIMAL)
```

For a bank processing billions of transactions:
- rounding errors compound
- regulatory audits require exact amounts
- financial reconciliation breaks with floating-point imprecision

**DECIMAL in Parquet:**
```python
pa.decimal128(18, 2)  # 18 total digits, 2 decimal places
```

Stores exact values up to 10^16 with 2 decimal places — sufficient for any financial amount.

---

### Q3: What are the limitations of Parquet schema evolution?

**Answer:**

**Supported:**
- ✅ Add new columns (appear as NULLs in old files)
- ✅ Widen integer types (INT32 → INT64)
- ✅ Widen float types (FLOAT → DOUBLE)
- ✅ Reorder columns

**Not supported:**
- ❌ Remove columns (they persist in old files, wasting space)
- ❌ Rename columns (name is embedded in the file)
- ❌ Change types arbitrarily (INT → STRING, STRING → INT)
- ❌ Change decimal precision/scale
- ❌ Modify nested structures

**Workaround**: Use Iceberg or Delta Lake on top of Parquet, which manage schema evolution at the table level across files.

---

### Q4: How do LIST and MAP types differ in Parquet and when to use each?

**Answer:**

**LIST**: Ordered collection of same-type values
```python
pa.list_(pa.string())  # ["email", "sms", "push"]
```
Use for: phone numbers, tags, preferences, document lists

**MAP**: Key-value pairs
```python
pa.map_(pa.string(), pa.float64())  # {"checking": 15000, "savings": 50000}
```
Use for: account balances by type, feature dictionaries, metadata

**Key differences:**
- LIST allows duplicates, MAP does not (unique keys)
- MAP provides O(1) key lookup, LIST requires scan
- LIST is simpler to store and query

**Example**: A customer's communication preferences are a LIST (can have multiple "email" entries), while account balances by type are a MAP (unique account types).

---

### Q5: What is the Dremel encoding and why does Parquet use it?

**Answer:**
Dremel encoding (from Google's Dremel paper) flattens nested structures into columns using **repetition and definition levels**.

**How it works:**

For nested data:
```json
{"name": "A", "tags": ["x", "y"]}
{"name": "B", "tags": null}
{"name": "C", "tags": ["z"]}
```

Dremel encodes:
```
name:   [A, B, C]
tags:   [x, y, z]
rep:    [1, 2, 1, 1]   ← 1=new record, 2=list element
def:    [2, 1, 0, 1]   ← 2=fully defined, 1=list present, 0=null
```

**Why Parquet uses it:**
1. Maintains columnar storage benefits for nested data
2. Efficient compression (repetition/definition levels are small integers)
3. Enables column pruning even within nested structures
4. Allows random access to specific nested fields

Without Dremel, nested data would require row-based storage, losing Parquet's core advantage.
