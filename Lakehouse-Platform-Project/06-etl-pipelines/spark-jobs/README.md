# Spark Jobs - Banking Data Platform

## Overview

This folder contains **2 Apache Spark (PySpark) jobs** that handle large-scale data transformations for the banking data platform. These jobs process data between the **Medallion Architecture layers** (Bronze → Silver → Gold) using distributed computing.

---

## Job Summary

| # | Job Name | Purpose | Input Layer | Output Layer |
|---|----------|---------|-------------|--------------|
| 1 | `bronze-to-silver.py` | Clean, validate, and conform raw data | Bronze | Silver |
| 2 | `silver-to-gold.py` | Create business-ready aggregations | Silver | Gold |

---

## How the Jobs Fit in the Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE WITH SPARK JOBS                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                        SOURCE SYSTEMS                                 │ │
│  │   Oracle (Core Banking)  │  Mainframe (Cards)  │  SQL Server (Loans)  │ │
│  └───────────────────────────────────┬───────────────────────────────────┘ │
│                                      │                                      │
│                                      ▼                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  BRONZE LAYER (Raw Data)                        s3://banking-lake/    │ │
│  │  ─────────────────────                          /bronze/              │ │
│  │  • Raw data as-is from sources                                     │ │
│  │  • Schema-on-read                                                  │ │
│  │  • Append-only, partitioned by ingestion_date                      │ │
│  └───────────────────────────────────┬───────────────────────────────────┘ │
│                                      │                                      │
│                    ┌─────────────────┴─────────────────┐                    │
│                    │                                    │                    │
│                    ▼                                    ▼                    │
│  ┌─────────────────────────────┐    ┌─────────────────────────────────┐    │
│  │  SPARK JOB 1:               │    │  AIRFLOW DAG (Alternative):     │    │
│  │  bronze-to-silver.py        │    │  silver_transformation.py       │    │
│  │                             │    │                                 │    │
│  │  • PySpark standalone       │    │  • Airflow orchestrated         │    │
│  │  • Batch processing         │    │  • Task groups + scheduling     │    │
│  │  • CLI execution            │    │  • UI monitoring                │    │
│  └──────────────┬──────────────┘    └───────────────┬─────────────────┘    │
│                 │                                    │                      │
│                 ▼                                    ▼                      │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  SILVER LAYER (Cleansed Data)                   s3://banking-lake/    │ │
│  │  ─────────────────────────────                  /silver/              │ │
│  │  • Deduplicated, validated                                          │ │
│  │  • Schema enforced                                                 │ │
│  │  • Standardized formats                                            │ │
│  └───────────────────────────────────┬───────────────────────────────────┘ │
│                                      │                                      │
│                    ┌─────────────────┴─────────────────┐                    │
│                    │                                    │                    │
│                    ▼                                    ▼                    │
│  ┌─────────────────────────────┐    ┌─────────────────────────────────┐    │
│  │  SPARK JOB 2:               │    │  AIRFLOW DAG (Alternative):     │    │
│  │  silver-to-gold.py          │    │  gold_aggregation.py            │    │
│  │                             │    │                                 │    │
│  │  • Aggregations             │    │  • Business logic               │    │
│  │  • Customer 360             │    │  • Dremio reflection refresh    │    │
│  │  • Risk dashboards          │    │  • Scheduling                   │    │
│  └──────────────┬──────────────┘    └───────────────┬─────────────────┘    │
│                 │                                    │                      │
│                 ▼                                    ▼                      │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  GOLD LAYER (Business-Ready)                   s3://banking-lake/     │ │
│  │  ───────────────────────────                   /gold/                │ │
│  │  • Star schemas, aggregations                                       │ │
│  │  • Customer 360°, Daily Summary, Credit Risk                        │ │
│  │  • Ready for BI tools and Dremio                                    │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Spark Job 1: Bronze to Silver (`bronze-to-silver.py`)

### Purpose
Read raw data from the **Bronze layer**, clean it, validate it, apply business rules, and write standardized data to the **Silver layer**. This is the most critical transformation step where data quality is enforced.

### When to Use
- **Daily batch processing** (typically run overnight)
- When you need **fine-grained control** over Spark configuration
- When running outside of Airflow (standalone execution)
- For **large datasets** that benefit from Spark's distributed processing

### Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BRONZE TO SILVER - EXECUTION FLOW                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  COMMAND: spark-submit bronze-to-silver.py 2024-01-15                       │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 1: CREATE SPARK SESSION                                       │   │
│  │  ────────────────────────────                                       │   │
│  │  • App Name: "Bronze_to_Silver"                                     │   │
│  │  • Partition overwrite: dynamic                                     │   │
│  │  • Shuffle partitions: 200                                          │   │
│  │  • Adaptive query execution: enabled                                │   │
│  │  • Serializer: Kryo (fast)                                          │   │
│  └───────────────────────────────────┬─────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 2: CLEAN EACH SOURCE (5 parallel tasks)                       │   │
│  │  ─────────────────────────────────────────────                      │   │
│  │                                                                     │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐  │   │
│  │  │  CUSTOMERS  │ │  ACCOUNTS   │ │ TRANSACTIONS│ │    CARDS    │  │   │
│  │  │             │ │             │ │             │ │             │  │   │
│  │  │ Read Bronze │ │ Read Bronze │ │ Read Bronze │ │ Read Bronze │  │   │
│  │  │     │       │ │     │       │ │     │       │ │     │       │  │   │
│  │  │     ▼       │ │     ▼       │ │     ▼       │ │     ▼       │  │   │
│  │  │ Drop Dups   │ │ Drop Dups   │ │ Drop Dups   │ │ Drop Dups   │  │   │
│  │  │ Validate    │ │ Validate    │ │ Validate    │ │ Validate    │  │   │
│  │  │ Standardize │ │ Standardize │ │ Standardize │ │ Standardize │  │   │
│  │  │     │       │ │     │       │ │     │       │ │     │       │  │   │
│  │  │     ▼       │ │     ▼       │ │     ▼       │ │     ▼       │  │   │
│  │  │ Write Silver│ │ Write Silver│ │ Write Silver│ │ Write Silver│  │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘  │   │
│  │                                                                     │   │
│  │  ┌─────────────┐                                                    │   │
│  │  │    LOANS    │                                                    │   │
│  │  │             │                                                    │   │
│  │  │ Read Bronze │                                                    │   │
│  │  │     │       │                                                    │   │
│  │  │     ▼       │                                                    │   │
│  │  │ Drop Dups   │                                                    │   │
│  │  │ Validate    │                                                    │   │
│  │  │ Standardize │                                                    │   │
│  │  │     │       │                                                    │   │
│  │  │     ▼       │                                                    │   │
│  │  │ Write Silver│                                                    │   │
│  │  └─────────────┘                                                    │   │
│  └───────────────────────────────────┬─────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 3: LOG SUMMARY                                                │   │
│  │  ─────────────────────                                              │   │
│  │  • customers: 10,000 rows cleaned                                   │   │
│  │  • accounts: 25,000 rows cleaned                                    │   │
│  │  • transactions: 500,000 rows cleaned                               │   │
│  │  • cards: 15,000 rows cleaned                                       │   │
│  │  • loans: 8,000 rows cleaned                                        │   │
│  └───────────────────────────────────┬─────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 4: STOP SPARK SESSION                                         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### What Each Cleaning Function Does

#### 1. `clean_customers()` - Customer Data Cleaning

```
INPUT:  s3://banking-lake/bronze/core-banking/customers/ingestion_date=2024-01-15/
OUTPUT: s3://banking-lake/silver/core-banking/customers/
```

| Step | Operation | Rule | Example |
|------|-----------|------|---------|
| 1 | `dropDuplicates` | Remove duplicate `customer_id` | ID 123 appears twice → keep one |
| 2 | `filter NOT NULL` | `customer_id` must exist | NULL IDs removed |
| 3 | `filter NOT EMPTY` | `customer_id` cannot be empty string | "" removed |
| 4 | `trim + upper` | `customer_name` → UPPERCASE, trimmed | " john doe " → "JOHN DOE" |
| 5 | `regex validate` | `email` must match `@` and `.` pattern | "invalid" → NULL |
| 6 | `regex validate` | `phone` must be 10-15 digits | "123" → NULL |
| 7 | `standardize` | `gender` → MALE/FEMALE/OTHER | "M" → "MALE" |
| 8 | `trim + upper` | `nationality` standardized | "vietnamese" → "VIETNAMESE" |
| 9 | `trim` | `pan_number` trimmed | " ABC123 " → "ABC123" |
| 10 | `timestamp` | Add `cleaned_at` column | Audit trail |

#### 2. `clean_accounts()` - Account Data Cleaning

```
INPUT:  s3://banking-lake/bronze/core-banking/accounts/ingestion_date=2024-01-15/
OUTPUT: s3://banking-lake/silver/core-banking/accounts/
```

| Step | Operation | Rule | Example |
|------|-----------|------|---------|
| 1 | `dropDuplicates` | Remove duplicate `account_id` | — |
| 2 | `filter NOT NULL` | `account_id` and `customer_id` required | — |
| 3 | `greatest` | `current_balance` ≥ 0 | -500 → 0 |
| 4 | `least` | `available_balance` ≤ `current_balance` | 1000 > 500 → 500 |
| 5 | `standardize` | `account_type` → SAVINGS/CURRENT/FIXED_DEPOSIT/RECURRING_DEPOSIT | "SAV" → "SAVINGS" |
| 6 | `standardize` | `status` → ACTIVE/CLOSED/DORMANT/FROZEN | "A" → "ACTIVE" |
| 7 | `trim + upper` | `currency` standardized | "vnd" → "VND" |
| 8 | `trim` | `branch_code` trimmed | — |

#### 3. `clean_transactions()` - Transaction Data Cleaning

```
INPUT:  s3://banking-lake/bronze/core-banking/transactions/ingestion_date=2024-01-15/
OUTPUT: s3://banking-lake/silver/core-banking/transactions/ (partitioned by txn_date)
```

| Step | Operation | Rule | Example |
|------|-----------|------|---------|
| 1 | `dropDuplicates` | Remove duplicate `txn_id` | — |
| 2 | `filter` | `txn_id` and `account_id` not NULL | — |
| 3 | `filter` | `amount` > 0 | 0 or negative removed |
| 4 | `abs` | `amount` always positive | -500 → 500 |
| 5 | `standardize` | `txn_type` → CREDIT/DEBIT/TRANSFER/OTHER | "CR" → "CREDIT" |
| 6 | `standardize` | `channel` → ATM/MOBILE/ONLINE/BRANCH/UPI/BANK_TRANSFER | "APP" → "MOBILE" |
| 7 | `computed` | `is_weekend` → TRUE/FALSE | Saturday = TRUE |
| 8 | `computed` | `time_bucket` → MORNING/AFTERNOON/EVENING/NIGHT | 10:00 → "MORNING" |
| 9 | `trim + upper` | `currency` standardized | — |
| 10 | `partitionBy` | Partition output by `txn_date` | Efficient date queries |

#### 4. `clean_cards()` - Credit Card Data Cleaning

```
INPUT:  s3://banking-lake/bronze/credit-cards/cards/ingestion_date=2024-01-15/
OUTPUT: s3://banking-lake/silver/credit-cards/cards/
```

| Step | Operation | Rule | Example |
|------|-----------|------|---------|
| 1 | `dropDuplicates` | Remove duplicate `card_number` | — |
| 2 | `filter` | `card_number`, `customer_id` not NULL, `card_limit` > 0 | — |
| 3 | `mask` | `card_number_masked` → last 4 digits only | "4111111111111234" → "XXXX-XXXX-XXXX-1234" |
| 4 | `standardize` | `card_brand` → VISA/MASTERCARD/AMEX/RUPAY | "MC" → "MASTERCARD" |
| 5 | `computed` | `available_credit` = limit - used | 100000 - 30000 = 70000 |
| 6 | `computed` | `utilization_pct` = (used / limit) × 100 | 30% utilization |

#### 5. `clean_loans()` - Loan Data Cleaning

```
INPUT:  s3://banking-lake/bronze/loans/loan_accounts/ingestion_date=2024-01-15/
OUTPUT: s3://banking-lake/silver/loans/loan_accounts/
```

| Step | Operation | Rule | Example |
|------|-----------|------|---------|
| 1 | `dropDuplicates` | Remove duplicate `loan_id` | — |
| 2 | `filter` | `loan_id`, `customer_id` not NULL | — |
| 3 | `filter` | `principal_amount` > 0 | — |
| 4 | `filter` | `interest_rate` > 0 AND < 30% | Invalid rates removed |
| 5 | `standardize` | `loan_type` → HOME_LOAN/PERSONAL_LOAN/CAR_LOAN/BUSINESS_LOAN/EDUCATION_LOAN/GOLD_LOAN | "HL" → "HOME_LOAN" |
| 6 | `greatest` | `principal_outstanding` ≥ 0 | — |
| 7 | `computed` | `interest_rate_band` → LOW/MEDIUM/HIGH | 10.5% → "MEDIUM" |
| 8 | `computed` | `loan_status` → ACTIVE/MATURED | Based on maturity_date |

### Running the Job

```bash
# Basic execution (uses yesterday's date)
spark-submit bronze-to-silver.py

# Specific date
spark-submit bronze-to-silver.py 2024-01-15

# With custom Spark config
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 4g \
  --executor-cores 4 \
  --num-executors 10 \
  bronze-to-silver.py 2024-01-15
```

### Spark Configuration Explained

| Config | Value | Purpose |
|--------|-------|---------|
| `partitionOverwriteMode` | `dynamic` | Only overwrite affected partitions (not entire table) |
| `shuffle.partitions` | `200` | Number of partitions for shuffles (join, groupBy) |
| `adaptive.enabled` | `true` | Auto-optimize shuffle partitions at runtime |
| `adaptive.coalescePartitions` | `true` | Merge small partitions automatically |
| `parquet.mergeSchema` | `false` | Skip schema merging (faster reads) |
| `serializer` | `KryoSerializer` | 2-10x faster than default Java serializer |

---

## Spark Job 2: Silver to Gold (`silver-to-gold.py`)

### Purpose
Read cleansed data from the **Silver layer**, apply business logic, create aggregations, and write **business-ready datasets** to the **Gold layer**. These Gold tables are consumed by BI tools, Dremio, and dashboards.

### When to Use
- **After Silver transformation** completes
- When building **Customer 360° views** or **risk dashboards**
- When creating **aggregated summaries** for reporting
- For **large-scale aggregations** that benefit from Spark

### Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SILVER TO GOLD - EXECUTION FLOW                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  COMMAND: spark-submit silver-to-gold.py                                    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 1: CREATE SPARK SESSION                                       │   │
│  │  ────────────────────────────                                       │   │
│  │  • App Name: "Silver_to_Gold"                                       │   │
│  │  • Adaptive query execution: enabled                                │   │
│  └───────────────────────────────────┬─────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 2: CREATE GOLD VIEWS (3 parallel tasks)                       │   │
│  │  ─────────────────────────────────────────────                      │   │
│  │                                                                     │   │
│  │  ┌───────────────────┐  ┌───────────────────┐  ┌─────────────────┐ │   │
│  │  │ CUSTOMER 360      │  │ DAILY TRANSACTION │  │ CREDIT RISK     │ │   │
│  │  │                   │  │ SUMMARY           │  │ DASHBOARD       │ │   │
│  │  │ Read 5 Silver     │  │                   │  │                 │ │   │
│  │  │ tables:           │  │ Read Silver       │  │ Read 3 Silver   │ │   │
│  │  │ • customers       │  │ transactions      │  │ tables:         │ │   │
│  │  │ • accounts        │  │                   │  │ • loans         │ │   │
│  │  │ • transactions    │  │ Group By:         │  │ • payments      │ │   │
│  │  │ • cards           │  │ • date            │  │ • customers     │ │   │
│  │  │ • loans           │  │ • channel         │  │                 │ │   │
│  │  │                   │  │ • type            │  │ Join +          │ │   │
│  │  │ Join + GroupBy    │  │                   │  │ Aggregate       │ │   │
│  │  │ customer          │  │ Aggregate:        │  │                 │ │   │
│  │  │                   │  │ • count           │  │ Calculate:      │ │   │
│  │  │ Calculate:        │  │ • sum             │  │ • success rate  │ │   │
│  │  │ • total_accounts  │  │ • avg             │  │ • risk class    │ │   │
│  │  │ • total_balance   │  │ • high_value      │  │ • DPD           │ │   │
│  │  │ • total_cards     │  │   count           │  │                 │ │   │
│  │  │ • total_loans     │  │                   │  │                 │ │   │
│  │  │ • net_value       │  │                   │  │                 │ │   │
│  │  │ • segment         │  │                   │  │                 │ │   │
│  │  └────────┬──────────┘  └────────┬──────────┘  └────────┬────────┘ │   │
│  │           │                      │                       │          │   │
│  │           ▼                      ▼                       ▼          │   │
│  │  ┌───────────────────┐  ┌───────────────────┐  ┌─────────────────┐ │   │
│  │  │ Write to Gold:    │  │ Write to Gold:    │  │ Write to Gold:  │ │   │
│  │  │ /gold/customer-   │  │ /gold/daily-      │  │ /gold/credit-   │ │   │
│  │  │   360/            │  │   transaction-    │  │   risk-         │ │   │
│  │  │                   │  │   summary/        │  │   dashboard/    │ │   │
│  │  └───────────────────┘  └───────────────────┘  └─────────────────┘ │   │
│  └───────────────────────────────────┬─────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 3: LOG SUMMARY                                                │   │
│  │  ─────────────────────                                              │   │
│  │  • customer_360: 50,000 customers                                   │   │
│  │  • daily_transactions: 1,500 records                                │   │
│  │  • credit_risk: 8,000 loans                                         │   │
│  └───────────────────────────────────┬─────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 4: STOP SPARK SESSION                                         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### What Each Gold Function Creates

#### 1. `create_customer_360()` - Customer 360° View

```
INPUT:  5 Silver tables (customers, accounts, transactions, cards, loans)
OUTPUT: s3://banking-lake/gold/customer-360/
```

**What it does:**
```
┌─────────────────────────────────────────────────────────────────┐
│                 CUSTOMER 360° VIEW                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Joins ALL customer data into ONE unified view:                │
│                                                                 │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                    │
│  │Customer │───►│Accounts │───►│Trans-   │                    │
│  │Profile  │    │(Savings,│    │actions  │                    │
│  │         │    │Current) │    │         │                    │
│  └─────────┘    └─────────┘    └─────────┘                    │
│       │              │              │                           │
│       │         ┌─────────┐    ┌─────────┐                    │
│       │         │Credit   │    │Loans    │                    │
│       │         │Cards    │    │(Home,   │                    │
│       │         │         │    │Personal)│                    │
│       │         └─────────┘    └─────────┘                    │
│       │              │              │                           │
│       └──────────────┼──────────────┘                           │
│                      ▼                                          │
│              ┌───────────────┐                                  │
│              │  CUSTOMER 360 │                                  │
│              │               │                                  │
│              │  • Name       │                                  │
│              │  • Email      │                                  │
│              │  • Phone      │                                  │
│              │  • City       │                                  │
│              │  • Accounts   │                                  │
│              │  • Balance    │                                  │
│              │  • Cards      │                                  │
│              │  • Loans      │                                  │
│              │  • Segment    │                                  │
│              └───────────────┘                                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Metrics Calculated:**

| Metric | Description | Example |
|--------|-------------|---------|
| `total_accounts` | Count of distinct accounts | 3 |
| `total_balance` | Sum of all account balances | 50,000,000 VND |
| `total_cards` | Count of distinct credit cards | 2 |
| `total_card_limit` | Sum of card limits | 100,000,000 VND |
| `total_card_outstanding` | Sum of credit used | 30,000,000 VND |
| `total_loans` | Count of distinct loans | 1 |
| `total_loan_outstanding` | Sum of principal outstanding | 500,000,000 VND |
| `total_monthly_emi` | Sum of monthly EMI amounts | 15,000,000 VND |
| `net_relationship_value` | balance + card_limit - loan_outstanding | -350,000,000 VND |
| `customer_segment` | Based on net_relationship_value | STANDARD |

**Customer Segmentation Rules:**

```
┌─────────────────────────────────────────────────────────────────┐
│                 CUSTOMER SEGMENTATION                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Net Relationship Value  →  Segment                            │
│  ──────────────────────────────────────                        │
│  ≥ 10,000,000,000 VND   →  PLATINUM  (High Net Worth)         │
│  ≥ 5,000,000,000 VND    →  GOLD      (Premium)                │
│  ≥ 1,000,000,000 VND    →  SILVER    (Regular)                │
│  < 1,000,000,000 VND    →  STANDARD  (Basic)                  │
│                                                                 │
│  Note: 10,000,000,000 VND ≈ $400,000 USD                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 2. `create_daily_transaction_summary()` - Daily Transaction Summary

```
INPUT:  Silver transactions table
OUTPUT: s3://banking-lake/gold/daily-transaction-summary/
```

**What it does:**
```
┌─────────────────────────────────────────────────────────────────┐
│                 DAILY TRANSACTION SUMMARY                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Groups transactions by:                                       │
│  • txn_date (e.g., 2024-01-15)                                 │
│  • channel_standardized (ATM, MOBILE, ONLINE, BRANCH)          │
│  • txn_type_standardized (CREDIT, DEBIT, TRANSFER)             │
│                                                                 │
│  Calculates:                                                   │
│  • transaction_count    → How many transactions                │
│  • total_amount         → Sum of all amounts                   │
│  • avg_amount           → Average transaction size             │
│  • unique_accounts      → How many distinct accounts           │
│  • high_value_count     → Transactions > 100,000 VND           │
│                                                                 │
│  Example Output:                                               │
│  ┌──────────┬─────────┬────────┬───────┬───────┬────────┐     │
│  │ Date     │ Channel │ Type   │ Count │ Total │ High   │     │
│  │          │         │        │       │       │ Value  │     │
│  ├──────────┼─────────┼────────┼───────┼───────┼────────┤     │
│  │ 2024-01-15│ MOBILE │ CREDIT │ 5,000 │ 500M  │ 120    │     │
│  │ 2024-01-15│ MOBILE │ DEBIT  │ 3,000 │ 300M  │ 80     │     │
│  │ 2024-01-15│ ATM    │ DEBIT  │ 2,000 │ 200M  │ 50     │     │
│  │ 2024-01-15│ ONLINE │ TRANSFER│ 1,500│ 750M  │ 200    │     │
│  └──────────┴─────────┴────────┴───────┴───────┴────────┘     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 3. `create_credit_risk_dashboard()` - Credit Risk Dashboard

```
INPUT:  3 Silver tables (loans, payments, customers)
OUTPUT: s3://banking-lake/gold/credit-risk-dashboard/
```

**What it does:**
```
┌─────────────────────────────────────────────────────────────────┐
│                 CREDIT RISK DASHBOARD                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Joins loans with payments and customers:                      │
│                                                                 │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                    │
│  │  Loans  │───►│ Payments│───►│Customer │                    │
│  │         │    │         │    │ Profile │                    │
│  └─────────┘    └─────────┘    └─────────┘                    │
│                                                                 │
│  Calculates:                                                   │
│  • total_payments_made      → Total EMI payments made          │
│  • successful_payments      → Payments that succeeded          │
│  • failed_payments          → Payments that failed             │
│  • payment_success_rate     → Success %                        │
│  • risk_classification      → STANDARD/MATURED                 │
│                                                                 │
│  Risk Rules:                                                   │
│  • maturity_date < today   → MATURED (loan completed)          │
│  • maturity_date ≥ today   → STANDARD (still active)           │
│                                                                 │
│  Example Output:                                               │
│  ┌──────────┬────────┬────────┬────────┬────────┬────────┐    │
│  │ Loan ID  │ Type   │ Amount │ Payments│ Success│ Risk   │    │
│  ├──────────┼────────┼────────┼────────┼────────┼────────┤    │
│  │ LN-001   │ HOME   │ 500M   │ 60/60  │ 100%   │ STD    │    │
│  │ LN-002   │ PERSONAL│ 50M   │ 10/36  │ 90%    │ STD    │    │
│  │ LN-003   │ CAR    │ 200M   │ 48/48  │ 95%    │ MAT    │    │
│  └──────────┴────────┴────────┴────────┴────────┴────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Running the Job

```bash
# Basic execution
spark-submit silver-to-gold.py

# With custom Spark config
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 20 \
  silver-to-gold.py
```

---

## Data Flow Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    COMPLETE DATA FLOW                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SOURCE SYSTEMS                                                            │
│  ───────────────                                                           │
│  Oracle (Core Banking) ──────┐                                             │
│  Mainframe (Cards) ──────────┼──► Bronze Layer (Raw)                       │
│  SQL Server (Loans) ─────────┘    s3://banking-lake/bronze/                │
│                                        │                                    │
│                                        ▼                                    │
│                              ┌──────────────────┐                          │
│                              │  SPARK JOB 1:    │                          │
│                              │  bronze-to-      │                          │
│                              │  silver.py       │                          │
│                              │                  │                          │
│                              │  • Clean         │                          │
│                              │  • Validate      │                          │
│                              │  • Standardize   │                          │
│                              └────────┬─────────┘                          │
│                                       │                                     │
│                                       ▼                                     │
│                              Silver Layer (Cleansed)                       │
│                              s3://banking-lake/silver/                     │
│                                       │                                     │
│                                       ▼                                     │
│                              ┌──────────────────┐                          │
│                              │  SPARK JOB 2:    │                          │
│                              │  silver-to-      │                          │
│                              │  gold.py         │                          │
│                              │                  │                          │
│                              │  • Aggregate     │                          │
│                              │  • Join tables   │                          │
│                              │  • Business logic│                          │
│                              └────────┬─────────┘                          │
│                                       │                                     │
│                                       ▼                                     │
│                              Gold Layer (Business-Ready)                   │
│                              s3://banking-lake/gold/                       │
│                                       │                                     │
│                    ┌──────────────────┼──────────────────┐                 │
│                    ▼                  ▼                  ▼                 │
│              ┌──────────┐      ┌──────────┐      ┌──────────┐            │
│              │Customer  │      │ Daily    │      │ Credit   │            │
│              │360       │      │ Summary  │      │ Risk     │            │
│              └──────────┘      └──────────┘      └──────────┘            │
│                    │                  │                  │                 │
│                    ▼                  ▼                  ▼                 │
│              ┌──────────────────────────────────────────────┐            │
│              │           CONSUMERS                           │            │
│              │  • Dremio (Query Engine)                     │            │
│              │  • Power BI (Dashboards)                     │            │
│              │  • Regulatory Reports (SBV)                  │            │
│              │  • Fraud Detection (ML)                      │            │
│              └──────────────────────────────────────────────┘            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Spark Jobs vs Airflow DAGs

| Aspect | Spark Jobs | Airflow DAGs |
|--------|------------|--------------|
| **Execution** | Standalone (`spark-submit`) | Orchestrated (Airflow scheduler) |
| **Monitoring** | Logs only | Airflow UI with rich visuals |
| **Scheduling** | External (cron, etc.) | Built-in scheduling |
| **Dependencies** | Manual (run job 1, then job 2) | Automatic (task dependencies) |
| **Retries** | Manual retry | Automatic retry with config |
| **Alerting** | Custom email code | Built-in email operators |
| **Best for** | Large-scale batch processing | Workflow orchestration |
| **When to use** | Data > 1TB, ML workloads | Need scheduling, monitoring, dependencies |

### Recommendation

```
┌─────────────────────────────────────────────────────────────────┐
│                 WHEN TO USE WHICH                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  USE SPARK JOBS WHEN:                                          │
│  • Data volume > 1TB                                           │
│  • Complex ML transformations needed                           │
│  • Running outside of Airflow (e.g., Databricks, EMR)          │
│  • Need fine-grained Spark configuration                      │
│  • Standalone batch processing                                 │
│                                                                 │
│  USE AIRFLOW DAGS WHEN:                                        │
│  • Need scheduling (hourly, daily)                             │
│  • Need monitoring and alerting                                │
│  • Multiple dependencies (Bronze → Silver → Gold → Reports)   │
│  • Team needs visibility into pipeline status                  │
│  • Need retry logic and failure handling                       │
│                                                                 │
│  USE BOTH TOGETHER WHEN:                                       │
│  • Airflow orchestrates, Spark processes                       │
│  • Airflow schedules `spark-submit` commands                   │
│  • Best of both worlds                                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Common Spark Commands

```bash
# Run Bronze to Silver
spark-submit bronze-to-silver.py 2024-01-15

# Run Silver to Gold
spark-submit silver-to-gold.py

# Run on YARN cluster
spark-submit --master yarn --deploy-mode cluster bronze-to-silver.py

# Run with more memory
spark-submit --executor-memory 8g --driver-memory 4g bronze-to-silver.py

# Run with more cores
spark-submit --executor-cores 8 --num-executors 20 bronze-to-silver.py

# Run with Kryo serialization (faster)
spark-submit --conf spark.serializer=org.apache.spark.serializer.KryoSerializer bronze-to-silver.py

# Run with dynamic allocation
spark-submit --conf spark.dynamicAllocation.enabled=true bronze-to-silver.py

# Check Spark UI (after job starts)
# http://localhost:4040
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `OutOfMemoryError` | Data too large for executor | Increase `--executor-memory` or add more executors |
| `FileNotFoundException` | Bronze data not ready | Check Bronze ingestion DAG completed |
| `AnalysisException: Column not found` | Schema mismatch | Verify Bronze data has expected columns |
| `SparkException: Job aborted` | Task failure | Check executor logs for root cause |
| `Slow performance` | Too few partitions | Increase `spark.sql.shuffle.partitions` |

### Performance Tips

| Tip | When to Use | How |
|-----|-------------|-----|
| Use Kryo serializer | Always | `--conf spark.serializer=org.apache.spark.serializer.KryoSerializer` |
| Enable AQE | Always | `--conf spark.sql.adaptive.enabled=true` |
| Increase partitions | Large datasets | `--conf spark.sql.shuffle.partitions=400` |
| Use coalesce | Writing large output | `df.coalesce(100).write...` |
| Cache frequently used DF | Multiple operations | `df.cache()` or `df.persist()` |

---

## Related Files

| File | Purpose |
|------|---------|
| `bronze-to-silver.py` | Bronze → Silver transformation (cleaning, validation) |
| `silver-to-gold.py` | Silver → Gold transformation (aggregation, business logic) |

---

## Related Documentation

| Document | Location |
|----------|----------|
| Airflow DAGs | `../airflow/dags/README.md` |
| ETL Pipelines Overview | `../README.md` |
| Performance Optimization | `../../10-performance/query-optimization.md` |

---

*Part of: [Lakehouse Platform Project](../../README.md)*
