# Airflow DAGs - Banking Data Platform

## Overview

This folder contains **5 Apache Airflow DAGs** that orchestrate the entire data pipeline for a banking data platform. Each DAG handles a specific layer of the **Medallion Architecture** (Bronze → Silver → Gold) plus regulatory reporting and real-time CDC ingestion.

---

## DAG Summary

| # | DAG Name | Purpose | Schedule | Layer |
|---|----------|---------|----------|-------|
| 1 | `bronze_ingestion` | Ingest raw data from source systems | Every hour | Bronze |
| 2 | `silver_transformation` | Clean, validate, and conform data | Daily 2 AM | Silver |
| 3 | `gold_aggregation` | Create business-ready aggregations | Daily 4 AM | Gold |
| 4 | `regulatory_reports` | Generate and submit SBV reports | Daily 6 AM | Reports |
| 5 | `cdc_kafka_ingestion` | Consume real-time CDC events from Kafka | Every 5 min | Bronze (Real-time) |

---

## How the DAGs Work Together

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    AIRFLOW DAG SCHEDULING FLOW                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ⏰ EVERY HOUR                          ⏰ EVERY 5 MINUTES                 │
│  ┌──────────────────┐                   ┌──────────────────┐                │
│  │ bronze_ingestion │                   │ cdc_kafka        │                │
│  │ (Batch Extract)  │                   │ _ingestion       │                │
│  └────────┬─────────┘                   │ (Real-time CDC)  │                │
│           │                             └────────┬─────────┘                │
│           ▼                                      ▼                          │
│  ┌──────────────────┐                   ┌──────────────────┐                │
│  │  BRONZE LAYER    │◄─────────────────►│  BRONZE LAYER    │                │
│  │  (Raw Data)      │                   │  (CDC Events)    │                │
│  └────────┬─────────┘                   └────────┬─────────┘                │
│           │                                      │                          │
│           ▼                                      ▼                          │
│  ⏰ DAILY 2:00 AM                                                           │
│  ┌──────────────────┐                                                       │
│  │ silver_transform │                                                       │
│  │ (Clean & Validate)                                                       │
│  └────────┬─────────┘                                                       │
│           │                                                                 │
│           ▼                                                                 │
│  ┌──────────────────┐                                                       │
│  │  SILVER LAYER    │                                                       │
│  │  (Cleansed Data) │                                                       │
│  └────────┬─────────┘                                                       │
│           │                                                                 │
│           ▼                                                                 │
│  ⏰ DAILY 4:00 AM                                                           │
│  ┌──────────────────┐                                                       │
│  │ gold_aggregation │                                                       │
│  │ (Aggregate Data) │                                                       │
│  └────────┬─────────┘                                                       │
│           │                                                                 │
│           ▼                                                                 │
│  ┌──────────────────┐                                                       │
│  │  GOLD LAYER      │                                                       │
│  │  (Business-Ready)│                                                       │
│  └────────┬─────────┘                                                       │
│           │                                                                 │
│           ▼                                                                 │
│  ⏰ DAILY 6:00 AM                                                           │  
│  ┌──────────────────┐                                                       │
│  │ regulatory       │                                                       │
│  │ _reports         │                                                       │
│  │ (SBV Reports)    │                                                       │
│  └────────┬─────────┘                                                       │
│           │                                                                 │
│           ▼                                                                 │
│  ┌──────────────────┐                                                       │
│  │  SBV Portal      │                                                       │
│  │  (Submission)    │                                                       │
│  └──────────────────┘                                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────
```

---

## DAG 1: Bronze Ingestion (`bronze_ingestion.py`)

### Purpose
Extract raw data from **3 banking source systems** and load it into the Bronze layer (data lake) in Parquet format.

### Schedule
- **Frequency:** Every hour (`0 * * * *`)
- **Timeout:** 1 hour
- **Retries:** 2 (with 5-minute delay)

### Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE INGESTION DAG                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              TaskGroup: extract_sources                 │    │
│  │                                                         │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │    │
│  │  │ extract_core │  │ extract_     │  │ extract_     │   │    │
│  │  │ _banking     │  │ credit_cards │  │ loans        │   │    │
│  │  │              │  │              │  │              │   │    │
│  │  │ Source:      │  │ Source:      │  │ Source:      │   │    │
│  │  │ Oracle DB    │  │ Mainframe    │  │ SQL Server   │   │    │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │    │
│  │         │                 │                  │          │    │
│  └─────────┼─────────────────┼──────────────────┼──────────┘    │
│            │                 │                  │               │
│            ▼                 ▼                  ▼               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           validate_bronze_data                          │    │
│  │                                                         │    │
│  │  • Check row counts > 0                                 │    │
│  │  • Verify columns exist                                 │    │
│  │  • Log metadata                                         │    │
│  └──────────────────────┬──────────────────────────────────┘    │
│                         │                                       │
│                         ▼                                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           notify_failure (on failure)                   │    │
│  │                                                         │    │
│  │  • Send email to data-engineering@bank.com              │    │
│  │  • Include execution date and log URL                   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### What Each Task Does

| Task | Source System | Extraction Method | Output Location |
|------|---------------|-------------------|-----------------|
| `extract_core_banking` | Oracle (Core Banking) | SQL query with `pd.read_sql()` | `s3://banking-lake/bronze/core-banking/` |
| `extract_credit_cards` | Mainframe (Cards) | API call (placeholder) | `s3://banking-lake/bronze/credit-cards/` |
| `extract_loans` | SQL Server (Loans) | SQL query with `pd.read_sql()` | `s3://banking-lake/bronze/loans/` |
| `validate_bronze_data` | All sources | Pandas read_parquet | Validation report |
| `notify_failure` | N/A | EmailOperator | Email notification |

### Key Features

- **Incremental extraction:** Uses `prev_execution_date_success` to extract only new/changed data
- **Partitioned output:** Data partitioned by `ingestion_date` for efficient querying
- **Metadata tracking:** Returns row counts and execution metadata
- **Failure notification:** Sends email alert if any extraction fails

### Output Structure

```
s3://banking-lake/bronze/
├── core-banking/
│   ├── accounts/
│   │   └── ingestion_date=2024-01-15/
│   │       └── part-00000.parquet
│   └── transactions/
│       └── ingestion_date=2024-01-15/
│           └── part-00000.parquet
├── credit-cards/
│   ├── cards/
│   │   └── ingestion_date=2024-01-15/
│   └── card_transactions/
│       └── ingestion_date=2024-01-15/
└── loans/
    ├── loan_accounts/
    │   └── ingestion_date=2024-01-15/
    └── loan_payments/
        └── ingestion_date=2024-01-15/
```

---

## DAG 2: Silver Transformation (`silver_transform.py`)

### Purpose
Clean, deduplicate, validate, and conform raw Bronze data into the Silver layer. Apply data quality rules and standardize formats.

### Schedule
- **Frequency:** Daily at 2 AM (`0 2 * * *`)
- **Timeout:** 2 hours
- **Retries:** 1 (with 10-minute delay)
- **Depends on past:** Yes (waits for previous run)

### Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER TRANSFORMATION DAG                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              TaskGroup: clean_sources                   │    │
│  │                                                         │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │    │
│  │  │ clean_       │  │ clean_       │  │ clean_       │   │    │
│  │  │ customers    │  │ accounts     │  │ transactions │   │    │
│  │  │              │  │              │  │              │   │    │
│  │  │ • Dedup      │  │ • Dedup      │  │ • Dedup      │   │    │
│  │  │ • Trim names │  │ • Validate   │  │ • Validate   │   │    │
│  │  │ • Validate   │  │ • Standardize│  │ • Standardize│   │    │
│  │  │   email/phone│  │   account    │  │   txn type   │   │    │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │    │
│  │         │                 │                  │          │    │
│  │  ┌──────┴───────┐                                       │    │
│  │  │ clean_       │                                       │    │
│  │  │ cards        │                                       │    │
│  │  │              │                                       │    │
│  │  │ • Dedup      │                                       │    │
│  │  │ • Mask card  │                                       │    │
│  │  │   numbers    │                                       │    │
│  │  │ • Calc       │                                       │    │
│  │  │   utilization│                                       │    │
│  │  └──────┬───────┘                                       │    │
│  │         │                                               │    │
│  └─────────┼─────────────────────────────────────────┘     │
│            │                                               │
│            ▼                                               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           validate_silver_data                          │    │
│  │                                                         │    │
│  │  • Check row counts                                     │    │
│  │  • Verify no null primary keys                          │    │
│  │  • Log validation results                               │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### What Each Cleaning Task Does

| Task | Input | Cleaning Rules | Output |
|------|-------|----------------|--------|
| `clean_customers` | Bronze customers | Dedup, trim names, validate email/phone | Silver customers |
| `clean_accounts` | Bronze accounts | Dedup, validate balances, standardize types | Silver accounts |
| `clean_transactions` | Bronze transactions | Dedup, validate amounts, standardize channels | Silver transactions |
| `clean_cards` | Bronze cards | Dedup, mask card numbers, calc utilization % | Silver cards |
| `validate_silver_data` | All Silver tables | Null checks, row counts | Validation report |

### Data Quality Rules Applied

```python
# Customer Cleaning Rules
customer_id    → NOT NULL (primary key)
customer_name  → TRIM + UPPER (standardize case)
email          → Regex validation (must contain @ and .)
phone          → Regex validation (10-15 digits only)

# Account Cleaning Rules
account_id     → NOT NULL
customer_id    → NOT NULL
current_balance → >= 0 (no negative balances)
available_balance → <= current_balance
account_type   → Standardized (SAVINGS, CURRENT, FIXED_DEPOSIT, OTHER)
status         → Standardized (ACTIVE, CLOSED, DORMANT, UNKNOWN)

# Transaction Cleaning Rules
txn_id         → NOT NULL
amount         → > 0, ABS() (no negative amounts)
txn_type       → Standardized (CREDIT, DEBIT, TRANSFER, OTHER)
channel        → Standardized (ATM, MOBILE, ONLINE, BRANCH, OTHER)
is_weekend     → Computed (Saturday/Sunday flag)

# Card Cleaning Rules
card_number    → NOT NULL
card_limit     → > 0
card_number_masked → XXXX-XXXX-XXXX-1234 (mask all but last 4)
utilization_pct → (credit_used / card_limit) * 100
```

### Output Structure

```
s3://banking-lake/silver/
├── core-banking/
│   ├── customers/          (overwrite mode)
│   ├── accounts/           (overwrite mode)
│   └── transactions/       (partitioned by txn_date)
└── credit-cards/
    └── cards/              (overwrite mode)
```

---

## DAG 3: Gold Aggregation (`gold_aggregation.py`)

### Purpose
Create **business-ready aggregated views** from Silver data. Build Customer 360°, Daily Transaction Summary, and Credit Risk Dashboard. Also refresh Dremio reflections for fast querying.

### Schedule
- **Frequency:** Daily at 4 AM (`0 4 * * *`)
- **Timeout:** 3 hours
- **Retries:** 1 (with 15-minute delay)
- **Depends on past:** Yes

### Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    GOLD AGGREGATION DAG                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              TaskGroup: create_gold_views               │    │
│  │                                                         │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │    │
│  │  │ create_      │  │ create_      │  │ create_      │   │    │
│  │  │ customer_360 │  │ daily_       │  │ credit_risk  │   │    │
│  │  │              │  │ transaction_ │  │ _dashboard   │   │    │
│  │  │ Joins:       │  │ summary      │  │              │   │    │
│  │  │ • customers  │  │              │  │ Joins:       │   │    │
│  │  │ • accounts   │  │ Groups by:   │  │ • loans      │   │    │
│  │  │ • transactions│ │ • date       │  │ • payments   │   │    │
│  │  │ • cards      │  │ • channel    │  │ • customers  │   │    │
│  │  │ • loans      │  │ • type       │  │              │   │    │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │    │
│  │         │                 │                  │          │    │
│  └─────────┼─────────────────┼──────────────────┼──────────┘    │
│            │                 │                  │               │
│            ▼                 ▼                  ▼               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           update_dremio_reflections                     │    │
│  │                                                         │    │
│  │  • Refresh customer-360-raw-reflection                  │    │
│  │  • Refresh daily-transactions-agg-reflection            │    │
│  │  • Refresh risk-dashboard-raw-reflection                │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### What Each Gold Task Creates

| Task | Input Tables | Aggregation | Output | Business Use |
|------|--------------|-------------|--------|--------------|
| `create_customer_360` | customers, accounts, transactions, cards, loans | Join + GroupBy customer | `s3://banking-lake/gold/customer-360/` | Call center, Relationship manager |
| `create_daily_transaction_summary` | transactions | GroupBy date, channel, type | `s3://banking-lake/gold/daily-transaction-summary/` | Operations dashboard |
| `create_credit_risk_dashboard` | loans, payments, customers | Join + Payment analysis | `s3://banking-lake/gold/credit-risk-dashboard/` | Risk management, NPA tracking |
| `update_dremio_reflections` | Gold tables | API call to Dremio | Refreshed reflections | Fast query performance |

### Customer 360° Metrics

```sql
-- What the Customer 360 view calculates:
total_accounts          → Count of distinct accounts
total_balance           → Sum of all account balances
total_cards             → Count of distinct credit cards
total_card_limit        → Sum of card limits
total_card_outstanding  → Sum of credit used
total_loans             → Count of distinct loans
total_loan_outstanding  → Sum of principal outstanding
total_monthly_emi       → Sum of monthly EMI amounts
total_transactions      → Count of all transactions

-- Computed Fields:
net_relationship_value  → balance + card_limit - loan_outstanding
customer_segment        → PLATINUM / GOLD / SILVER / STANDARD
```

### Credit Risk Classifications

```
┌─────────────────────────────────────────────────────────────────┐
│                 RISK CLASSIFICATION RULES                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Days Past Due (DPD)  →  Risk Classification                    │
│  ────────────────────────────────────────────                   │
│  DPD = 0              →  STANDARD                               │
│  DPD = 1-30           →  SPECIAL_MENTION                        │
│  DPD = 31-60          →  SUB_STANDARD                           │
│  DPD = 61-90          →  DOUBTFUL                               │
│  DPD > 90             →  NPA (Non-Performing Asset)             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## DAG 4: Regulatory Reports (`regulatory_reports.py`)

### Purpose
Generate **mandatory regulatory reports** for the State Bank of Vietnam (SBV) and submit them via the SBV portal. Also generate executive summaries for bank management.

### Schedule
- **Frequency:** Daily at 6 AM (`0 6 * * *`)
- **Timeout:** 2 hours
- **Retries:** 2 (with 15-minute delay)
- **Depends on past:** Yes

### Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    REGULATORY REPORTS DAG                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              TaskGroup: generate_reports                │    │
│  │                                                         │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │    │
│  │  │ generate_    │  │ generate_    │  │ generate_    │   │    │
│  │  │ call_report  │  │ basel_iii_   │  │ aml_report   │   │    │
│  │  │              │  │ report       │  │              │   │    │
│  │  │ Daily:       │  │ Monthly:     │  │ Daily:       │   │    │
│  │  │ • Assets     │  │ • CET1 ratio │  │ • CTR count  │   │    │
│  │  │ • Deposits   │  │ • Tier1 ratio│  │ • STR alerts │   │    │
│  │  │ • Loans      │  │ • CAR ratio  │  │ • PEP count  │   │    │
│  │  │ • Capital    │  │ • RWA        │  │ • Risk status│   │    │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │    │
│  │         │                 │                  │          │    │
│  └─────────┼─────────────────┼──────────────────┼──────────┘    │
│            │                 │                  │               │
│            ▼                 ▼                  ▼               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           submit_to_sbv                                 │    │
│  │                                                         │    │
│  │  • POST reports to SBV API portal                       │    │
│  │  • Submit: call_report, aml_report                      │    │
│  │  • Log submission status                                │    │
│  └──────────────────────┬──────────────────────────────────┘    │
│                         │                                       │
│                         ▼                                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           generate_executive_summary                    │    │
│  │                                                         │    │
│  │  • Query CEO dashboard from Gold layer                  │    │
│  │  • Generate text summary with key metrics               │    │
│  │  • Save to /data/reports/executive/                     │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Reports Generated

| Report | Frequency | Key Metrics | Submission |
|--------|-----------|-------------|------------|
| **Call Report** | Daily | Total assets, deposits, loans, CAR, LCR, NPL ratio | SBV Portal |
| **Basel III Report** | Monthly | CET1, Tier1, Total capital, RWA, compliance status | SBV Portal |
| **AML Report** | Daily | CTR count, STR alerts, PEP count, risk status | SBV Portal |
| **Executive Summary** | Daily | Assets, NIM, NPL, CAR, digital %, fraud alerts | Management |

### Key Banking Metrics Explained

```
┌─────────────────────────────────────────────────────────────────┐
│                 BANKING METRICS GLOSSARY                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  CAPITAL ADEQUACY:                                              │
│  • CET1 Ratio     → Common Equity Tier 1 / Risk-Weighted Assets │
│  • Tier1 Ratio    → Tier 1 Capital / Risk-Weighted Assets       │
│  • CAR Ratio      → Total Capital / Risk-Weighted Assets        │
│  • SBV Minimum    → CET1 ≥ 4.5%, Tier1 ≥ 6%, CAR ≥ 8%           │
│                                                                 │
│  LIQUIDITY:                                                     │
│  • LCR Ratio      → High Quality Liquid Assets / Net Outflows   │
│  • SBV Minimum    → LCR ≥ 100%                                  │
│                                                                 │
│  ASSET QUALITY:                                                 │
│  • NPL Ratio      → Non-Performing Loans / Total Loans          │
│  • Provision Coverage → Provisions / NPLs                       │
│                                                                 │
│  AML (Anti-Money Laundering):                                   │
│  • CTR            → Currency Transaction Report (>VND 500M)     │
│  • STR            → Suspicious Transaction Report               │
│  • PEP            → Politically Exposed Persons                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## DAG 5: CDC Kafka Ingestion (`cdc_kafka_ingestion.py`)

### Purpose
Consume **real-time Change Data Capture (CDC) events** from Kafka topics and load them into the Bronze layer. This enables near-real-time data updates between source systems and the data lake.

### Schedule
- **Frequency:** Every 5 minutes (`*/5 * * * *`)
- **Timeout:** 30 minutes
- **Retries:** 3 (with 1-minute delay)
- **Depends on past:** No (independent runs)

### Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    CDC KAFKA INGESTION DAG                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              TaskGroup: consume_cdc_events              │    │
│  │                                                         │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │    │
│  │  │ consume_     │  │ consume_     │  │ consume_     │   │    │
│  │  │ core_banking │  │ cards_cdc    │  │ loans_cdc    │   │    │
│  │  │ _cdc         │  │              │  │              │   │    │
│  │  │              │  │              │  │              │   │    │
│  │  │ Topics:      │  │ Topics:      │  │ Topics:      │   │    │
│  │  │ • accounts   │  │ • cards      │  │ • loan_      │   │    │
│  │  │ • customers  │  │ • card_      │  │   accounts   │   │    │
│  │  │ • transactions│ │   transactions│ │ • loan_      │   │    │
│  │  │              │  │              │  │   payments   │   │    │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │    │
│  │         │                 │                  │          │    │
│  └─────────┼─────────────────┼──────────────────┼──────────┘    │
│            │                 │                  │               │
│            ▼                 ▼                  ▼               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           validate_cdc_data                             │    │
│  │                                                         │    │
│  │  • Check each CDC topic has data                        │    │
│  │  • Verify row counts                                    │    │
│  │  • Log validation results                               │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Kafka Topics Consumed

| Topic | Source System | CDC Connector | Data Type |
|-------|---------------|---------------|-----------|
| `accounts` | Core Banking (Oracle) | Debezium | Account updates |
| `customers` | Core Banking (Oracle) | Debezium | Customer changes |
| `transactions` | Core Banking (Oracle) | Debezium | New transactions |
| `cards` | Credit Cards (Mainframe) | Debezium | Card updates |
| `card_transactions` | Credit Cards (Mainframe) | Debezium | Card transactions |
| `loan_accounts` | Loans (SQL Server) | Debezium | Loan updates |
| `loan_payments` | Loans (SQL Server) | Debezium | Payment events |

### How CDC Works

```
┌─────────────────────────────────────────────────────────────────┐
│                    CDC DATA FLOW                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Source DB        Debezium       Kafka         Airflow DAG      │
│  ─────────       ─────────      ──────        ───────────       │
│                                                                 │
│  ┌─────────┐    ┌──────────┐   ┌─────────┐   ┌────────────┐     │
│  │ Oracle  │───►│ Debezium │──►│ Kafka   │──►│ Airflow    │     │
│  │ (WAL)   │    │ Connector│   │ Topic:  │   │ Consumer   │     │
│  └─────────┘    └──────────┘   │ accounts│   │            │     │
│                                 └─────────┘  │ Micro-batch│     │
│  ┌─────────┐    ┌──────────┐   ┌─────────┐   │ (1000 msgs)│     │
│  │ SQL     │───►│ Debezium │──►│ Kafka   │──►│            │     │
│  │ Server  │    │ Connector│   │ Topic:  │   │ Write to   │     │
│  └─────────┘    └──────────┘   │ loans   │   │ Bronze     │     │
│                                └─────────┘   └────────────┘     │
│                                                                 │
│  Key: WAL = Write-Ahead Log (transaction log)                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Micro-Batch Processing

```python
# How the CDC consumer works:
1. Connect to Kafka consumer group
2. Read messages in micro-batches (up to 1000 messages)
3. Convert to DataFrame
4. Process by topic
5. Write to Bronze layer as Parquet
6. Validate data quality

# Key Settings:
group_id = 'airflow-cdc-core-banking'  # Consumer group
auto_offset_reset = 'latest'           # Only new messages
enable_auto_commit = False             # Manual offset control
```

---

## DAG Configuration Summary

| Setting | bronze_ingestion | silver_transform | gold_aggregation | regulatory_reports | cdc_kafka |
|---------|------------------|------------------|------------------|--------------------|-----------|
| **Schedule** | `0 * * * *` | `0 2 * * *` | `0 4 * * *` | `0 6 * * *` | `*/5 * * * *` |
| **Timeout** | 1 hour | 2 hours | 3 hours | 2 hours | 30 min |
| **Retries** | 2 | 1 | 1 | 2 | 3 |
| **Retry Delay** | 5 min | 10 min | 15 min | 15 min | 1 min |
| **Depends on Past** | No | Yes | Yes | Yes | No |
| **Max Active Runs** | 1 | 1 | 1 | 1 | 1 |
| **Owner** | data-engineering | data-engineering | data-engineering | compliance-team | data-engineering |

---

## Task Dependency Chain

```
BRONZE (Every Hour)
    │
    ├── extract_core_banking ─┐
    ├── extract_credit_cards ─┼──► validate_bronze_data
    └── extract_loans ────────┘         │
                                        ▼
                                  notify_failure (on failure)

SILVER (Daily 2 AM)
    │
    ├── clean_customers ─────┐
    ├── clean_accounts ──────┼──► validate_silver_data
    ├── clean_transactions ──┤
    └── clean_cards ─────────┘

GOLD (Daily 4 AM)
    │
    ├── create_customer_360 ─────┐
    ├── create_daily_txn_summary ┼──► update_dremio_reflections
    └── create_credit_risk ──────┘

REGULATORY (Daily 6 AM)
    │
    ├── generate_call_report ───┐
    ├── generate_basel_iii ─────┼──► submit_to_sbv ──► generate_executive_summary
    └── generate_aml_report ────┘

CDC (Every 5 Minutes)
    │
    ├── consume_core_banking_cdc ─┐
    ├── consume_cards_cdc ────────┼──► validate_cdc_data
    └── consume_loans_cdc ────────┘
```

---

## Environment Variables Required

| Variable | Description | Example |
|----------|-------------|---------|
| `ORACLE_CONN_STR` | Core Banking Oracle connection | `oracle+cx_oracle://user:pass@host:1521/COREBANK` |
| `SQLSERVER_CONN_STR` | Loans SQL Server connection | `mssql+pyodbc://user:pass@host/LoansDB` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker list | `kafka-1:9092,kafka-2:9092` |
| `DREMIO_URL` | Dremio endpoint | `http://dremio:9047` |
| `DREMIO_TOKEN` | Dremio authentication token | `Bearer xxx` |
| `SBV_API_URL` | SBV portal API | `https://sbv-portal.gov.vn/api` |
| `SBV_API_KEY` | SBV API key | `xxx` |
| `S3_BUCKET` | MinIO/S3 bucket name | `banking-lake` |

---

## How to Run

### Manual Trigger (Airflow UI)
```bash
# Access Airflow UI
open http://localhost:8080

# Navigate to DAGs and click "Trigger DAG" for any of:
# - bronze_ingestion
# - silver_transformation
# - gold_aggregation
# - regulatory_reports
# - cdc_kafka_ingestion
```

### CLI Commands
```bash
# Trigger a single DAG
airflow dags trigger bronze_ingestion

# Trigger with specific date
airflow dags trigger bronze_ingestion --exec-date 2024-01-15

# Check DAG status
airflow dags list

# View task logs
airflow tasks logs bronze_ingestion extract_core_banking 2024-01-15
```

### Test Individual Tasks
```bash
# Test a specific task
airflow tasks test bronze_ingestion extract_core_banking 2024-01-15

# Dry run (no execution)
airflow dags test bronze_ingestion 2024-01-15
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `Bronze extraction timeout` | Source system slow | Increase `execution_timeout` |
| `Silver validation fails` | Null primary keys | Check Bronze data quality |
| `Gold reflection refresh fails` | Dremio not ready | Check Dremio health |
| `CDC messages not consumed` | Kafka offset issue | Reset consumer group offset |
| `SBV submission fails` | API key expired | Update `SBV_API_KEY` |

### Log Locations
```bash
# Airflow logs
/opt/airflow/logs/bronze_ingestion/
/opt/airflow/logs/silver_transformation/
/opt/airflow/logs/gold_aggregation/
/opt/airflow/logs/regulatory_reports/
/opt/airflow/logs/cdc_kafka_ingestion/
```

---

## Related Files

| File | Purpose |
|------|---------|
| `bronze_ingestion.py` | Bronze layer extraction |
| `silver_transform.py` | Silver layer cleaning |
| `gold_aggregation.py` | Gold layer aggregation |
| `regulatory_reports.py` | SBV regulatory reports |
| `cdc_kafka_ingestion.py` | Real-time CDC ingestion |

---

*Part of: [Lakehouse Platform Project](../../README.md)*
