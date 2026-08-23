# 🏦 Banking Scenarios - Data Warehouse

> **Real-world banking use cases implemented as SQL views and queries**

---

## 📋 Table of Contents

1. [Overview](#1-overview)
2. [Scenario Summary](#2-scenario-summary)
3. [Scenario 1: Customer Analytics](#3-scenario-1-customer-analytics)
4. [Scenario 2: Financial Reporting](#4-scenario-2-financial-reporting)
5. [Scenario 3: Regulatory Reports](#5-scenario-3-regulatory-reports)
6. [Scenario 4: Executive Dashboards](#6-scenario-4-executive-dashboards)
7. [How Scenarios Map to Star Schema](#7-how-scenarios-map-to-star-schema)
8. [Running the Scenarios](#8-running-the-scenarios)

---

## 1. Overview

This folder contains **4 real-world banking scenarios** that demonstrate how to use the Data Warehouse for practical business needs. Each scenario includes SQL views and queries that can be used directly in production.

### What are Banking Scenarios?

| Term | Meaning | Example |
|------|---------|---------|
| **Customer Analytics** | Understand customer behavior | Customer 360° view, segmentation |
| **Financial Reporting** | Generate financial statements | P&L, balance sheet, regional analysis |
| **Regulatory Reports** | Comply with regulations | Basel III, AML monitoring |
| **Executive Dashboards** | High-level KPIs for leadership | CEO dashboard, branch performance |

---

## 2. Scenario Summary

| Scenario | Folder | Files | Primary Use Case |
|----------|--------|-------|------------------|
| **Customer Analytics** | `01-customer-analytics/` | 1 SQL file | Relationship managers, call center |
| **Financial Reporting** | `02-financial-reporting/` | 1 SQL file | Finance team, management |
| **Regulatory Reports** | `03-regulatory-reports/` | README | Compliance, auditors |
| **Executive Dashboards** | `04-executive-dashboards/` | README | C-suite, board members |

### Scenario Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    BANKING SCENARIOS ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    DATA WAREHOUSE (Star Schema)                 │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │   │
│  │  │dim_      │  │dim_      │  │dim_      │  │fact_     │      │   │
│  │  │customer  │  │account   │  │date      │  │trans     │      │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                 │
│                    ┌─────────────────┼─────────────────┐               │
│                    ▼                 ▼                 ▼               │
│  ┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────┐  │
│  │  CUSTOMER ANALYTICS │ │  FINANCIAL REPORTS  │ │   REGULATORY   │  │
│  │                     │ │                     │ │                │  │
│  │  • Customer 360°   │ │  • P&L Statement    │ │  • Basel III   │  │
│  │  • Segmentation    │ │  • Regional P&L     │ │  • AML Monitor │  │
│  │  • Activity Analysis│ │  • Product Perf.    │ │  • Call Report │  │
│  └─────────────────────┘ └─────────────────────┘ └─────────────────┘  │
│                    │                 │                 │               │
│                    └─────────────────┼─────────────────┘               │
│                                      ▼                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    EXECUTIVE DASHBOARDS                         │   │
│  │  • CEO Dashboard                                                │   │
│  │  • Branch Performance                                           │   │
│  │  • Risk Overview                                                │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Scenario 1: Customer Analytics

### Purpose

Provide a **unified 360° view of each customer** across all banking products (accounts, cards, loans).

### Files

| File | Purpose |
|------|---------|
| `customer_360.sql` | Customer 360° view, segmentation, activity analysis |

### Views Created

| View | Description | Use Case |
|------|-------------|----------|
| `vw_customer_360` | Complete customer profile with all products | Call center, relationship managers |
| `vw_customer_segmentation` | Customer segments with aggregates | Marketing, risk analysis |

### What It Answers

| Business Question | How It Answers |
|-------------------|----------------|
| "What is the customer's total relationship?" | Shows all accounts, balances, loans |
| "Which customers are high-value?" | Segments by deposit balance and transaction volume |
| "Which customers have NPA (non-performing assets)?" | Flags customers with overdue payments |
| "How active are customers this month?" | Transaction count and amount by segment |

### Example Queries

```sql
-- Get complete customer profile
SELECT * FROM vw_customer_360 
WHERE customer_id = 'CUST-001';

-- Get top 10 customers by balance
SELECT customer_id, customer_name, total_deposit_balance
FROM vw_customer_360
ORDER BY total_deposit_balance DESC
LIMIT 10;

-- Customer segmentation summary
SELECT * FROM vw_customer_segmentation;

-- Monthly activity by segment
SELECT customer_segment, month_name, transaction_count, total_amount
FROM vw_customer_activity
ORDER BY month_number, customer_segment;
```

### Key Metrics

| Metric | Description |
|--------|-------------|
| `total_accounts` | Number of accounts per customer |
| `total_deposit_balance` | Savings + Fixed Deposit balance |
| `total_transactions_30d` | Transactions in last 30 days |
| `total_loan_outstanding` | Outstanding loan amount |
| `has_npa` | Flag if customer has NPA |

---

## 4. Scenario 2: Financial Reporting

### Purpose

Generate **financial statements** (P&L, regional analysis, product performance) for management reporting.

### Files

| File | Purpose |
|------|---------|
| `pnl_report.sql` | Monthly P&L, regional P&L, product performance |

### Views Created

| View | Description | Use Case |
|------|-------------|----------|
| `vw_monthly_pnl` | Monthly P&L statement | Finance team, management |
| `vw_regional_pnl` | P&L by region and branch | Regional managers |
| `vw_product_performance` | Product-wise performance | Product managers |

### What It Answers

| Business Question | How It Answers |
|-------------------|----------------|
| "What is the total revenue this month?" | Interest income + fee income |
| "Which region is most profitable?" | Regional P&L breakdown |
| "Which products generate the most income?" | Product performance ranking |
| "What is the loan portfolio size?" | Total principal outstanding |

### Example Queries

```sql
-- Monthly P&L summary
SELECT * FROM vw_monthly_pnl 
WHERE year = 2024;

-- Regional P&L for Q1
SELECT * FROM vw_regional_pnl 
WHERE quarter_name = 'Q1-2024';

-- Product performance ranking
SELECT product_name, total_balance, avg_interest_rate
FROM vw_product_performance
ORDER BY total_balance DESC;

-- Executive summary
SELECT 'Total Revenue' AS metric, SUM(total_revenue) AS value
FROM vw_monthly_pnl WHERE year = 2024;
```

### Key Metrics

| Metric | Description |
|--------|-------------|
| `total_interest_income` | Interest earned from all loans |
| `total_fee_income` | Transaction fees collected |
| `total_revenue` | Interest + Fee income |
| `total_transaction_volume` | Total money transacted |
| `loan_portfolio` | Total outstanding loans |

---

## 5. Scenario 3: Regulatory Reports

### Purpose

Generate reports required by **State Bank of Vietnam (SBV)** and other regulatory authorities.

### Files

| File | Purpose |
|------|---------|
| `README.md` | Documentation for regulatory report requirements |

### Regulatory Reports

| Report | Frequency | Authority | Purpose |
|--------|-----------|-----------|---------|
| **Basel III** | Quarterly | SBV | Capital adequacy |
| **AML Monitoring** | Daily | SBV | Anti-money laundering |
| **Call Report** | Quarterly | SBV | Financial health |
| **Credit Risk** | Monthly | SBV | Loan portfolio risk |

### What It Answers

| Business Question | How It Answers |
|-------------------|----------------|
| "Are we Basel III compliant?" | Capital adequacy ratio calculation |
| "Are there suspicious transactions?" | AML rule-based detection |
| "What is the NPA ratio?" | Non-performing assets / Total loans |
| "What is the provision coverage?" | Provisions / NPA |

### Basel III Example

```sql
-- Capital Adequacy Ratio (CAR)
SELECT 
    SUM(risk_weighted_assets) AS total_rwa,
    SUM(core_capital) AS tier1_capital,
    SUM(core_capital) / SUM(risk_weighted_assets) * 100 AS car_ratio
FROM regulatory.basel_iii_calculation
WHERE reporting_date = '2024-03-31';
```

### AML Monitoring Example

```sql
-- Suspicious transactions (high amount, unusual pattern)
SELECT 
    customer_id,
    transaction_id,
    transaction_amount,
    transaction_date,
    'HIGH_AMOUNT' AS alert_type
FROM dw.fact_transactions t
JOIN dw.dim_customer c ON t.customer_sk = c.customer_sk
WHERE t.transaction_amount > 500000000  -- 500M VND
  AND t.transaction_date >= CURRENT_DATE - INTERVAL '7 days';
```

---

## 6. Scenario 4: Executive Dashboards

### Purpose

Provide **high-level KPIs and dashboards** for C-suite executives and board members.

### Files

| File | Purpose |
|------|---------|
| `README.md` | Documentation for executive dashboard requirements |

### Dashboard Views

| Dashboard | Audience | Key Metrics |
|-----------|----------|-------------|
| **CEO Dashboard** | CEO, Board | Revenue, Growth, Risk, Customer count |
| **Branch Performance** | Regional Managers | Branch-wise deposits, loans, transactions |
| **Risk Overview** | Risk Committee | NPA ratio, Provision coverage, CAR |

### What It Answers

| Business Question | How It Answers |
|-------------------|----------------|
| "How is the bank performing overall?" | Revenue, profit, customer growth |
| "Which branches are underperforming?" | Branch-wise comparison |
| "What is the current risk exposure?" | NPA, provision, CAR metrics |
| "Are we meeting regulatory requirements?" | Compliance status |

### CEO Dashboard Example

```sql
-- CEO Dashboard KPIs
SELECT 
    'Total Customers' AS metric,
    COUNT(DISTINCT customer_sk) AS value
FROM dw.dim_customer WHERE is_current = TRUE

UNION ALL

SELECT 
    'Total Deposits',
    SUM(current_balance)
FROM dw.dim_account WHERE account_type IN ('SAVINGS', 'FIXED_DEPOSIT')

UNION ALL

SELECT 
    'Total Loans',
    SUM(principal_outstanding)
FROM dw.fact_loan_payment

UNION ALL

SELECT 
    'Monthly Revenue',
    SUM(total_revenue)
FROM vw_monthly_pnl 
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE) 
  AND month_number = EXTRACT(MONTH FROM CURRENT_DATE);
```

### Branch Performance Example

```sql
-- Branch performance ranking
SELECT 
    branch_name,
    region,
    COUNT(DISTINCT customer_sk) AS customer_count,
    SUM(current_balance) AS total_deposits,
    SUM(principal_outstanding) AS total_loans,
    COUNT(DISTINCT transaction_sk) AS transaction_count
FROM dw.dim_branch b
LEFT JOIN dw.dim_account a ON b.branch_sk = a.branch_sk
LEFT JOIN dw.fact_loan_payment lp ON b.branch_sk = lp.branch_sk
LEFT JOIN dw.fact_transactions t ON b.branch_sk = t.branch_sk
GROUP BY branch_name, region
ORDER BY total_deposits DESC;
```

---

## 7. How Scenarios Map to Star Schema

### Dimension and Fact Usage

| Scenario | Dimensions Used | Facts Used |
|----------|-----------------|------------|
| **Customer Analytics** | dim_customer, dim_account, dim_date | fact_transactions, fact_loan_payment |
| **Financial Reporting** | dim_date, dim_branch, dim_product | fact_loan_payment, fact_transactions |
| **Regulatory Reports** | dim_customer, dim_date | fact_transactions, fact_loan_payment, fact_account_balance |
| **Executive Dashboards** | dim_branch, dim_date | All facts |

### Star Schema Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    STAR SCHEMA - BANKING DATA WAREHOUSE                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│                      ┌──────────────┐                                  │
│                      │  dim_date    │                                  │
│                      │──────────────│                                  │
│                      │ date_key (PK)│                                  │
│                      │ full_date    │                                  │
│                      │ month_name   │                                  │
│                      │ quarter_name │                                  │
│                      └──────┬───────┘                                  │
│                             │                                          │
│         ┌───────────────────┼───────────────────┐                     │
│         │                   │                   │                     │
│         ▼                   ▼                   ▼                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │
│  │dim_customer  │  │dim_account   │  │dim_branch    │               │
│  │──────────────│  │──────────────│  │──────────────│               │
│  │customer_sk   │  │account_sk    │  │branch_sk     │               │
│  │customer_id   │  │account_id    │  │branch_name   │               │
│  │customer_name │  │account_type  │  │region        │               │
│  │segment       │  │balance       │  └──────┬───────┘               │
│  └──────┬───────┘  └──────┬───────┘         │                       │
│         │                 │                 │                       │
│         └────────┬────────┴────────┬────────┘                       │
│                  ▼                 ▼                                 │
│           ┌───────────┐    ┌───────────┐                            │
│           │fact_      │    │fact_      │                            │
│           │transactions│   │loan_payment│                           │
│           │───────────│    │───────────│                            │
│           │txn_sk (PK)│   │payment_sk │                            │
│           │customer_sk│   │customer_sk│                            │
│           │date_key   │    │date_key   │                            │
│           │amount     │    │amount     │                            │
│           └───────────┘    └───────────┘                            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Running the Scenarios

### Prerequisites

```bash
# Ensure Data Warehouse is running
docker-compose up -d

# Ensure star schema is loaded
psql -h localhost -U postgres -d banking_dw -f 03-star-schema/scripts/create_schema.sql
```

### Running Customer Analytics

```bash
# Connect to Data Warehouse
psql -h localhost -U postgres -d banking_dw

# Run Customer 360 queries
\i 05-banking-scenarios/01-customer-analytics/customer_360.sql
```

### Running Financial Reports

```bash
# Run P&L queries
\i 05-banking-scenarios/02-financial-reporting/pnl_report.sql
```

### Query Examples

```sql
-- Customer 360 view
SELECT * FROM vw_customer_360 WHERE customer_id = 'CUST-001';

-- Monthly P&L
SELECT * FROM vw_monthly_pnl WHERE year = 2024;

-- Regional performance
SELECT * FROM vw_regional_pnl WHERE quarter_name = 'Q1-2024';

-- Product ranking
SELECT * FROM vw_product_performance ORDER BY total_balance DESC;

-- Customer segmentation
SELECT * FROM vw_customer_segmentation;
```

---

## 📊 Summary

| Scenario | Views | Key Metrics | Primary Users |
|----------|-------|-------------|---------------|
| **Customer Analytics** | 3 | Customer 360, Segmentation, Activity | Relationship Managers, Call Center |
| **Financial Reporting** | 3 | P&L, Regional P&L, Product Perf. | Finance Team, Management |
| **Regulatory Reports** | 4 | Basel III, AML, Call Report, Credit Risk | Compliance, Auditors |
| **Executive Dashboards** | 3 | CEO KPIs, Branch Perf., Risk Overview | C-Suite, Board Members |

### Key Takeaways

1. **Customer Analytics** - Understand customer behavior across all products
2. **Financial Reporting** - Generate accurate financial statements
3. **Regulatory Reports** - Comply with SBV and other regulations
4. **Executive Dashboards** - Provide high-level KPIs for leadership

### Next Steps

- [ ] Add more regulatory reports (Basel III, AML)
- [ ] Create executive dashboard views
- [ ] Add automated report generation via Airflow
- [ ] Connect to BI tools (Grafana, Power BI)

---

*Built with ❤️ for Data Engineers learning Banking Data Warehouse scenarios*
