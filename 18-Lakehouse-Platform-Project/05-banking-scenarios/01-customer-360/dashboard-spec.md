# Customer 360° Dashboard Specification

## Overview
A comprehensive dashboard providing relationship managers with a complete view of customer relationships across all banking products.

## Target Users
- Relationship Managers
- Branch Managers
- Customer Service Officers

## Dashboard Layout

### Header Section
| Element | Description | Source |
|---------|-------------|--------|
| Customer Name | Full legal name | core_banking_customers |
| Customer ID | Unique identifier | core_banking_customers |
| Customer Segment | Platinum/Gold/Silver/Bronze | Computed |
| Relationship Since | Account opening date | core_banking_customers |
| Risk Rating | Low/Medium/High | Computed |

### Section 1: Account Overview
```
┌─────────────────────────────────────────────────────────────┐
│                    ACCOUNT OVERVIEW                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Total Accounts: 3                                          │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐                       │
│  │ Savings │ │ Current │ │  FD     │                       │
│  │ 150M    │ │ 25M     │ │ 500M   │                       │
│  │ VND     │ │ VND     │ │ VND     │                       │
│  └─────────┘ └─────────┘ └─────────┘                       │
│                                                             │
│  Total Balance: 675,000,000 VND                             │
│  Available Balance: 625,000,000 VND                         │
│                                                             │
│  [Chart: Balance Trend - Last 12 Months]                    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Section 2: Credit Cards
```
┌─────────────────────────────────────────────────────────────┐
│                    CREDIT CARDS                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Card 1: XXXX-XXXX-XXXX-1234 (Visa)                        │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ ████████████████████░░░░░░░░░░░░ 60% utilized       │   │
│  │ Limit: 50M VND  |  Used: 30M VND  |  Avail: 20M VND│   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Card 2: XXXX-XXXX-XXXX-5678 (Mastercard)                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ ████████░░░░░░░░░░░░░░░░░░░░░░░░ 25% utilized       │   │
│  │ Limit: 30M VND  |  Used: 7.5M VND | Avail: 22.5M   │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Total Limit: 80M VND                                       │
│  Total Outstanding: 37.5M VND                               │
│  Avg Utilization: 46.9%                                     │
│                                                             │
│  [Chart: Monthly Spend - Last 6 Months]                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Section 3: Loans
```
┌─────────────────────────────────────────────────────────────┐
│                    LOANS                                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Home Loan: HL-001                                          │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Principal: 2,000M VND                                │   │
│  │ Outstanding: 1,500M VND                              │   │
│  │ Interest Rate: 9.5%                                  │   │
│  │ EMI: 16,800,000 VND/month                            │   │
│  │ Tenure: 20 years (Remaining: 15 years)               │   │
│  │ Payment Success Rate: 98%                            │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  [Chart: Payment History - Last 12 Months]                  │
│  [Chart: Principal vs Interest Breakdown]                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Section 4: Transaction Activity
```
┌─────────────────────────────────────────────────────────────┐
│                TRANSACTION ACTIVITY (Last 30 Days)          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Total Transactions: 45                                     │
│  Total Credits: 150,000,000 VND                             │
│  Total Debits: 120,000,000 VND                              │
│  Net Flow: +30,000,000 VND                                  │
│                                                             │
│  [Chart: Daily Transaction Volume]                          │
│  [Chart: Transaction by Channel]                            │
│  ┌─────────┬─────────┬─────────┬─────────┐                 │
│  │ Mobile  │ Online  │  ATM    │ Branch  │                 │
│  │  45%    │  30%    │  15%    │  10%    │                 │
│  └─────────┴─────────┴─────────┴─────────┘                 │
│                                                             │
│  [Table: Recent Transactions]                               │
│  Date       | Type   | Amount      | Channel | Description  │
│  2024-01-15 | Credit | 5,000,000   | Mobile  | Salary       │
│  2024-01-14 | Debit  | 2,500,000   | Online  | Bill Payment │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Section 5: Relationship Summary
```
┌─────────────────────────────────────────────────────────────┐
│              RELATIONSHIP SUMMARY                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Net Relationship Value: 675,000,000 VND                    │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Assets: 755M VND (Accounts + Cards)                 │   │
│  │ Liabilities: 80M VND (Cards + Loans)                │   │
│  │ Net: 675M VND                                       │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Customer Segment: GOLD                                     │
│  Engagement Level: HIGH                                     │
│                                                             │
│  [Chart: Asset vs Liability Breakdown]                      │
│  [Chart: Value Trend - Last 12 Months]                      │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ RECOMMENDED ACTIONS                                  │   │
│  │ • Pre-approved loan: 500M VND available              │   │
│  │ • Premium card upgrade: Eligible                     │   │
│  │ • Investment products: High engagement, recommend    │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Key Metrics (KPIs)

| KPI | Formula | Target |
|-----|---------|--------|
| Total Relationship Value | Assets - Liabilities | > 500M VND |
| Card Utilization | Used / Limit * 100 | 30-70% |
| Payment Success Rate | Successful / Total * 100 | > 95% |
| Transaction Frequency | Count / 30 days | > 10 |
| Engagement Level | Based on txn frequency | HIGH |

## Filters

| Filter | Options | Default |
|--------|---------|---------|
| Date Range | Last 7/30/90 days, Custom | Last 30 days |
| Product Type | All, Savings, Cards, Loans | All |
| Transaction Type | All, Credit, Debit | All |

## Refresh Schedule

| Data | Frequency | Method |
|------|-----------|--------|
| Account Balances | Real-time | Dremio Reflection |
| Transaction Data | Hourly | Dremio Reflection |
| Loan Data | Daily | Dremio Reflection |
| Card Data | Real-time | Dremio Reflection |
