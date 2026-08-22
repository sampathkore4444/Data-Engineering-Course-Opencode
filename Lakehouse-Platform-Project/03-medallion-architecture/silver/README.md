# Silver Layer - Cleansed & Validated Data

## Purpose
The Silver layer contains cleaned, deduplicated, validated, and conformed data. All data quality issues from Bronze are resolved here.

## Design Principles

| Principle | Description |
|-----------|-------------|
| **Deduplicated** | Only latest record kept per primary key |
| **Validated** | Data types enforced, constraints checked |
| **Conformed** | Standardized codes, names, formats |
| **Enriched** | Business rules applied (e.g., risk flags) |
| **Quarantined** | Bad records moved to `_quarantine` |

## Data Flow

```
Bronze (Raw)  →  Deduplication  →  Validation  →  Conformance  →  Silver (Clean)
```

## Silver Tables

| Table | Source | Key Transformations |
|-------|--------|---------------------|
| `silver.core_banking_customers` | Bronze customers | Dedup, validate email/phone, standardize names |
| `silver.core_banking_accounts` | Bronze accounts | Dedup, validate balances, standardize status |
| `silver.core_banking_transactions` | Bronze transactions | Dedup, validate amounts, add time buckets |
| `silver.credit_cards` | Bronze cards | Dedup, mask card numbers, calculate utilization |
| `silver.credit_card_transactions` | Bronze card txns | Dedup, risk flags, merchant categorization |
| `silver.loan_accounts` | Bronze loans | Dedup, validate rates, standardize types |
| `silver.loan_payments` | Bronze payments | Dedup, validate amounts, standardize modes |

## Data Quality Rules

| Rule | Threshold | Action on Failure |
|------|-----------|-------------------|
| **Uniqueness** | Primary key = 100% unique | Reject duplicate |
| **Completeness** | Required fields = 99.9% non-null | Log warning |
| **Validity** | Values within expected range | Quarantine record |
| **Consistency** | Cross-table FK validation | Log warning |
| **Timeliness** | Data < 24 hours old | Alert ops team |

## Quarantine Process

```
Bronze → Validation Failed → _quarantine/ → Manual Review → Fix & Re-ingest
```

## Partitioning

```
silver/
├── core_banking/
│   └── transactions/
│       ├── txn_date=2024-01-15/
│       └── txn_date=2024-01-16/
├── credit_cards/
│   └── card_transactions/
│       └── txn_date=2024-01-15/
└── loans/
    └── loan_payments/
        └── payment_date=2024-01-15/
```
