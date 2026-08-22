# Bronze Layer - Raw Data Ingestion

## Purpose
The Bronze layer stores raw data as-is from source systems. No transformations are applied. This preserves the original data format for debugging and reprocessing.

## Design Principles

| Principle | Description |
|-----------|-------------|
| **Append-Only** | Data is never updated or deleted in Bronze |
| **Schema-on-Read** | Schema is applied when reading, not when writing |
| **Immutable** | Original data is preserved exactly as received |
| **Partitioned by Ingestion Date** | Easy to track when data arrived |
| **Retained for 90 Days** | Then archived to cold storage |

## Directory Structure

```
bronze/
├── core-banking/          # From Oracle → Mainframe
│   ├── accounts/          # account_id, customer_id, balance, etc.
│   ├── transactions/      # txn_id, amount, type, timestamp
│   └── customers/         # customer_id, name, dob, etc.
├── credit-cards/          # From Mainframe → Cards System
│   ├── cards/             # card_number, card_type, limit
│   ├── card_transactions/ # txn_id, amount, merchant, timestamp
│   └── statements/        # statement_id, billing_cycle, total
├── loans/                 # From SQL Server → Loan System
│   ├── loan_accounts/     # loan_id, customer_id, principal
│   ├── loan_payments/     # payment_id, amount, date
│   └── loan_applications/ # application_id, status, amount
└── metadata/
    ├── ingestion_log.json  # Track what was ingested
    └── schema_versions.json # Track schema evolution
```

## Partitioning Strategy

```
bronze/
├── core-banking/
│   └── transactions/
│       ├── ingestion_date=2024-01-15/
│       │   ├── part-00001.parquet
│       │   └── part-00002.parquet
│       └── ingestion_date=2024-01-16/
│           └── part-00001.parquet
```

## Ingestion Tracking

```json
{
  "ingestion_id": "ING-20240115-001",
  "source_system": "core-banking",
  "source_table": "transactions",
  "target_path": "bronze/core-banking/transactions/ingestion_date=2024-01-15/",
  "row_count": 125000,
  "file_count": 3,
  "schema_version": "v2.1",
  "ingestion_start": "2024-01-15T02:00:00Z",
  "ingestion_end": "2024-01-15T02:15:32Z",
  "status": "SUCCESS",
  "checksum": "sha256:abc123..."
}
```

## Data Quality Rules (Minimal)

| Rule | Description | Action on Failure |
|------|-------------|-------------------|
| **File Format Check** | Ensure file is valid Parquet/JSON/CSV | Reject and alert |
| **Non-Empty** | File must contain at least 1 row | Log warning, continue |
| **Column Count** | Must match expected column count | Reject and alert |
| **No Null Primary Key** | Primary key columns cannot be null | Log and move to quarantine |

## Retention Policy

| Timeframe | Storage Tier | Cost |
|-----------|-------------|------|
| 0-30 days | Hot (S3 Standard) | High |
| 30-60 days | Warm (S3 IA) | Medium |
| 60-90 days | Cold (S3 Glacier) | Low |
| 90+ days | Delete (or archive) | Free |
