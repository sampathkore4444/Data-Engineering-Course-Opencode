# ADR-002: Medallion Architecture Adoption

## Status
Accepted

## Date
2024-01-15

## Context

We need to organize our banking data in a way that:
1. Preserves raw data for debugging and reprocessing
2. Provides clean, validated data for analytics
3. Delivers business-ready data for BI and ML
4. Supports both batch and streaming workloads
5. Enables data quality at each layer

## Decision

We will adopt the **Medallion Architecture** (Bronze-Silver-Gold) for our data lakehouse:

### 1. Bronze Layer (Raw)
- Store raw data as-is from source systems
- Append-only, no transformations
- Preserve original format for reprocessing
- Retain for 90 days, then archive

### 2. Silver Layer (Cleansed)
- Clean, validate, and deduplicate data
- Apply schema enforcement
- Standardize codes and formats
- Move bad records to quarantine

### 3. Gold Layer (Business-Ready)
- Pre-aggregated, business logic applied
- Star schemas for BI tools
- Materialized views for performance
- Governed access at view level

## Rationale

### Why Medallion Architecture?

| Benefit | Description |
|---------|-------------|
| **Clear Separation** | Each layer has distinct purpose |
| **Data Quality** | Quality checks at each layer |
| **Debugging** | Easy to trace from Gold to Bronze |
| **Flexibility** | Support both batch and streaming |
| **Governance** | Different policies per layer |
| **Performance** | Optimized for different workloads |

### Why Not Other Approaches?

| Alternative | Why Not |
|-------------|---------|
| **Single Layer** | Mixes raw and processed data |
| **Two Layers** | Insufficient quality controls |
| **Data Vault** | Too complex for our needs |
| **Kimball** | Not optimized for data lake |

## Implementation

### Bronze Layer
```sql
-- Raw data ingestion
CREATE TABLE bronze.core_banking_transactions (
    txn_id BIGINT,
    account_id VARCHAR(20),
    amount DECIMAL(18,2),
    txn_date DATE,
    -- Raw columns as-is from source
)
PARTITION BY (txn_date);
```

### Silver Layer
```sql
-- Cleansed and validated
CREATE VIEW silver.core_banking_transactions AS
SELECT 
    txn_id,
    account_id,
    ABS(amount) AS amount,  -- Validate
    txn_date,
    UPPER(TRIM(txn_type)) AS txn_type_standardized,  -- Standardize
    CURRENT_TIMESTAMP AS cleaned_at
FROM bronze.core_banking_transactions
WHERE txn_id IS NOT NULL  -- Filter invalid
  AND amount > 0;
```

### Gold Layer
```sql
-- Business-ready aggregation
CREATE VIEW gold.daily_transaction_summary AS
SELECT 
    txn_date,
    channel_standardized,
    txn_type_standardized,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM silver.core_banking_transactions
GROUP BY txn_date, channel_standardized, txn_type_standardized;
```

## Consequences

### Positive
1. **Data Quality** - Quality checks at each layer
2. **Debugging** - Easy to trace data issues
3. **Performance** - Optimized for different workloads
4. **Flexibility** - Support batch and streaming
5. **Governance** - Different policies per layer

### Negative
1. **Complexity** - Three layers to manage
2. **Storage** - Data stored in multiple formats
3. **Maintenance** - ETL pipelines for each layer
4. **Learning Curve** - Team needs training

### Risks
1. **Data Duplication** - Mitigate with proper retention
2. **ETL Failures** - Mitigate with monitoring and alerts
3. **Schema Changes** - Mitigate with schema evolution

## Review Date
2024-07-15 (6 months after implementation)
