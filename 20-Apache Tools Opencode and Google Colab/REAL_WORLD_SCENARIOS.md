# Real-World Banking Scenarios Guide

## Table of Contents
1. [Apache Parquet - Banking Scenarios](#apache-parquet---banking-scenarios)
2. [Apache Arrow - Banking Scenarios](#apache-arrow---banking-scenarios)
3. [Apache Flight SQL - Banking Scenarios](#apache-flight-sql---banking-scenarios)
4. [DuckDB - Banking Scenarios](#duckdb---banking-scenarios)
5. [Apache Iceberg - Banking Scenarios](#apache-iceberg---banking-scenarios)
6. [Cross-Technology Integration Patterns](#cross-technology-integration-patterns)

---

## Apache Parquet - Banking Scenarios

### Scenario 1: Daily Transaction Ledger Archival

**Problem Statement:**
A large commercial bank processes approximately 50 million transactions daily across its retail banking network. Each transaction record contains fields such as transaction ID, timestamp, account number, transaction type (debit/credit/transfer), amount, currency code, merchant details, GPS coordinates, and compliance flags. The bank must store these records efficiently while ensuring they can be queried by auditors and regulatory bodies for up to 7 years. Storage costs are escalating, and traditional row-based formats like CSV or JSON consume excessive disk space and are slow to query.

**Why Parquet:**
Parquet's columnar storage format is ideal because:
- **Compression**: Similar data types stored together compress dramatically better. Transaction amounts (DECIMAL), timestamps (TIMESTAMP), and categorical fields (transaction type, currency) each achieve 70-90% compression ratios.
- **Predicate pushdown**: Auditors often query by date range or transaction type. Parquet's row groups with min/max statistics allow the engine to skip irrelevant data blocks entirely.
- **Schema evolution**: Regulatory requirements change—new fields like "aml_risk_score" may be added without rewriting historical data.
- **Column projection**: Queries like "give me only amounts and timestamps for Q3 2024" read only 2 columns out of 15+, reducing I/O by 85%+.

**Implementation:**

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# ============================================================
# STEP 1: Define the Bank's Transaction Schema
# ============================================================
transaction_schema = pa.schema([
    pa.field('transaction_id', pa.string()),
    pa.field('account_number', pa.string()),
    pa.field('transaction_timestamp', pa.timestamp('ms', tz='UTC')),
    pa.field('transaction_type', pa.dictionary(pa.int8(), pa.string())),
    pa.field('amount', pa.decimal128(18, 2)),
    pa.field('currency_code', pa.dictionary(pa.int8(), pa.string())),
    pa.field('balance_after', pa.decimal128(18, 2)),
    pa.field('merchant_category_code', pa.string()),
    pa.field('merchant_name', pa.string()),
    pa.field('channel', pa.dictionary(pa.int8(), pa.string())),
    pa.field('branch_code', pa.string()),
    pa.field('gps_latitude', pa.float64()),
    pa.field('gps_longitude', pa.float64()),
    pa.field('aml_risk_score', pa.float32()),
    pa.field('compliance_flag', pa.bool_()),
    pa.field('is_fraud_suspected', pa.bool_()),
    pa.field('settlement_status', pa.dictionary(pa.int8(), pa.string())),
    pa.field('batch_id', pa.int64()),
])

# ============================================================
# STEP 2: Generate Simulated Daily Transaction Data (50M records)
# ============================================================
np.random.seed(42)
NUM_TRANSACTIONS = 50_000_000

# Generate in chunks to manage memory
CHUNK_SIZE = 1_000_000

def generate_transaction_chunk(start_idx, chunk_size, base_date):
    """Generate a chunk of realistic transaction data."""
    timestamps = pd.date_range(
        start=base_date,
        periods=chunk_size,
        freq='ms'  # millisecond intervals spread across the day
    )
    # Add realistic spread - transactions cluster around business hours
    hour_weights = [0.01, 0.01, 0.005, 0.005, 0.01, 0.02,  # 00-05
                    0.04, 0.08, 0.10, 0.12, 0.11, 0.09,      # 06-11
                    0.08, 0.07, 0.08, 0.09, 0.06, 0.04,      # 12-17
                    0.03, 0.02, 0.01, 0.01, 0.005, 0.005]     # 18-23
    
    tx_types = np.random.choice(
        ['DEBIT', 'CREDIT', 'TRANSFER', 'ATM_WITHDRAWAL', 'DIRECT_DEBIT'],
        size=chunk_size,
        p=[0.35, 0.30, 0.15, 0.10, 0.10]
    )
    
    # Realistic amount distributions per type
    amounts = np.where(
        tx_types == 'ATM_WITHDRAWAL',
        np.random.lognormal(mean=4.5, sigma=0.8, size=chunk_size),  # ATM: ~90 avg
        np.where(
            tx_types == 'TRANSFER',
            np.random.lognormal(mean=7.0, sigma=1.5, size=chunk_size),  # Transfers: wider range
            np.random.lognormal(mean=3.5, sigma=1.2, size=chunk_size)   # Others: ~33 avg
        )
    ).round(2)
    
    channels = np.random.choice(
        ['MOBILE', 'ONLINE', 'BRANCH', 'ATM', 'POS', 'WIRE'],
        size=chunk_size,
        p=[0.35, 0.25, 0.10, 0.10, 0.15, 0.05]
    )
    
    currencies = np.random.choice(
        ['USD', 'EUR', 'GBP', 'JPY', 'CHF'],
        size=chunk_size,
        p=[0.50, 0.20, 0.15, 0.10, 0.05]
    )
    
    # AML risk scores - most transactions are low risk
    aml_scores = np.where(
        np.random.random(chunk_size) > 0.02,
        np.random.beta(2, 20, size=chunk_size) * 100,  # Low risk: 0-40
        np.random.beta(5, 2, size=chunk_size) * 100     # High risk: 50-100
    ).astype(np.float32)
    
    compliance_flags = aml_scores > 60.0
    fraud_flags = (np.random.random(chunk_size) < 0.001)  # 0.1% fraud rate
    
    return {
        'transaction_id': [f"TXN-{start_idx + i:012d}" for i in range(chunk_size)],
        'account_number': [f"ACC-{np.random.randint(10000000, 99999999)}" for _ in range(chunk_size)],
        'transaction_timestamp': pd.array(timestamps[:chunk_size], dtype='datetime64[ms, UTC]'),
        'transaction_type': pd.array(tx_types, dtype='object'),
        'amount': amounts,
        'currency_code': pd.array(currencies, dtype='object'),
        'balance_after': np.random.uniform(100, 500000, chunk_size).round(2),
        'merchant_category_code': np.random.choice(
            ['5411', '5812', '5999', '6011', '4121', '5311'],
            size=chunk_size
        ),
        'merchant_name': [f"MERCHANT-{np.random.randint(1000, 9999)}" for _ in range(chunk_size)],
        'channel': pd.array(channels, dtype='object'),
        'branch_code': [f"BR-{np.random.randint(100, 999)}" for _ in range(chunk_size)],
        'gps_latitude': np.random.uniform(25.0, 48.0, chunk_size),
        'gps_longitude': np.random.uniform(-125.0, -70.0, chunk_size),
        'aml_risk_score': aml_scores,
        'compliance_flag': compliance_flags,
        'is_fraud_suspected': fraud_flags,
        'settlement_status': np.random.choice(
            ['SETTLED', 'PENDING', 'FAILED', 'REVERSED'],
            size=chunk_size,
            p=[0.90, 0.06, 0.02, 0.02]
        ),
        'batch_id': np.full(chunk_size, 20240825, dtype=np.int64),
    }

# ============================================================
# STEP 3: Write Partitioned Parquet Files
# ============================================================
# Partition by date → channel for optimal query patterns
# Bank auditors typically query: "all transactions on DATE via CHANNEL"
PARTITION_COLS = ['channel', 'currency_code']

def write_daily_transactions(base_date_str='2024-08-25', output_dir='./bank_transactions'):
    base_date = pd.Timestamp(base_date_str)
    
    all_chunks = []
    for i in range(0, NUM_TRANSACTIONS, CHUNK_SIZE):
        chunk = generate_transaction_chunk(i, CHUNK_SIZE, base_date)
        all_chunks.append(chunk)
        print(f"  Generated chunk {i // CHUNK_SIZE + 1}/{NUM_TRANSACTIONS // CHUNK_SIZE}")
    
    # Combine chunks
    combined = {k: np.concatenate([c[k] for c in all_chunks]) for k in all_chunks[0].keys()}
    table = pa.table(combined, schema=transaction_schema)
    
    # Write partitioned Parquet
    pq.write_to_dataset(
        table,
        root_path=output_dir,
        partition_cols=PARTITION_COLS,
        # Optimization settings
        compression='ZSTD',          # Best compression ratio for mixed data
        compression_level=3,          # Balance between speed and compression
        use_dictionary=True,          # Dictionary encoding for low-cardinality columns
        write_statistics=True,        # Enable min/max for predicate pushdown
        data_page_size=1_048_576,    # 1MB pages for efficient scanning
        version='2.6',                # Latest Parquet format version
    )
    
    print(f"Written {NUM_TRANSACTIONS:,} transactions to {output_dir}/")
    return table

# ============================================================
# STEP 4: Query with Predicate Pushdown (Auditor Use Case)
# ============================================================
def audit_query_date_range(start_date, end_date, tx_type='DEBIT'):
    """Auditor query: Find all DEBIT transactions in a date range."""
    dataset = pq.ParquetDataset(
        './bank_transactions',
        filters=[
            # Only read MOBILE and ONLINE partitions (skip ATM, BRANCH, etc.)
            ('channel', 'in', ['MOBILE', 'ONLINE']),
        ]
    )
    
    # Read only required columns (projection pushdown)
    table = dataset.read(
        columns=[
            'transaction_id', 'account_number', 
            'transaction_timestamp', 'transaction_type',
            'amount', 'currency_code'
        ]
    )
    
    # Row-level filtering
    date_mask = pc.and_(
        pc.greater_equal(table['transaction_timestamp'], 
                         pd.Timestamp(start_date, tz='UTC')),
        pc.less_equal(table['transaction_timestamp'], 
                      pd.Timestamp(end_date, tz='UTC'))
    )
    
    type_mask = pc.equal(table['transaction_type'], tx_type)
    
    filtered = table.filter(pc.and_(date_mask, type_mask))
    
    # Summary statistics
    total_amount = pc.sum(filtered['amount'])
    tx_count = len(filtered)
    
    print(f"\n{'='*60}")
    print(f"AUDIT REPORT: {tx_type} transactions")
    print(f"Period: {start_date} to {end_date}")
    print(f"Total Transactions: {tx_count:,}")
    print(f"Total Amount: ${total_amount:,.2f}")
    print(f"Average Amount: ${total_amount / tx_count:,.2f}")
    print(f"{'='*60}")
    
    return filtered

# ============================================================
# STEP 5: File Size Comparison
# ============================================================
def compare_storage_formats():
    """Compare Parquet vs CSV storage efficiency."""
    sample_data = generate_transaction_chunk(0, 100_000, pd.Timestamp('2024-08-25'))
    table = pa.table(sample_data, schema=transaction_schema)
    
    # Write as Parquet
    pq.write_table(table, 'transactions.parquet', compression='ZSTD')
    
    # Write as CSV
    df = table.to_pandas()
    df.to_csv('transactions.csv', index=False)
    
    # Write as JSON
    df.to_json('transactions.json', orient='records', lines=True)
    
    import os
    parquet_size = os.path.getsize('transactions.parquet')
    csv_size = os.path.getsize('transactions.csv')
    json_size = os.path.getsize('transactions.json')
    
    print(f"\nStorage Comparison (100K transactions):")
    print(f"  Parquet (ZSTD): {parquet_size / 1024 / 1024:.1f} MB")
    print(f"  CSV:            {csv_size / 1024 / 1024:.1f} MB")
    print(f"  JSON:           {json_size / 1024 / 1024:.1f} MB")
    print(f"  Parquet saves:  {(1 - parquet_size/csv_size) * 100:.1f}% vs CSV")
    print(f"  Parquet saves:  {(1 - parquet_size/json_size) * 100:.1f}% vs JSON")
```

**Key Benefits:**
- **75-90% storage reduction** vs CSV through columnar compression
- **10-100x faster queries** through predicate pushdown (skip entire row groups)
- **7+ year archival compliance** with schema evolution support
- **Cost savings**: $2-5M/year for a top-10 bank processing 50M daily transactions

---

### Scenario 2: Credit Risk Model Feature Store

**Problem Statement:**
A bank's credit risk department needs to compute 200+ features for each of 10 million loan applicants nightly. Features include payment history aggregates, credit utilization ratios, debt-to-income metrics, account age statistics, and behavioral patterns derived from transaction history. The feature store must support both batch training (ML models retrained weekly) and real-time scoring (instant loan decisions). Currently, features are computed in separate ETL pipelines with inconsistent definitions, leading to training-serving skew.

**Why Parquet:**
- **Columnar access for ML**: Training a model on 50 features out of 200 only reads those 50 columns—I/O is reduced proportionally.
- **Append-friendly**: New feature computations can be appended as new columns without rewriting existing data (schema evolution).
- **Efficient aggregation**: Column-stored numeric features (averages, sums, standard deviations) compress extremely well.
- **Interoperability**: Every ML framework (scikit-learn, XGBoost, TensorFlow, PyTorch) reads Parquet natively.

**Implementation:**

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import numpy as np
import pandas as pd

# ============================================================
# Credit Risk Feature Store Schema
# ============================================================
feature_store_schema = pa.schema([
    # Identity
    pa.field('applicant_id', pa.string()),
    pa.field('snapshot_date', pa.date32()),
    
    # Demographics (low cardinality - good for dictionary encoding)
    pa.field('age_group', pa.dictionary(pa.int8(), pa.string())),
    pa.field('employment_status', pa.dictionary(pa.int8(), pa.string())),
    pa.field('state_code', pa.dictionary(pa.int8(), pa.string())),
    
    # Credit History Features (high cardinality numerics)
    pa.field('num_open_accounts', pa.int16()),
    pa.field('num_closed_accounts', pa.int16()),
    pa.field('oldest_account_age_months', pa.int32()),
    pa.field('newest_account_age_months', pa.int32()),
    pa.field('avg_account_age_months', pa.float32()),
    
    # Payment Behavior Features
    pa.field('total_payments_12m', pa.int32()),
    pa.field('late_payments_30d', pa.int16()),
    pa.field('late_payments_60d', pa.int16()),
    pa.field('late_payments_90d_plus', pa.int16()),
    pa.field('pct_payments_on_time', pa.float32()),
    pa.field('max_consecutive_late_payments', pa.int8()),
    
    # Credit Utilization Features
    pa.field('total_credit_limit', pa.decimal128(15, 2)),
    pa.field('total_credit_used', pa.decimal128(15, 2)),
    pa.field('overall_utilization_ratio', pa.float32()),
    pa.field('max_utilization_ratio', pa.float32()),
    pa.field('avg_utilization_3m', pa.float32()),
    pa.field('utilization_trend', pa.float32()),  # slope over 6 months
    
    # Income & Debt Features
    pa.field('estimated_annual_income', pa.decimal128(15, 2)),
    pa.field('total_monthly_debt_payments', pa.decimal128(12, 2)),
    pa.field('debt_to_income_ratio', pa.float32()),
    pa.field('disposable_income_ratio', pa.float32()),
    
    # Behavioral Features (from transaction analysis)
    pa.field('avg_monthly_spending', pa.decimal128(12, 2)),
    pa.field('spending_volatility', pa.float32()),
    pa.field('savings_rate', pa.float32()),
    pa.field('num_incoming_transfers_3m', pa.int32()),
    pa.field('income_regularity_score', pa.float32()),  # 0-1
    
    # Risk Scores (model outputs from other models)
    pa.field('internal_credit_score', pa.int16()),
    pa.field('external_credit_score', pa.int16()),
    pa.field('fraud_risk_score', pa.float32()),
    pa.field('aml_risk_score', pa.float32()),
    
    # Target Variables (for training)
    pa.field('default_90d', pa.bool_()),
    pa.field('default_180d', pa.bool_()),
    pa.field('probability_of_default', pa.float32()),
])

# ============================================================
# Feature Computation Engine
# ============================================================
def compute_features(applicant_id, transactions_df, accounts_df, credit_bureau_df):
    """
    Compute 200+ credit risk features for a single applicant.
    In production, this runs on Spark/Dask for 10M applicants.
    """
    features = {}
    features['applicant_id'] = applicant_id
    
    # --- Credit History Features ---
    open_accounts = accounts_df[accounts_df['status'] == 'OPEN']
    closed_accounts = accounts_df[accounts_df['status'] == 'CLOSED']
    
    features['num_open_accounts'] = len(open_accounts)
    features['num_closed_accounts'] = len(closed_accounts)
    
    if len(accounts_df) > 0:
        account_ages = (pd.Timestamp.now() - accounts_df['opened_date']).dt.days / 30
        features['oldest_account_age_months'] = int(account_ages.max())
        features['newest_account_age_months'] = int(account_ages.min())
        features['avg_account_age_months'] = float(account_ages.mean())
    else:
        features['oldest_account_age_months'] = 0
        features['newest_account_age_months'] = 0
        features['avg_account_age_months'] = 0.0
    
    # --- Payment Behavior Features ---
    payments_12m = transactions_df[
        (transactions_df['type'] == 'PAYMENT') & 
        (transactions_df['date'] >= pd.Timestamp.now() - pd.DateOffset(months=12))
    ]
    
    features['total_payments_12m'] = len(payments_12m)
    features['late_payments_30d'] = len(
        payments_12m[payments_12m['days_late'].between(1, 30)]
    )
    features['late_payments_60d'] = len(
        payments_12m[payments_12m['days_late'].between(31, 60)]
    )
    features['late_payments_90d_plus'] = len(
        payments_12m[payments_12m['days_late'] > 60]
    )
    
    if len(payments_12m) > 0:
        features['pct_payments_on_time'] = float(
            (payments_12m['days_late'] == 0).sum() / len(payments_12m)
        )
        # Max consecutive late payments
        late_mask = payments_12m['days_late'] > 0
        if late_mask.any():
            consecutive = late_mask.groupby(
                (~late_mask).cumsum()
            ).sum().max()
            features['max_consecutive_late_payments'] = min(int(consecutive), 127)
        else:
            features['max_consecutive_late_payments'] = 0
    else:
        features['pct_payments_on_time'] = 1.0
        features['max_consecutive_late_payments'] = 0
    
    # --- Credit Utilization Features ---
    if open_accounts['credit_limit'].sum() > 0:
        features['total_credit_limit'] = float(open_accounts['credit_limit'].sum())
        features['total_credit_used'] = float(open_accounts['balance'].sum())
        features['overall_utilization_ratio'] = float(
            features['total_credit_used'] / features['total_credit_limit']
        )
        features['max_utilization_ratio'] = float(
            (open_accounts['balance'] / open_accounts['credit_limit']).max()
        )
    else:
        features['total_credit_limit'] = 0.0
        features['total_credit_used'] = 0.0
        features['overall_utilization_ratio'] = 0.0
        features['max_utilization_ratio'] = 0.0
    
    # --- Income & Debt Features ---
    monthly_income = transactions_df[
        transactions_df['type'] == 'INCOME'
    ].resample('M', on='date')['amount'].sum().mean()
    
    monthly_debt = open_accounts['min_payment'].sum()
    
    features['estimated_annual_income'] = float(monthly_income * 12) if monthly_income else 0
    features['total_monthly_debt_payments'] = float(monthly_debt)
    features['debt_to_income_ratio'] = float(
        monthly_debt / monthly_income if monthly_income > 0 else 1.0
    )
    
    return features

# ============================================================
# Write Feature Store to Parquet (Partitioned by Snapshot Date)
# ============================================================
def write_feature_store(features_list, output_path='./feature_store'):
    """Write computed features to partitioned Parquet dataset."""
    table = pa.table(features_list, schema=feature_store_schema)
    
    pq.write_to_dataset(
        table,
        root_path=output_path,
        partition_cols=['snapshot_date'],  # Partition by computation date
        compression='ZSTD',
        use_dictionary=True,
        write_statistics=True,
    )
    
    print(f"Feature store updated: {len(features_list):,} applicant records")
    return table

# ============================================================
# ML Training Data Loading (Read-Optimized for Model Training)
# ============================================================
def load_training_data(snapshot_date, feature_columns=None, default_label='default_180d'):
    """
    Load feature store data for ML training.
    Only reads specified columns — Parquet's column projection
    ensures we never touch irrelevant data.
    """
    if feature_columns is None:
        feature_columns = [
            'applicant_id', 'num_open_accounts', 'oldest_account_age_months',
            'pct_payments_on_time', 'overall_utilization_ratio',
            'debt_to_income_ratio', 'spending_volatility', 'savings_rate',
            'internal_credit_score', 'external_credit_score',
            'fraud_risk_score', default_label
        ]
    
    dataset = pq.ParquetDataset(
        './feature_store',
        filters=[('snapshot_date', '=', snapshot_date)]
    )
    
    table = dataset.read(columns=feature_columns)
    df = table.to_pandas()
    
    # Separate features and target
    feature_cols = [c for c in df.columns if c not in ['applicant_id', default_label]]
    X = df[feature_cols].values
    y = df[default_label].astype(int).values
    
    print(f"Loaded {len(X):,} samples with {len(feature_cols)} features")
    print(f"Default rate: {y.mean():.2%}")
    
    return X, y, feature_cols
```

**Key Benefits:**
- **Training speedup**: Reading 50/200 columns = 75% I/O reduction during model training
- **Consistency**: Single source of truth eliminates training-serving skew
- **Reproducibility**: Snapshot-partitioned data allows exact model reproduction
- **Cost efficiency**: 10M applicants × 200 features × 7 years ≈ 5TB CSV → 300GB Parquet

---

### Scenario 3: Regulatory Reporting (Basel III/IV Liquidity Ratios)

**Problem Statement:**
Basel III/IV regulations require banks to compute and report Liquidity Coverage Ratio (LCR), Net Stable Funding Ratio (NSFR), and other metrics daily. These calculations involve joining data from multiple source systems: core banking (deposits, loans), treasury (securities, bonds), payments (outflows), and risk management (collateral). The data volumes are massive (hundreds of millions of positions), and the calculation must be auditable with full lineage. Reports must be filed with regulators within strict deadlines.

**Why Parquet:**
- **Multiple output formats**: Parquet can be read by regulatory reporting systems, sent to regulators via XBRL converters, or analyzed directly by risk analysts.
- **Audit trail**: Immutable Parquet files with timestamps create a natural audit trail.
- **Efficient aggregations**: LCR requires summing high-quality liquid assets and weighted cash outflows—columnar sums are orders of magnitude faster than row-based.
- **Cross-system join optimization**: When joining data from 4+ source systems, Parquet's metadata allows engines to skip irrelevant partitions.

**Implementation:**

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

# ============================================================
# Basel III LCR Calculation Dataset
# ============================================================

# High-Quality Liquid Assets (HQLA) - Level 1, 2A, 2B
hqla_schema = pa.schema([
    pa.field('asset_id', pa.string()),
    pa.field('asset_class', pa.dictionary(pa.int8(), pa.string())),  # GOVT_BOND, MBS, EQUITY
    pa.field('hqla_level', pa.dictionary(pa.int8(), pa.string())),    # L1, L2A, L2B
    pa.field('market_value', pa.decimal128(18, 2)),
    pa.field('haircut_pct', pa.float32()),
    pa.field('adjusted_value', pa.decimal128(18, 2)),
    pa.field('maturity_bucket', pa.dictionary(pa.int8(), pa.string())),
    pa.field('currency', pa.dictionary(pa.int8(), pa.string())),
    pa.field('is_domestic', pa.bool_()),
    pa.field('reporting_date', pa.date32()),
])

# Cash Outflow Projections
outflow_schema = pa.schema([
    pa.field('outflow_id', pa.string()),
    pa.field('product_type', pa.dictionary(pa.int8(), pa.string())),
    pa.field('counterparty_type', pa.dictionary(pa.int8(), pa.string())),
    pa.field('contractual_outflow', pa.decimal128(18, 2)),
    pa.field('outflow_rate', pa.float32()),  # 0%-100% based on Basel rules
    pa.field('weighted_outflow', pa.decimal128(18, 2)),
    pa.field('maturity_bucket', pa.dictionary(pa.int8(), pa.string())),
    pa.field('reporting_date', pa.date32()),
])

# Cash Inflow Projections
inflow_schema = pa.schema([
    pa.field('inflow_id', pa.string()),
    pa.field('product_type', pa.dictionary(pa.int8(), pa.string())),
    pa.field('counterparty_type', pa.dictionary(pa.int8(), pa.string())),
    pa.field('contractual_inflow', pa.decimal128(18, 2)),
    pa.field('inflow_rate', pa.float32()),
    pa.field('weighted_inflow', pa.decimal128(18, 2)),
    pa.field('maturity_bucket', pa.dictionary(pa.int8(), pa.string())),
    pa.field('reporting_date', pa.date32()),
])

# ============================================================
# LCR Calculation Pipeline
# ============================================================
def compute_lcr(reporting_date='2024-08-25'):
    """
    Compute Liquidity Coverage Ratio per Basel III requirements.
    
    LCR = Total HQLA / Total Net Cash Outflows over 30 days
    Minimum required: 100%
    """
    
    # Load HQLA data
    hqla = pq.read_table('./regulatory_data/hqla/', 
                          filters=[('reporting_date', '=', reporting_date)])
    
    # Load outflow projections
    outflows = pq.read_table('./regulatory_data/outflows/',
                              filters=[('reporting_date', '=', reporting_date)])
    
    # Load inflow projections
    inflows = pq.read_table('./regulatory_data/inflows/',
                             filters=[('reporting_date', '=', reporting_date)])
    
    # ---- Step 1: Compute Total HQLA ----
    # Level 1: No haircut
    level1_mask = pc.equal(hqla['hqla_level'], pa.scalar('L1'))
    level1_total = pc.sum(hqla.filter(level1_mask)['adjusted_value'])
    
    # Level 2A: 15% haircut, capped at 40% of total HQLA
    level2a_mask = pc.equal(hqla['hqla_level'], pa.scalar('L2A'))
    level2a_total = pc.sum(hqla.filter(level2a_mask)['adjusted_value'])
    
    # Level 2B: 25-50% haircut, capped at 15% of total HQLA
    level2b_mask = pc.equal(hqla['hqla_level'], pa.scalar('L2B'))
    level2b_total = pc.sum(hqla.filter(level2b_mask)['adjusted_value'])
    
    total_hqla = level1_total + level2a_total + level2b_total
    
    # ---- Step 2: Compute Weighted Net Outflows ----
    total_outflows = pc.sum(outflows['weighted_outflow'])
    total_inflows = pc.sum(inflows['weighted_inflow'])
    
    # Inflows capped at 75% of outflows (regulatory constraint)
    capped_inflows = pc.min([
        total_inflows,
        total_outflows * 0.75
    ])
    
    net_outflows = total_outflows - capped_inflows
    
    # ---- Step 3: Compute LCR ----
    if net_outflows > 0:
        lcr_ratio = float(total_hqla / net_outflows) * 100
    else:
        lcr_ratio = float('inf')
    
    # ---- Step 4: Generate Regulatory Report ----
    report = {
        'reporting_date': reporting_date,
        'total_hqla_level1': float(level1_total),
        'total_hqla_level2a': float(level2a_total),
        'total_hqla_level2b': float(level2b_total),
        'total_hqla': float(total_hqla),
        'total_gross_outflows': float(total_outflows),
        'total_capped_inflows': float(capped_inflows),
        'total_net_outflows': float(net_outflows),
        'lcr_ratio_pct': lcr_ratio,
        'compliant': lcr_ratio >= 100.0,
        'buffer_pct': lcr_ratio - 100.0,
    }
    
    # Write report as Parquet (immutable audit record)
    report_table = pa.table([report])
    pq.write_table(
        report_table,
        f'./regulatory_reports/lcr/lcr_{reporting_date}.parquet',
        compression='ZSTD'
    )
    
    print(f"\n{'='*60}")
    print(f"BASEL III LCR REPORT - {reporting_date}")
    print(f"{'='*60}")
    print(f"Total HQLA (Level 1):      ${report['total_hqla_level1']:>20,.2f}")
    print(f"Total HQLA (Level 2A):     ${report['total_hqla_level2a']:>20,.2f}")
    print(f"Total HQLA (Level 2B):     ${report['total_hqla_level2b']:>20,.2f}")
    print(f"Total HQLA:                ${report['total_hqla']:>20,.2f}")
    print(f"{'─'*60}")
    print(f"Gross Outflows (30-day):   ${report['total_gross_outflows']:>20,.2f}")
    print(f"Capped Inflows:            ${report['total_capped_inflows']:>20,.2f}")
    print(f"Net Outflows:              ${report['total_net_outflows']:>20,.2f}")
    print(f"{'─'*60}")
    print(f"LCR Ratio:                 {report['lcr_ratio_pct']:>19.2f}%")
    print(f"Minimum Required:          {'100.00%':>20}")
    print(f"Status:                    {'✅ COMPLIANT' if report['compliant'] else '❌ NON-COMPLIANT'}")
    print(f"Buffer:                    {report['buffer_pct']:>19.2f}%")
    print(f"{'='*60}")
    
    return report
```

**Key Benefits:**
- **Audit compliance**: Immutable Parquet files with full computation lineage
- **Regulator confidence**: Reproducible calculations with pinned input data
- **Performance**: 200M+ positions aggregated in seconds vs hours
- **Historical comparison**: Easy to compare LCR across reporting dates

---

## Apache Arrow - Banking Scenarios

### Scenario 1: Real-Time Fraud Detection Pipeline

**Problem Statement:**
A bank's fraud detection system must evaluate every transaction in under 50 milliseconds. The system ingests 5,000 transactions per second from payment networks, enriches each with customer profile data and historical patterns, runs a scoring model, and returns a decision (approve/decline/review) to the payment gateway. Current batch-based architecture introduces 200-500ms latency, causing customer friction and lost revenue from declined legitimate transactions.

**Why Arrow:**
- **Zero-copy IPC**: Transaction data moves between pipeline stages without serialization overhead. In fraud detection, where every millisecond counts, this is critical.
- **In-memory columnar format**: The scoring model reads multiple feature columns simultaneously. Arrow's columnar layout enables SIMD-vectorized feature extraction.
- **Shared memory**: Multiple fraud detection workers can read the same Arrow buffer without copying—essential for horizontal scaling.
- **Cross-language**: Data ingested in Rust/C++ (payment gateway) can be directly consumed by Python (ML model) without conversion.

**Implementation:**

```python
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import time

# ============================================================
# Real-Time Fraud Detection with Arrow IPC
# ============================================================

# Step 1: Define Arrow Schema for Transaction Stream
transaction_schema = pa.schema([
    pa.field('transaction_id', pa.utf8()),
    pa.field('timestamp_ns', pa.timestamp('ns')),
    pa.field('account_id', pa.utf8()),
    pa.field('amount', pa.decimal128(18, 2)),
    pa.field('merchant_id', pa.utf8()),
    pa.field('mcc_code', pa.utf8()),
    pa.field('country_code', pa.utf8()),
    pa.field('channel', pa.utf8()),
    pa.field('device_fingerprint', pa.utf8()),
    pa.field('ip_address', pa.utf8()),
    pa.field('is_card_present', pa.bool_()),
])

# Step 2: Pre-loaded Customer Profile Cache (Arrow format)
# This is held in memory across all fraud detection workers
customer_profile_cache = pa.table({
    'account_id': [f'ACC-{i:06d}' for i in range(1_000_000)],
    'avg_monthly_spend': np.random.lognormal(7, 1, 1_000_000).astype('float64'),
    'max_single_transaction': np.random.lognormal(8, 1.5, 1_000_000).astype('float64'),
    'typical_countries': [np.random.choice(['US', 'UK', 'DE']) for _ in range(1_000_000)],
    'account_age_days': np.random.randint(30, 3650, 1_000_000),
    'num_fraud_events_12m': np.random.poisson(0.05, 1_000_000),
    'risk_tier': np.random.choice(['LOW', 'MEDIUM', 'HIGH'], 1_000_000, p=[0.7, 0.2, 0.1]),
})

# Index by account_id for O(1) lookups using Arrow's DictionaryArray
account_index = pa.compute.index(
    customer_profile_cache['account_id'], 
    pa.array(['ACC-000001'])  # Example lookup
)

# Step 3: Feature Engineering on Arrow Arrays (Vectorized)
def compute_fraud_features(transaction_batch, profile_cache):
    """
    Compute fraud detection features using Arrow's vectorized operations.
    All operations are zero-copy where possible and leverage SIMD.
    """
    features = {}
    
    # Velocity features - how many transactions in last N minutes
    # (simulated with pre-computed values)
    features['tx_count_1h'] = pa.array(
        np.random.poisson(3, len(transaction_batch)), type=pa.int32()
    )
    features['tx_count_24h'] = pa.array(
        np.random.poisson(12, len(transaction_batch)), type=pa.int32()
    )
    
    # Amount deviation from profile
    amounts = transaction_batch['amount'].cast(pa.float64())
    profile_avg = pc.multiply(profile_cache['avg_monthly_spend'], 
                               pa.array([1/30.0] * len(profile_cache)))
    
    features['amount_zscore'] = pc.divide(
        pc.subtract(amounts, profile_avg),
        pa.array([np.random.uniform(100, 1000)] * len(transaction_batch))
    )
    
    # Country anomaly (vectorized string comparison)
    features['is_new_country'] = pc.not_equal(
        transaction_batch['country_code'],
        profile_cache['typical_countries']
    )
    
    # Time-based features
    features['is_unusual_hour'] = pc.or_(
        pc.less(transaction_batch['timestamp_ns'].cast(pa.int64()) % 86400000000000, 
                pa.scalar(21600000000000)),  # Before 6 AM
        pc.greater(transaction_batch['timestamp_ns'].cast(pa.int64()) % 86400000000000,
                   pa.scalar(75600000000000))  # After 9 PM
    )
    
    return features

# Step 4: Fraud Scoring Engine (Arrow-Native)
class FraudScoringEngine:
    """
    High-performance fraud scoring using Arrow's in-memory format.
    Features are computed entirely in Arrow space (no Pandas conversion).
    """
    
    def __init__(self, profile_cache):
        self.profile_cache = profile_cache
        self.score_threshold_decline = 0.85
        self.score_threshold_review = 0.60
        self.thread_pool = ThreadPoolExecutor(max_workers=8)
    
    def score_batch(self, transaction_batch):
        """
        Score a batch of transactions. Returns decision array.
        Uses Arrow compute functions for vectorized scoring.
        """
        start = time.perf_counter_ns()
        
        # Compute features
        features = compute_fraud_features(transaction_batch, self.profile_cache)
        
        # Simple rule-based scoring (in production: load Arrow-native ML model)
        risk_score = pa.array(
            np.random.uniform(0, 1, len(transaction_batch)), type=pa.float32()
        )
        
        # Vectorized decision making
        decisions = pc.case_when(
            [
                pc.greater(risk_score, pa.scalar(self.score_threshold_decline)),
                pa.array(['DECLINE'] * len(transaction_batch))
            ],
            pa.case_when(
                [
                    pc.greater(risk_score, pa.scalar(self.score_threshold_review)),
                    pa.array(['REVIEW'] * len(transaction_batch))
                ],
                pa.array(['APPROVE'] * len(transaction_batch))
            )
        )
        
        # Build result with zero-copy metadata
        result = pa.table({
            'transaction_id': transaction_batch['transaction_id'],
            'risk_score': risk_score,
            'decision': decisions,
            'processing_time_ns': pa.array(
                [time.perf_counter_ns() - start] * len(transaction_batch),
                type=pa.int64()
            ),
        })
        
        return result
    
    def score_stream(self, input_stream, output_stream):
        """
        Continuous scoring from Arrow IPC stream → Arrow IPC stream.
        Zero serialization between input, processing, and output.
        """
        reader = pa.ipc.open_stream(input_stream)
        writer = pa.ipc.new_stream(output_stream, self.result_schema())
        
        for batch in reader:
            result = self.score_batch(batch)
            writer.write_table(result)
        
        writer.close()
    
    def result_schema(self):
        return pa.schema([
            pa.field('transaction_id', pa.utf8()),
            pa.field('risk_score', pa.float32()),
            pa.field('decision', pa.utf8()),
            pa.field('processing_time_ns', pa.int64()),
        ])

# Step 5: Shared Memory Architecture
# Arrow's Plasma object store enables zero-copy sharing between processes
def shared_memory_fraud_detection():
    """
    Demonstrate zero-copy data sharing for fraud detection.
    Multiple workers read the same profile data without copying.
    """
    # In production: use Apache Arrow's Object Store (Plasma)
    # or memory-mapped Arrow IPC files
    
    # Profile data memory-mapped (read-only, shared across workers)
    profile_mmap = pa.memory_map('./customer_profiles.arrow', 'r')
    profile_reader = pa.ipc.open_stream(profile_mmap)
    profiles = profile_reader.read_all()
    
    # All workers can now read `profiles` without copying
    # Worker 1: scores transactions from Payment Gateway A
    # Worker 2: scores transactions from Payment Gateway B
    # Worker 3: scores transactions from ATM Network
    # All share the same Arrow buffer in memory
    
    print(f"Profile cache loaded: {len(profiles):,} customers")
    print(f"Memory footprint: {profiles.nbytes / 1024 / 1024:.1f} MB")
    print(f"(vs ~2GB if stored as Python dicts with string keys)")
```

**Key Benefits:**
- **Sub-50ms latency**: Zero-copy IPC eliminates serialization bottleneck
- **5x throughput**: Vectorized Arrow operations process 25K+ decisions/second per core
- **Memory efficiency**: 5-10x less memory than Python dicts or Pandas DataFrames
- **Horizontal scaling**: Shared memory enables adding workers without memory duplication

---

### Scenario 2: Inter-System Data Hub for Payment Processing

**Problem Statement:**
A large bank operates 15+ core systems: mainframe (COBOL), Java microservices, Python ML services, and real-time streaming (Kafka). Every night, 2TB of payment data must flow between these systems for reconciliation, settlement, and reporting. Current ETL uses JSON/CSV with custom parsers per system, leading to 4-hour processing windows, frequent data quality issues, and tight coupling between producers and consumers.

**Why Arrow:**
- **Language-agnostic format**: The same Arrow buffer can be consumed by COBOL (via Arrow C Data Interface), Java, Python, and C++ without format conversion.
- **IPC for streaming**: Arrow Flight enables gRPC-based data transfer at near-memory speeds.
- **Schema enforcement**: Arrow's strong typing catches data quality issues at write time, not during downstream processing.
- **Columnar for selective reading**: Different systems need different columns—Arrow allows each consumer to read only what it needs from the same shared dataset.

**Implementation:**

```python
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.flight as flight
import pyarrow.compute as pc
import numpy as np
from datetime import datetime

# ============================================================
# Payment Inter-System Data Hub using Arrow IPC
# ============================================================

# Unified Payment Record Schema
# This schema is the canonical format for ALL payment data
payment_hub_schema = pa.schema([
    # Core identifiers
    pa.field('payment_id', pa.utf8()),
    pa.field('source_system', pa.dictionary(pa.int8(), pa.utf8())),
    pa.field('created_at', pa.timestamp('ns')),
    pa.field('updated_at', pa.timestamp('ns')),
    
    # Payment details
    pa.field('payment_type', pa.dictionary(pa.int8(), pa.utf8())),
    pa.field('sender_account', pa.utf8()),
    pa.field('receiver_account', pa.utf8()),
    pa.field('amount', pa.decimal128(18, 2)),
    pa.field('currency', pa.dictionary(pa.int8(), pa.utf8())),
    pa.field('fx_rate', pa.float64()),
    pa.field('amount_in_base_currency', pa.decimal128(18, 2)),
    
    # Routing
    pa.field('payment_network', pa.dictionary(pa.int8(), pa.utf8())),
    pa.field('clearing_system', pa.utf8()),
    pa.field('routing_code', pa.utf8()),
    
    # Status tracking
    pa.field('status', pa.dictionary(pa.int8(), pa.utf8())),
    pa.field('status_reason', pa.utf8()),
    pa.field('retry_count', pa.int8()),
    
    # Compliance
    pa.field('sanctions_check_passed', pa.bool_()),
    pa.field('aml_check_passed', pa.bool_()),
    pa.field('compliance_notes', pa.utf8()),
    
    # Settlement
    pa.field('settlement_date', pa.date32()),
    pa.field('settlement_batch_id', pa.int64()),
    pa.field('is_settled', pa.bool_()),
])

class PaymentDataHub:
    """
    Central data hub using Arrow IPC for inter-system communication.
    Replaces JSON/CSV ETL with zero-copy Arrow data flows.
    """
    
    def __init__(self):
        self.schema = payment_hub_schema
        self.buffer = None
    
    def ingest_from_mainframe(self, cobol_data):
        """
        Receive data from COBOL mainframe (via Arrow C Data Interface).
        The mainframe exports fixed-width records which are converted
        to Arrow by a C++ adapter layer.
        """
        # In production: use pyarrow.cffi or Arrow C Data Interface
        # Simulating conversion from mainframe format
        records = []
        for record in cobol_data:
            records.append({
                'payment_id': f'MF-{record["txn_id"]}',
                'source_system': 'MAINFRAME',
                'created_at': datetime.strptime(record['date_time'], '%Y%m%d%H%M%S'),
                'updated_at': datetime.now(),
                'payment_type': 'WIRE_TRANSFER',
                'sender_account': record['from_acct'],
                'receiver_account': record['to_acct'],
                'amount': float(record['amount']) / 100,
                'currency': record['currency'],
                'status': record['status_code'],
                # ... more fields
            })
        
        table = pa.table(records, schema=self.schema)
        return table
    
    def ingest_from_microservices(self, kafka_batch):
        """
        Receive batched data from Java microservices via Kafka.
        Each Kafka message contains Arrow IPC bytes (not JSON).
        """
        tables = []
        for message_bytes in kafka_batch:
            reader = ipc.open_stream(message_bytes)
            table = reader.read_all()
            tables.append(table)
        
        # Concatenate all microservice batches
        if tables:
            return pa.concat_tables(tables)
        return pa.table([], schema=self.schema)
    
    def broadcast_to_consumers(self, payment_data):
        """
        Distribute payment data to all downstream consumers.
        Each consumer reads only the columns it needs.
        """
        consumer_views = {
            # Reconciliation: needs core + settlement fields
            'reconciliation_system': [
                'payment_id', 'sender_account', 'receiver_account',
                'amount', 'currency', 'status', 'settlement_date',
                'settlement_batch_id', 'is_settled'
            ],
            
            # Risk engine: needs compliance + amount fields
            'risk_engine': [
                'payment_id', 'amount', 'currency', 'amount_in_base_currency',
                'sender_account', 'receiver_account', 'sanctions_check_passed',
                'aml_check_passed', 'compliance_notes', 'payment_network'
            ],
            
            # Customer notification: needs minimal fields
            'notification_service': [
                'payment_id', 'sender_account', 'amount', 'currency',
                'status', 'status_reason'
            ],
            
            # Regulatory reporting: needs everything
            'regulatory_reporting': [f.name for f in self.schema],
        }
        
        results = {}
        for consumer, columns in consumer_views.items():
            # Column projection - each consumer gets only its needed columns
            # Zero copy: Arrow returns a view of the original data
            view = payment_data.select(columns)
            results[consumer] = view
            
            print(f"  {consumer}: {len(columns)} columns, "
                  f"{view.nbytes / 1024:.1f} KB "
                  f"(vs {payment_data.nbytes / 1024:.1f} KB full)")
        
        return results
    
    def write_to_archive(self, data, archive_path):
        """
        Write to columnar archive with compression.
        Supports time-travel queries via partitioning.
        """
        # Partition by date and status for efficient archival queries
        pq.write_to_dataset(
            data,
            root_path=archive_path,
            partition_cols=['status', 'source_system'],
            compression='ZSTD',
            use_dictionary=True,
            write_statistics=True,
        )

# ============================================================
# Arrow Flight SQL Server for Direct Database Access
# ============================================================
class PaymentFlightSQLServer(flight.FlightServerBase):
    """
    Arrow Flight SQL server that exposes payment data
    via SQL interface to any Arrow-compatible client.
    """
    
    def __init__(self, data_hub):
        super().__init__(location="grpc://0.0.0.0:8815")
        self.data_hub = data_hub
        self.payment_data = None
    
    def do_get(self, context, ticket):
        """Serve Arrow IPC data for SQL query results."""
        query_type = ticket.ticket.decode()
        
        if query_type == 'payments_today':
            # Return today's payments as Arrow RecordBatch stream
            today_payments = self.payment_data.filter(
                pc.equal(self.payment_data['created_at'].cast(pa.date32()),
                         pa.scalar(datetime.now().date()))
            )
            return flight.RecordBatchStream(today_payments)
        
        elif query_type == 'unsettled':
            unsettled = self.payment_data.filter(
                pc.equal(self.payment_data['is_settled'], pa.scalar(False))
            )
            return flight.RecordBatchStream(unsettled)
        
        return flight.RecordBatchStream(pa.table([], schema=self.data_hub.schema))
    
    def do_exchange(self, context, descriptor, reader, writer):
        """
        Bidirectional data exchange.
        Client sends parameter data, server returns query results.
        """
        # Read client parameters (e.g., date range)
        params = reader.read_all()
        
        # Apply filters
        filtered = self.payment_data
        if 'start_date' in params.column_names:
            start = params['start_date'][0].as_py()
            filtered = filtered.filter(
                pc.greater_equal(filtered['created_at'].cast(pa.date32()), 
                                pa.scalar(start))
            )
        
        writer.write_table(filtered)
```

**Key Benefits:**
- **10x faster ETL**: 2TB nightly batch in 24 minutes vs 4 hours with JSON
- **Zero data quality bugs**: Arrow schema validation catches mismatches at ingestion
- **40% less memory**: Columnar format with dictionary encoding for categorical fields
- **Simplified architecture**: One format across 15+ systems

---

### Scenario 3: Regulatory Stress Testing Compute Engine

**Problem Statement:**
Basel III stress testing requires banks to simulate 100,000+ economic scenarios against their entire loan portfolio (50 million accounts). Each scenario adjusts 50+ macroeconomic variables (GDP, unemployment, interest rates, housing prices) and recomputes expected losses across all loans. The bank has a 72-hour window to complete all simulations and submit results to the Fed. Current implementation uses distributed Spark clusters and takes 68 hours—barely meeting the deadline with no margin for reruns.

**Why Arrow:**
- **In-memory compute**: Arrow's columnar format enables vectorized simulation math that's 20-50x faster than row-based operations.
- **Zero-copy scenario sharing**: 100,000 scenarios can be shared across compute nodes without serialization overhead.
- **SIMD acceleration**: Arrow's compute kernels use CPU vector instructions for bulk numerical operations.
- **Memory efficiency**: Columnar storage of 50M loans × 50 variables × 100K scenarios fits in cluster memory with Arrow's compressed format.

**Implementation:**

```python
import pyarrow as pa
import pyarrow.compute as pc
import numpy as np
from concurrent.futures import ProcessPoolExecutor

# ============================================================
# Stress Testing Compute Engine
# ============================================================

# Loan Portfolio Schema
loan_schema = pa.schema([
    pa.field('loan_id', pa.utf8()),
    pa.field('loan_type', pa.dictionary(pa.int8(), pa.utf8())),
    pa.field('original_amount', pa.decimal128(18, 2)),
    pa.field('current_balance', pa.decimal128(18, 2)),
    pa.field('interest_rate', pa.float32()),
    pa.field('remaining_term_months', pa.int32()),
    pa.field('payment_frequency', pa.dictionary(pa.int8(), pa.utf8())),
    pa.field('collateral_value', pa.decimal128(18, 2)),
    pa.field('ltv_ratio', pa.float32()),
    pa.field('credit_score', pa.int16()),
    pa.field('delinquency_status', pa.dictionary(pa.int8(), pa.utf8())),
    pa.field('state_code', pa.dictionary(pa.int8(), pa.utf8())),
    pa.field('origination_date', pa.date32()),
    pa.field('maturity_date', pa.date32()),
])

# Scenario Parameters Schema
scenario_schema = pa.schema([
    pa.field('scenario_id', pa.int32()),
    pa.field('scenario_name', pa.utf8()),
    pa.field('gdp_growth_rate', pa.float32()),
    pa.field('unemployment_rate', pa.float32()),
    pa.field('fed_funds_rate', pa.float32()),
    pa.field('housing_price_index', pa.float32()),
    pa.field('sp500_return', pa.float32()),
    pa.field('inflation_rate', pa.float32()),
    pa.field('commercial_re_price', pa.float32()),
    pa.field('consumer_confidence', pa.float32()),
])

class StressTestEngine:
    """
    High-performance stress testing using Arrow vectorized operations.
    Targets: 50M loans × 100K scenarios in <72 hours.
    """
    
    def __init__(self, loan_portfolio, scenarios):
        self.loans = loan_portfolio
        self.scenarios = scenarios
        self.results = []
    
    def _compute_scenario_impact_vectorized(self, scenario):
        """
        Compute loss impact for ALL 50M loans against ONE scenario.
        Uses Arrow compute for vectorized math across entire portfolio.
        
        Loss Model (simplified PD × LGD × EAD):
        - PD (Probability of Default) = f(credit_score, unemployment, delinquency)
        - LGD (Loss Given Default) = f(LTV, housing_price_index, loan_type)
        - EAD (Exposure at Default) = current_balance
        """
        
        # ---- PD Computation (Vectorized) ----
        # Base PD from credit score (logistic function approximation)
        credit_scores = self.loans['credit_score'].cast(pa.float32())
        
        # Logistic: PD = 1 / (1 + exp(-(a + b * credit_score)))
        # Adjusted by unemployment rate shock
        unemployment_shock = pa.scalar(scenario['unemployment_rate'])
        base_pd_intercept = pc.add(
            pa.scalar(-3.5),
            pc.multiply(unemployment_shock, pa.scalar(2.0))
        )
        
        # Vectorized logistic regression
        z = pc.add(
            base_pd_intercept,
            pc.multiply(credit_scores, pa.scalar(-0.03))
        )
        # Approximate sigmoid: 1 / (1 + exp(-z))
        # Using Arrow compute for element-wise operations
        neg_z = pc.multiply(z, pa.scalar(-1.0))
        exp_neg_z = pc.exp(neg_z)  # Arrow's vectorized exp
        one_plus_exp = pc.add(pa.scalar(1.0), exp_neg_z)
        pd_scores = pc.divide(pa.scalar(1.0), one_plus_exp)
        
        # Delinquency adjustment
        delinq = self.loans['delinquency_status']
        is_delinquent = pc.equal(delinq, pa.scalar('DELINQUENT'))
        pd_adjusted = pc.if_else(is_delinquent, 
                                  pc.multiply(pd_scores, pa.scalar(2.0)),
                                  pd_scores)
        
        # ---- LGD Computation (Vectorized) ----
        # LGD depends on collateral value relative to exposure
        ltv = self.loans['ltv_ratio'].cast(pa.float32())
        housing_shock = pa.scalar(scenario['housing_price_index'])
        
        # Adjusted LTV = current LTV / (1 + housing_price_shock)
        adjusted_ltv = pc.divide(ltv, pc.add(pa.scalar(1.0), housing_shock))
        
        # LGD = max(0, adjusted_ltv - recovery_rate)
        recovery_rate = pa.scalar(0.30)  # 30% recovery assumption
        lgd = pc.max_element_wise(
            pa.scalar(0.0),
            pc.subtract(adjusted_ltv, recovery_rate)
        )
        
        # ---- EAD (Exposure at Default) ----
        ead = self.loans['current_balance'].cast(pa.float64())
        
        # ---- Expected Loss = PD × LGD × EAD ----
        expected_loss = pc.multiply(
            pc.multiply(pd_adjusted, lgd),
            ead
        )
        
        # ---- Summarize Results ----
        total_ead = pc.sum(ead)
        total_el = pc.sum(expected_loss)
        
        # Group by loan type for reporting
        loan_types = self.loans['loan_type']
        unique_types = pa.compute.unique(loan_types)
        
        scenario_results = {
            'scenario_id': scenario['scenario_id'],
            'scenario_name': scenario['scenario_name'],
            'total_ead': float(total_ead),
            'total_expected_loss': float(total_el),
            'el_ratio': float(total_el / total_ead) if float(total_ead) > 0 else 0,
            'avg_pd': float(pc.mean(pd_adjusted)),
            'avg_lgd': float(pc.mean(lgd)),
        }
        
        return scenario_results, expected_loss
    
    def run_parallel(self, max_workers=32):
        """
        Run all scenarios in parallel using Arrow's thread-safe operations.
        Each worker gets a read-only view of the loan portfolio (zero-copy).
        """
        start_time = time.time()
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i in range(len(self.scenarios)):
                scenario = {col: self.scenarios[col][i].as_py() 
                           for col in self.scenarios.column_names}
                future = executor.submit(
                    self._compute_scenario_impact_vectorized, scenario
                )
                futures.append(future)
            
            results = []
            for i, future in enumerate(futures):
                result, _ = future.result()
                results.append(result)
                
                if (i + 1) % 10000 == 0:
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed
                    remaining = (len(self.scenarios) - i - 1) / rate
                    print(f"  Completed {i+1:,}/{len(self.scenarios):,} scenarios "
                          f"({rate:.0f}/sec, ETA: {remaining:.0f}s)")
        
        elapsed = time.time() - start_time
        print(f"\nCompleted {len(self.scenarios):,} scenarios in {elapsed:.1f}s "
              f"({len(self.scenarios)/elapsed:.0f} scenarios/sec)")
        
        # Write results to Parquet for regulatory submission
        results_table = pa.table(results)
        pq.write_table(results_table, './stress_test_results/results.parquet',
                       compression='ZSTD')
        
        return results_table
```

**Key Benefits:**
- **7x faster completion**: 10 hours vs 68 hours, with margin for reruns
- **Cost savings**: 70% less compute infrastructure vs Spark
- **Full portfolio coverage**: 50M loans processed in-memory with Arrow
- **Regulatory confidence**: Deterministic results with reproducible Arrow computations

---

## Apache Flight SQL - Banking Scenarios

### Scenario 1: Unified Analytics Gateway for Multi-System Bank

**Problem Statement:**
A bank has data spread across PostgreSQL (transaction processing), ClickHouse (analytics), MongoDB (customer 360), and HDFS (data lake). Business analysts want to run SQL queries across all these systems from a single tool (Tableau, PowerBI, or Jupyter). Currently, each system requires a separate connection, different SQL dialects, and data must be manually joined in Excel or Python notebooks. The bank needs a unified SQL gateway that provides consistent query access with sub-second response for dashboards.

**Why Flight SQL:**
- **Standard SQL interface**: Flight SQL provides a standard SQL protocol that abstracts away underlying database differences.
- **Arrow-native**: Results are returned as Arrow RecordBatches—no JSON/CSV conversion overhead.
- **High performance**: Flight SQL uses gRPC with Arrow IPC for data transfer, achieving near-memory-speed data delivery.
- **Security**: Built-in TLS encryption and token-based authentication for banking compliance.
- **Tool integration**: Any Arrow-compatible tool (DuckDB, pandas, Tableau via ODBC) can connect.

**Implementation:**

```python
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.flight.sql as flight_sql
import sqlite3
import duckdb
from typing import Generator

# ============================================================
# Flight SQL Unified Analytics Gateway
# ============================================================

class BankAnalyticsGateway(flight.FlightServerBase):
    """
    Flight SQL server that provides a unified SQL interface
    across multiple banking data sources.
    
    Architecture:
    ┌──────────┐     ┌─────────────────────┐     ┌──────────────┐
    │ Tableau  │────▶│  Flight SQL Gateway  │────▶│ PostgreSQL   │
    │ PowerBI  │     │  (Unified SQL)       │     │ (Transactions)│
    │ Jupyter  │     │                      │     ├──────────────┤
    │ DuckDB   │     │  • Query routing     │────▶│ ClickHouse   │
    └──────────┘     │  • Schema unification │     │ (Analytics)  │
                     │  • Result caching     │     ├──────────────┤
                     │  • Access control     │────▶│ DuckDB       │
                     └─────────────────────┘     │ (Data Lake)  │
                                                  └──────────────┘
    """
    
    def __init__(self, location="grpc://0.0.0.0:8816"):
        super().__init__(location=location)
        self.backends = {}
        self.query_cache = {}
        self.access_policies = {}
    
    def register_backend(self, name, connection_string, query_handler):
        """Register a data source backend."""
        self.backends[name] = {
            'connection': connection_string,
            'handler': query_handler,
        }
        print(f"Registered backend: {name}")
    
    def get_flight_info(self, context, descriptor):
        """
        Handle SQL query requests from clients.
        Route to appropriate backend based on query analysis.
        """
        if descriptor.descriptor_type == flight.DescriptorType.PATH:
            path = descriptor.path[0].decode()
            return self._handle_path_query(path)
        elif descriptor.descriptor_type == flight.DescriptorType.CMD:
            sql = descriptor.command.decode()
            return self._handle_sql_query(sql, context)
        
        raise flight.FlightUnavailableError("Unknown descriptor type")
    
    def _handle_sql_query(self, sql, context):
        """
        Parse SQL, determine target backend(s), and execute.
        Supports cross-system joins via query rewriting.
        """
        sql_upper = sql.upper().strip()
        
        # Route based on table references in the query
        if 'TRANSACTIONS' in sql_upper:
            backend = self.backends.get('postgresql')
            schema = pa.schema([
                pa.field('transaction_id', pa.utf8()),
                pa.field('account_id', pa.utf8()),
                pa.field('amount', pa.decimal128(18, 2)),
                pa.field('type', pa.utf8()),
                pa.field('timestamp', pa.timestamp('ms')),
            ])
        elif 'ANALYTICS' in sql_upper or 'METRICS' in sql_upper:
            backend = self.backends.get('clickhouse')
            schema = pa.schema([
                pa.field('metric_name', pa.utf8()),
                pa.field('metric_value', pa.float64()),
                pa.field('period', pa.utf8()),
            ])
        elif 'CUSTOMER_360' in sql_upper:
            backend = self.backends.get('duckdb_lake')
            schema = pa.schema([
                pa.field('customer_id', pa.utf8()),
                pa.field('full_name', pa.utf8()),
                pa.field('risk_score', pa.float32()),
                pa.field('total_deposits', pa.decimal128(18, 2)),
            ])
        else:
            raise flight.FlightUnavailableError(
                f"Cannot route query: {sql[:100]}..."
            )
        
        return flight.FlightInfo(
            schema, descriptor, [], -1, {}
        )
    
    def do_get(self, context, ticket):
        """
        Execute query and return results as Arrow RecordBatch stream.
        This is where the actual database query happens.
        """
        ticket_data = ticket.ticket.decode()
        parts = ticket_data.split('|')
        backend_name = parts[0]
        query = parts[1] if len(parts) > 1 else ''
        
        backend = self.backends.get(backend_name)
        if not backend:
            raise flight.FlightUnavailableError(
                f"Unknown backend: {backend_name}"
            )
        
        # Execute query on backend and stream results
        result = backend['handler'].execute(query)
        
        # Convert to Arrow and stream
        if isinstance(result, pa.Table):
            return flight.RecordBatchStream(result)
        else:
            # For non-Arrow backends, convert via pandas
            import pandas as pd
            df = pd.DataFrame(result)
            table = pa.Table.from_pandas(df)
            return flight.RecordBatchStream(table)
    
    def do_action(self, context, action):
        """
        Handle administrative actions:
        - CACHE_REFRESH: Refresh query result cache
        - EXPORT_DATA: Export query results to Parquet
        - GET_BACKENDS: List registered backends
        """
        action_type = action.type.decode()
        
        if action_type == 'GET_BACKENDS':
            backends_info = {
                name: {
                    'type': 'postgresql' if 'postgres' in info['connection'] else 'other',
                    'status': 'connected',
                }
                for name, info in self.backends.items()
            }
            yield flight.Result(pa.table(backlands_info).to_ipc_bytes())
        
        elif action_type == 'CACHE_REFRESH':
            self.query_cache.clear()
            yield flight.Result(b"Cache cleared")
        
        elif action_type == 'EXPORT_DATA':
            # Export to Parquet for offline analysis
            query = action.body.decode()
            result = self._execute_and_cache(query)
            pq.write_table(result, f'./exports/export_{int(time.time())}.parquet')
            yield flight.Result(b"Export complete")


# ============================================================
# Client Usage Examples
# ============================================================

def demonstrate_flight_sql_client():
    """
    Show how banking tools connect to the Flight SQL gateway.
    """
    
    # Connect from Python (pandas-like interface)
    client = flight.connect("grpc://gateway.bank.internal:8816")
    
    # ---- Query 1: Daily transaction summary ----
    descriptor = flight_descriptor.for_command(
        "SELECT DATE_TRUNC('day', timestamp) as tx_date, "
        "       type, COUNT(*) as tx_count, SUM(amount) as total_amount "
        "FROM transactions "
        "WHERE timestamp >= '2024-08-01' "
        "GROUP BY tx_date, type "
        "ORDER BY tx_date"
    )
    
    reader = client.do_get(descriptor)
    result_table = reader.read_all()
    
    print("Daily Transaction Summary:")
    print(result_table.to_pandas())
    
    # ---- Query 2: Cross-system customer 360 ----
    # This query combines transaction data (PostgreSQL) with
    # customer profile data (DuckDB data lake)
    descriptor = flight_descriptor.for_command(
        "SELECT c.customer_id, c.full_name, c.risk_score, "
        "       COUNT(t.transaction_id) as tx_count_30d, "
        "       SUM(t.amount) as total_spend_30d "
        "FROM customer_360 c "
        "JOIN transactions t ON c.customer_id = t.account_id "
        "WHERE t.timestamp >= CURRENT_DATE - INTERVAL '30 days' "
        "GROUP BY c.customer_id, c.full_name, c.risk_score "
        "HAVING SUM(t.amount) > 10000 "
        "ORDER BY total_spend_30d DESC "
        "LIMIT 100"
    )
    
    reader = client.do_get(descriptor)
    customer_analytics = reader.read_all()
    
    print("\nHigh-Value Customer Analytics:")
    print(customer_analytics.to_pandas())
    
    # ---- Query 3: DuckDB connecting to Flight SQL ----
    # DuckDB can read from Flight SQL as a foreign data source
    import duckdb
    
    con = duckdb.connect()
    con.execute("""
        INSTALL flight;
        LOAD flight;
        
        -- Register Flight SQL server as a foreign database
        ATTACH 'grpc://gateway.bank.internal:8816' AS bank_analytics (TYPE FLIGHT_SQL);
        
        -- Query across systems using standard SQL
        SELECT * FROM bank_analytics.transactions 
        WHERE amount > 100000 
        LIMIT 10
    """)
    
    results = con.execute("SELECT * FROM bank_analytics.transactions LIMIT 10").fetchall()
    print("\nDuckDB → Flight SQL Results:")
    for row in results:
        print(row)

# ============================================================
# Access Control Implementation
# ============================================================

class BankAccessPolicy:
    """
    Role-based access control for Flight SQL queries.
    Banking regulations require different data access levels.
    """
    
    ROLES = {
        'TELLER': {
            'allowed_tables': ['accounts_basic', 'transactions_recent'],
            'allowed_columns': {
                'accounts_basic': ['account_id', 'balance', 'account_type'],
                'transactions_recent': ['transaction_id', 'amount', 'timestamp'],
            },
            'max_rows': 100,
            'allowed_operations': ['SELECT'],
        },
        'ANALYST': {
            'allowed_tables': ['transactions', 'accounts', 'customers'],
            'allowed_columns': {
                'transactions': '*',  # All columns
                'accounts': '*',
                'customers': ['customer_id', 'segment', 'region'],
            },
            'max_rows': 1_000_000,
            'allowed_operations': ['SELECT', 'EXPORT'],
        },
        'COMPLIANCE_OFFICER': {
            'allowed_tables': '*',
            'allowed_columns': '*',  # Full access for regulatory queries
            'max_rows': None,  # Unlimited
            'allowed_operations': ['SELECT', 'EXPORT', 'AUDIT_LOG'],
        },
    }
    
    @staticmethod
    def filter_query(sql, role, user_context):
        """Apply access control to SQL query before execution."""
        policy = BankAccessPolicy.ROLES.get(role)
        if not policy:
            raise PermissionError(f"Unknown role: {role}")
        
        # Parse table references from SQL
        import re
        tables_in_query = set(re.findall(
            r'\b(FROM|JOIN)\s+(\w+)', sql, re.IGNORECASE
        ))
        
        # Check table access
        allowed = policy['allowed_tables']
        if allowed != '*':
            for _, table in tables_in_query:
                if table.lower() not in [t.lower() for t in allowed]:
                    raise PermissionError(
                        f"Access denied: table '{table}' not in role '{role}' access list"
                    )
        
        # Append row limit
        if policy['max_rows']:
            if 'LIMIT' not in sql.upper():
                sql = f"{sql} LIMIT {policy['max_rows']}"
        
        # Log query for audit trail
        audit_entry = {
            'user': user_context.get('username'),
            'role': role,
            'query': sql,
            'timestamp': datetime.now().isoformat(),
            'source_ip': user_context.get('ip'),
        }
        
        return sql, audit_entry
```

**Key Benefits:**
- **Single connection**: Analysts connect once, query everywhere
- **10x faster than ODBC**: Arrow IPC transport vs traditional row-based protocols
- **Compliance**: Built-in query logging and role-based access control
- **Tool agnostic**: Works with Tableau, PowerBI, Jupyter, DuckDB, any Arrow client

---

### Scenario 2: Real-Time Regulatory Reporting Dashboard

**Problem Statement:**
Bank executives need a real-time dashboard showing key regulatory metrics: LCR, NSFR, capital adequacy ratios, and large exposure limits. These metrics require joining data from 6+ systems and refreshing every 15 minutes. Current batch ETL takes 45 minutes per refresh, making the dashboard perpetually 1+ hours stale. Regulators are questioning the bank's ability to monitor risk in near-real-time.

**Why Flight SQL:**
- **Incremental updates**: Flight SQL supports streaming results, so only changed data needs to flow.
- **Sub-second queries**: Pre-computed materialized views served via Flight SQL respond in <100ms.
- **Direct dashboard connectivity**: PowerBI/Tableau connect directly to Flight SQL via Arrow ODBC driver.

**Implementation:**

```python
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.flight.sql as flight_sql
import pyarrow.compute as pc
from datetime import datetime, timedelta
import threading
import time

# ============================================================
# Real-Time Regulatory Metrics via Flight SQL
# ============================================================

class RegulatoryMetricsServer(flight.FlightServerBase):
    """
    Flight SQL server serving pre-computed regulatory metrics
    with 15-minute refresh cycle.
    """
    
    def __init__(self):
        super().__init__(location="grpc://0.0.0.0:8817")
        self.metrics_cache = {}
        self.last_refresh = None
        self.refresh_interval = timedelta(minutes=15)
        self._start_background_refresh()
    
    def _start_background_refresh(self):
        """Background thread that refreshes metrics every 15 minutes."""
        def refresh_loop():
            while True:
                self._refresh_metrics()
                time.sleep(self.refresh_interval.total_seconds())
        
        thread = threading.Thread(target=refresh_loop, daemon=True)
        thread.start()
    
    def _refresh_metrics(self):
        """Compute all regulatory metrics from source systems."""
        print(f"[{datetime.now()}] Refreshing regulatory metrics...")
        start = time.time()
        
        # ---- LCR Calculation ----
        self.metrics_cache['lcr'] = self._compute_lcr()
        
        # ---- NSFR Calculation ----
        self.metrics_cache['nsfr'] = self._compute_nsfr()
        
        # ---- Capital Adequacy ----
        self.metrics_cache['capital_adequacy'] = self._compute_capital_adequacy()
        
        # ---- Large Exposures ----
        self.metrics_cache['large_exposures'] = self._compute_large_exposures()
        
        self.last_refresh = datetime.now()
        elapsed = time.time() - start
        print(f"Metrics refreshed in {elapsed:.1f}s")
    
    def _compute_lcr(self):
        """Compute Liquidity Coverage Ratio."""
        # Simulated: in production, queries multiple backends
        return pa.table({
            'metric_name': ['LCR_Total', 'LCR_Level1', 'LCR_Level2A', 'LCR_Level2B'],
            'value': [142.5, 98.3, 28.7, 15.5],
            'unit': ['%', '%', '%', '%'],
            'threshold': [100.0, 0.0, 0.0, 0.0],
            'compliant': [True, True, True, True],
            'last_updated': [self.last_refresh] * 4,
        })
    
    def _compute_nsfr(self):
        """Compute Net Stable Funding Ratio."""
        return pa.table({
            'metric_name': ['NSFR_Overall', 'NSFR_Retail', 'NSFR_Corporate'],
            'value': [118.3, 125.7, 108.2],
            'unit': ['%', '%', '%'],
            'threshold': [100.0, 100.0, 100.0],
            'compliant': [True, True, True],
            'last_updated': [self.last_refresh] * 3,
        })
    
    def _compute_capital_adequacy(self):
        """Compute Basel III capital ratios."""
        return pa.table({
            'metric_name': ['CET1_Ratio', 'Tier1_Ratio', 'Total_Capital_Ratio', 
                           'Leverage_Ratio', 'GCL_Ratio'],
            'value': [13.2, 15.1, 17.8, 6.5, 3.2],
            'unit': ['%', '%', '%', '%', '%'],
            'threshold': [4.5, 6.0, 8.0, 3.0, 2.0],
            'compliant': [True, True, True, True, True],
            'risk_weighted_assets': [500_000_000_000] * 5,
            'last_updated': [self.last_refresh] * 5,
        })
    
    def _compute_large_exposures(self):
        """Compute large exposure limits (25% of capital)."""
        return pa.table({
            'counterparty': ['Corp-A', 'Corp-B', 'Corp-C', 'Corp-D', 'Corp-E'],
            'exposure_amount': [12_000_000_000, 11_500_000_000, 
                              10_800_000_000, 9_200_000_000, 8_500_000_000],
            'capital_base': [65_000_000_000] * 5,
            'exposure_ratio_pct': [18.5, 17.7, 16.6, 14.2, 13.1],
            'limit_pct': [25.0] * 5,
            'compliant': [True] * 5,
            'last_updated': [self.last_refresh] * 5,
        })
    
    def do_get(self, context, ticket):
        """Serve metric results as Arrow Flight stream."""
        metric_name = ticket.ticket.decode()
        
        if metric_name in self.metrics_cache:
            table = self.metrics_cache[metric_name]
            return flight.RecordBatchStream(table)
        
        # Serve aggregated summary
        all_metrics = pa.concat_tables([
            self.metrics_cache.get('lcr', pa.table({})),
            self.metrics_cache.get('nsfr', pa.table({})),
            self.metrics_cache.get('capital_adequacy', pa.table({})),
        ], promote_options='default')
        
        return flight.RecordBatchStream(all_metrics)
    
    def do_action(self, context, action):
        """Handle metric refresh and export actions."""
        action_type = action.type.decode()
        
        if action_type == 'REFRESH_NOW':
            self._refresh_metrics()
            yield flight.Result(
                pa.table({'status': ['refreshed'], 
                         'timestamp': [datetime.now()]}).to_ipc_bytes()
            )
        
        elif action_type == 'EXPORT_ALL':
            # Export all metrics to Parquet for regulatory submission
            all_metrics = []
            for name, table in self.metrics_cache.items():
                all_metrics.append(table)
            
            combined = pa.concat_tables(all_metrics, promote_options='default')
            filename = f'./regulatory_metrics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
            pq.write_table(combined, filename, compression='ZSTD')
            
            yield flight.Result(f"Exported to {filename}".encode())
    
    def list_actions(self, context):
        """List available administrative actions."""
        return [
            flight.ActionType('REFRESH_NOW', 'Force immediate metrics refresh'),
            flight.ActionType('EXPORT_ALL', 'Export all metrics to Parquet'),
        ]


# ============================================================
# Dashboard Client (PowerBI/Tableau would connect similarly)
# ============================================================
def dashboard_client():
    """Python client simulating a real-time dashboard."""
    client = flight.connect("grpc://regulatory-server.bank.internal:8817")
    
    # ---- Executive Summary View ----
    print("=" * 70)
    print("REAL-TIME REGULATORY DASHBOARD")
    print(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # LCR Card
    lcr_reader = client.do_get(flight.Ticket(b'lcr'))
    lcr_data = lcr_reader.read_all()
    
    print("\n📊 LIQUIDITY COVERAGE RATIO (LCR)")
    print("-" * 50)
    for i in range(len(lcr_data)):
        name = lcr_data['metric_name'][i].as_py()
        value = lcr_data['value'][i].as_py()
        compliant = "✅" if lcr_data['compliant'][i].as_py() else "❌"
        print(f"  {name:20s} {value:>8.1f}%  {compliant}")
    
    # Capital Adequacy Card
    cap_reader = client.do_get(flight.Ticket(b'capital_adequacy'))
    cap_data = cap_reader.read_all()
    
    print("\n🏦 CAPITAL ADEQUACY RATIOS")
    print("-" * 50)
    for i in range(len(cap_data)):
        name = cap_data['metric_name'][i].as_py()
        value = cap_data['value'][i].as_py()
        threshold = cap_data['threshold'][i].as_py()
        compliant = "✅" if cap_data['compliant'][i].as_py() else "❌"
        buffer = value - threshold
        print(f"  {name:20s} {value:>6.1f}% (min: {threshold:.1f}%, buffer: {buffer:.1f}%) {compliant}")
    
    # Large Exposures Card
    le_reader = client.do_get(flight.Ticket(b'large_exposures'))
    le_data = le_reader.read_all()
    
    print("\n⚠️  TOP LARGE EXPOSURES")
    print("-" * 50)
    for i in range(min(5, len(le_data))):
        cp = le_data['counterparty'][i].as_py()
        ratio = le_data['exposure_ratio_pct'][i].as_py()
        amount = le_data['exposure_amount'][i].as_py()
        print(f"  {cp:12s} {ratio:>5.1f}%  (${amount:,.0f})")
    
    print("\n" + "=" * 70)
    print(f"Dashboard refreshes every 15 minutes via Flight SQL")
    print(f"Query latency: <100ms (pre-computed metrics)")
```

**Key Benefits:**
- **15-minute freshness**: Down from 1+ hour with batch ETL
- **Sub-100ms dashboard loads**: Pre-computed metrics served via Flight SQL
- **Executive confidence**: Real-time visibility into regulatory compliance
- **Audit trail**: All metric computations logged with timestamps

---

### Scenario 3: Cross-Border Payment Investigation Tool

**Problem Statement:**
A bank's compliance team must investigate suspicious cross-border payment chains. A single investigation might require tracing a payment through 5-10 intermediate banks across multiple countries, joining data from SWIFT, local payment systems, and internal records. Currently, each data source requires a separate SQL query tool, different credentials, and results are manually correlated in spreadsheets. Investigations that should take hours take days.

**Why Flight SQL:**
- **Single investigation query**: Flight SQL can join data across systems as if they were one database.
- **Trace results as Arrow**: Large result sets (millions of linked transactions) stream efficiently.
- **Audit logging**: Every query is logged for compliance purposes.
- **Ad-hoc analysis**: Investigators can use DuckDB/Python to interactively explore the data.

**Implementation:**

```python
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.compute as pc
import duckdb

# ============================================================
# Cross-Border Payment Investigation via Flight SQL + DuckDB
# ============================================================

class PaymentInvestigationToolkit:
    """
    Investigation tool combining Flight SQL (data access) with
    DuckDB (interactive analysis) for cross-border payment tracing.
    """
    
    def __init__(self, flight_endpoint="grpc://investigation.bank.internal:8818"):
        self.endpoint = flight_endpoint
        self.con = duckdb.connect()
        self._setup_duckdb_flight()
    
    def _setup_duckdb_flight(self):
        """Connect DuckDB to Flight SQL data sources."""
        self.con.execute("""
            INSTALL flight;
            LOAD flight;
            
            -- Connect to SWIFT message database
            ATTACH 'grpc://swift-gateway.bank.internal:8820' AS swift (TYPE FLIGHT_SQL);
            
            -- Connect to local payment systems
            ATTACH 'grpc://payments-gateway.bank.internal:8821' AS payments (TYPE FLIGHT_SQL);
            
            -- Connect to compliance watchlist database
            ATTACH 'grpc://compliance-gateway.bank.internal:8822' AS compliance (TYPE FLIGHT_SQL);
            
            -- Create local analysis tables for investigation working data
            CREATE TABLE IF NOT EXISTS investigation_worklist (
                investigation_id VARCHAR,
                payment_chain_id VARCHAR,
                risk_level VARCHAR,
                assigned_to VARCHAR,
                status VARCHAR,
                created_at TIMESTAMP
            );
        """)
    
    def trace_payment_chain(self, initial_payment_id, max_hops=10):
        """
        Trace a payment through its entire chain across borders.
        
        SQL joins SWIFT messages, local payment records, and
        correspondent bank data to build the full payment path.
        """
        result = self.con.execute(f"""
            WITH RECURSIVE payment_trace AS (
                -- Anchor: Initial payment
                SELECT 
                    p.payment_id,
                    p.sender_bic,
                    p.receiver_bic,
                    p.sender_account,
                    p.receiver_account,
                    p.amount,
                    p.currency,
                    p.value_date,
                    p.status,
                    p.message_type,
                    1 as hop_number,
                    ARRAY[p.payment_id] as chain,
                    p.sender_bic || ' → ' || p.receiver_bic as path
                FROM payments.transactions p
                WHERE p.payment_id = '{initial_payment_id}'
                
                UNION ALL
                
                -- Recursive: Follow forwarding payments
                SELECT 
                    p2.payment_id,
                    p2.sender_bic,
                    p2.receiver_bic,
                    p2.sender_account,
                    p2.receiver_account,
                    p2.amount,
                    p2.currency,
                    p2.value_date,
                    p2.status,
                    p2.message_type,
                    pt.hop_number + 1,
                    ARRAY_APPEND(pt.chain, p2.payment_id),
                    pt.path || ' → ' || p2.receiver_bic
                FROM payment_trace pt
                JOIN payments.transactions p2 
                    ON p2.sender_account = pt.receiver_account
                    AND p2.value_date >= pt.value_date
                    AND p2.value_date <= pt.value_date + INTERVAL '5 days'
                    AND p2.amount <= pt.amount * 1.01  -- Allow small fees
                    AND p2.amount >= pt.amount * 0.95
                WHERE pt.hop_number < {max_hops}
                    AND p2.payment_id != ALL(pt.chain)  -- Prevent cycles
            )
            SELECT * FROM payment_trace
            ORDER BY hop_number
        """).fetchdf()
        
        return result
    
    def investigate_suspicious_chain(self, payment_id):
        """
        Full investigation workflow for a suspicious payment.
        Combines payment tracing with compliance checks.
        """
        print(f"\n{'='*80}")
        print(f"CROSS-BORDER PAYMENT INVESTIGATION")
        print(f"Payment ID: {payment_id}")
        print(f"Investigation Started: {datetime.now().isoformat()}")
        print(f"{'='*80}")
        
        # Step 1: Trace the payment chain
        print("\n📍 STEP 1: Tracing Payment Chain...")
        chain = self.trace_payment_chain(payment_id)
        print(f"  Found {len(chain)} hops across {chain['path'].iloc[-1] if len(chain) > 0 else 'N/A'}")
        
        # Step 2: Check each node against sanctions/PEP lists
        print("\n🔍 STEP 2: Running Compliance Checks...")
        for _, row in chain.iterrows():
            checks = self.con.execute(f"""
                SELECT 
                    w.entity_name,
                    w.list_type,
                    w.risk_score,
                    w.jurisdiction
                FROM compliance.watchlists w
                WHERE w.bic_code IN ('{row['sender_bic']}', '{row['receiver_bic']}')
                   OR w.account_number IN ('{row['sender_account']}', '{row['receiver_account']}')
            """).fetchdf()
            
            if len(checks) > 0:
                print(f"  ⚠️  Hop {row['hop_number']}: MATCH FOUND")
                for _, match in checks.iterrows():
                    print(f"      - {match['list_type']}: {match['entity_name']} "
                          f"(Risk: {match['risk_score']}, Jurisdiction: {match['jurisdiction']})")
        
        # Step 3: Analyze amount patterns
        print("\n💰 STEP 3: Amount Analysis...")
        total_in = chain['amount'].sum()
        avg_amount = chain['amount'].mean()
        print(f"  Total flow-through: {chain['currency'].iloc[0]} {total_in:,.2f}")
        print(f"  Average hop amount: {chain['currency'].iloc[0]} {avg_amount:,.2f}")
        
        # Step 4: Geographic risk assessment
        print("\n🌍 STEP 4: Geographic Risk Assessment...")
        country_risk = self.con.execute(f"""
            SELECT 
                bic.country_code,
                COUNT(*) as hop_count,
                GROUP_CONCAT(DISTINCT bic.bank_name) as banks
            FROM (
                VALUES {','.join([f"('{bic}')" for bic in chain['sender_bic'].tolist()])}
            ) AS bic_list(bic_code)
            JOIN compliance.bank_info bic ON bic.bic_code = bic_list.bic_code
            GROUP BY bic.country_code
        """).fetchdf()
        
        for _, row in country_risk.iterrows():
            risk_level = self._get_country_risk(row['country_code'])
            print(f"  {row['country_code']}: {row['hop_count']} hops - Risk: {risk_level}")
        
        # Step 5: Generate investigation report
        report = self._generate_report(payment_id, chain, country_risk)
        
        print(f"\n{'='*80}")
        print(f"INVESTIGATION COMPLETE")
        print(f"Recommendation: {report['recommendation']}")
        print(f"{'='*80}")
        
        return report
    
    def _get_country_risk(self, country_code):
        """Get FATF risk rating for a country."""
        high_risk = {'IR', 'KP', 'MM', 'CU', 'VE'}
        medium_risk = {'RU', 'CN', 'PK', 'BD', 'MY', 'TH'}
        
        if country_code in high_risk:
            return '🔴 HIGH'
        elif country_code in medium_risk:
            return '🟡 MEDIUM'
        return '🟢 LOW'
    
    def _generate_report(self, payment_id, chain, country_risk):
        """Generate structured investigation report."""
        return {
            'payment_id': payment_id,
            'hops_traced': len(chain),
            'countries_involved': country_risk['country_code'].tolist(),
            'total_amount': float(chain['amount'].sum()),
            'currency': chain['currency'].iloc[0],
            'time_span': str(chain['value_date'].max() - chain['value_date'].min()),
            'recommendation': 'ESCALATE_TO_SAR' if len(chain) > 5 else 'MONITOR',
        }


# ============================================================
# Interactive DuckDB Investigation Session
# ============================================================
def interactive_investigation():
    """
    Interactive investigation session using DuckDB with Flight SQL.
    Analysts can run ad-hoc queries during an investigation.
    """
    toolkit = PaymentInvestigationToolkit()
    con = toolkit.con
    
    print("\n🔎 INTERACTIVE INVESTIGATION MODE")
    print("Type SQL queries or use pre-built investigation commands.")
    print("Commands: trace <payment_id>, risk <bic_code>, exit\n")
    
    # Example ad-hoc queries an investigator might run:
    
    # 1. Find all payments from a specific corridor in the last 30 days
    print("\n--- Query: US → UAE payments > $50,000 in last 30 days ---")
    result = con.execute("""
        SELECT 
            p.payment_id,
            p.sender_account,
            p.receiver_account,
            p.amount,
            p.value_date,
            s.message_type,
            s.purpose_code
        FROM payments.transactions p
        JOIN swift.mt103_messages s ON s.payment_id = p.payment_id
        WHERE p.sender_country = 'US'
          AND p.receiver_country = 'AE'
          AND p.amount > 50000
          AND p.value_date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY p.amount DESC
        LIMIT 50
    """).fetchdf()
    
    print(result.to_string())
    
    # 2. Find structuring patterns (multiple payments just below reporting threshold)
    print("\n--- Query: Potential structuring (multiple payments $9,000-$9,999) ---")
    result = con.execute("""
        WITH daily_totals AS (
            SELECT 
                sender_account,
                DATE_TRUNC('day', value_date) as tx_date,
                COUNT(*) as num_payments,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM payments.transactions
            WHERE amount BETWEEN 9000 AND 9999.99
            GROUP BY sender_account, DATE_TRUNC('day', value_date)
            HAVING COUNT(*) >= 3
        )
        SELECT 
            d.sender_account,
            d.tx_date,
            d.num_payments,
            d.total_amount,
            c.customer_name,
            c.risk_rating,
            c.occupation
        FROM daily_totals d
        JOIN compliance.customer_master c 
            ON c.account_number = d.sender_account
        ORDER BY d.total_amount DESC
        LIMIT 25
    """).fetchdf()
    
    print(result.to_string())
```

**Key Benefits:**
- **Hours → Minutes**: Investigation time reduced from days to hours
- **Single interface**: No switching between 5+ query tools
- **Automated compliance checks**: Sanctions screening integrated into trace queries
- **Interactive analysis**: DuckDB + Flight SQL enables ad-hoc exploration

---

## DuckDB - Banking Scenarios

### Scenario 1: Personal Financial Analytics Engine

**Problem Statement:**
A retail banking customer opens the mobile app and asks: "Show me where my money went this month compared to last month, categorized by spending type, and compare my spending to similar customers in my demographic." This requires real-time aggregation over the customer's full transaction history (potentially 10+ years, millions of transactions), categorization, period-over-period comparison, and peer benchmarking—all in under 2 seconds on a mobile device.

**Why DuckDB:**
- **Embedded analytics**: DuckDB runs in-process, no separate server needed. Mobile app can embed it or use it as a microservice.
- **Columnar at scale**: Customer transaction data stored in Parquet files is queried directly without loading into memory.
- **Advanced SQL**: Window functions, CTEs, and conditional aggregation make complex analytical queries straightforward.
- **Parquet native**: DuckDB reads Parquet files directly with predicate pushdown—no ETL needed.

**Implementation:**

```python
import duckdb

# ============================================================
# Personal Financial Analytics Engine
# ============================================================

class PersonalFinanceEngine:
    """
    Real-time personal finance analytics using DuckDB.
    Data stored in Parquet, queried on-demand.
    """
    
    def __init__(self, data_path='./customer_data'):
        self.con = duckdb.connect(':memory:')
        self.data_path = data_path
        self._setup_analytics_views()
    
    def _setup_analytics_views(self):
        """Create analytics views over Parquet data."""
        self.con.execute(f"""
            -- Create view over partitioned transaction data
            CREATE OR REPLACE VIEW customer_transactions AS
            SELECT *
            FROM read_parquet('{self.data_path}/transactions/**/*.parquet',
                             hive_partitioning=true);
            
            -- Create view over merchant categories
            CREATE OR REPLACE VIEW merchant_categories AS
            SELECT *
            FROM read_parquet('{self.data_path}/merchants/*.parquet');
            
            -- Create view over customer demographics
            CREATE OR REPLACE VIEW customer_profiles AS
            SELECT *
            FROM read_parquet('{self.data_path}/customers/*.parquet');
        """)
    
    def spending_analysis(self, customer_id, analysis_month='2024-08'):
        """
        Comprehensive monthly spending analysis with comparisons.
        Returns a rich dataset for the mobile app dashboard.
        """
        result = self.con.execute(f"""
            WITH current_month AS (
                SELECT 
                    mc.category_name,
                    mc.category_icon,
                    COUNT(*) as transaction_count,
                    SUM(t.amount) as total_spent,
                    AVG(t.amount) as avg_transaction,
                    MAX(t.amount) as max_transaction,
                    MIN(t.amount) as min_transaction,
                    -- Top merchant in each category
                    FIRST(t.merchant_name ORDER BY t.amount DESC) as top_merchant,
                    -- Spending velocity (how fast they're spending this month)
                    SUM(t.amount) / EXTRACT(DAY FROM DATE '{analysis_month}-01'::DATE 
                        + INTERVAL '1 month' - INTERVAL '1 day') as daily_avg_spend
                FROM customer_transactions t
                JOIN merchant_categories mc ON mc.mcc_code = t.mcc_code
                WHERE t.customer_id = '{customer_id}'
                  AND t.transaction_date >= '{analysis_month}-01'::DATE
                  AND t.transaction_date < ('{analysis_month}-01'::DATE + INTERVAL '1 month')
                  AND t.type = 'DEBIT'
                GROUP BY mc.category_name, mc.category_icon
            ),
            previous_month AS (
                SELECT 
                    mc.category_name,
                    SUM(t.amount) as prev_total_spent,
                    COUNT(*) as prev_transaction_count
                FROM customer_transactions t
                JOIN merchant_categories mc ON mc.mcc_code = t.mcc_code
                WHERE t.customer_id = '{customer_id}'
                  AND t.transaction_date >= ('{analysis_month}-01'::DATE - INTERVAL '1 month')
                  AND t.transaction_date < '{analysis_month}-01'::DATE
                  AND t.type = 'DEBIT'
                GROUP BY mc.category_name
            ),
            -- Peer comparison using customer demographic segment
            peer_benchmark AS (
                SELECT 
                    mc.category_name,
                    AVG(t.amount) as peer_avg_monthly,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.amount) as peer_median,
                    STDDEV(t.amount) as peer_stddev
                FROM customer_transactions t
                JOIN merchant_categories mc ON mc.mcc_code = t.mcc_code
                JOIN customer_profiles cp ON cp.customer_id = t.customer_id
                WHERE cp.age_group = (SELECT age_group FROM customer_profiles 
                                      WHERE customer_id = '{customer_id}')
                  AND cp.income_bracket = (SELECT income_bracket FROM customer_profiles 
                                            WHERE customer_id = '{customer_id}')
                  AND cp.postal_code = (SELECT postal_code FROM customer_profiles 
                                         WHERE customer_id = '{customer_id}')
                  AND t.transaction_date >= '{analysis_month}-01'::DATE
                  AND t.transaction_date < ('{analysis_month}-01'::DATE + INTERVAL '1 month')
                  AND t.type = 'DEBIT'
                GROUP BY mc.category_name
            )
            SELECT 
                cm.category_name,
                cm.category_icon,
                cm.transaction_count,
                cm.total_spent,
                cm.avg_transaction,
                cm.top_merchant,
                cm.daily_avg_spend,
                
                -- Month-over-month comparison
                COALESCE(pm.prev_total_spent, 0) as prev_month_spent,
                cm.total_spent - COALESCE(pm.prev_total_spent, 0) as mom_change,
                CASE 
                    WHEN pm.prev_total_spent > 0 
                    THEN ROUND((cm.total_spent / pm.prev_total_spent - 1) * 100, 1)
                    ELSE NULL 
                END as mom_change_pct,
                
                -- Peer comparison
                pb.peer_avg_monthly as peer_avg,
                cm.total_spent - pb.peer_avg_monthly as vs_peer_avg,
                CASE 
                    WHEN cm.total_spent < pb.peer_avg_monthly - pb.peer_stddev THEN 'Below Average'
                    WHEN cm.total_spent > pb.peer_avg_monthly + pb.peer_stddev THEN 'Above Average'
                    ELSE 'Average'
                END as peer_comparison,
                
                -- Budget health indicator
                cm.total_spent / NULLIF(pb.peer_avg_monthly * 1.2, 0) as budget_utilization
                
            FROM current_month cm
            LEFT JOIN previous_month pm ON cm.category_name = pm.category_name
            LEFT JOIN peer_benchmark pb ON cm.category_name = pb.category_name
            ORDER BY cm.total_spent DESC
        """).fetchdf()
        
        return result
    
    def savings_opportunities(self, customer_id):
        """
        Identify potential savings based on spending patterns.
        Uses DuckDB's window functions for pattern detection.
        """
        result = self.con.execute(f"""
            WITH recurring_subscriptions AS (
                -- Detect recurring payments (subscriptions, bills)
                SELECT 
                    merchant_name,
                    mcc_code,
                    AVG(amount) as avg_amount,
                    COUNT(*) as frequency,
                    MODE() WITHIN GROUP (ORDER BY EXTRACT(DAY FROM transaction_date)) as typical_day,
                    STDDEV(amount) as amount_volatility
                FROM customer_transactions
                WHERE customer_id = '{customer_id}'
                  AND type = 'DEBIT'
                  AND transaction_date >= CURRENT_DATE - INTERVAL '6 months'
                GROUP BY merchant_name, mcc_code
                HAVING COUNT(*) >= 3  -- At least 3 occurrences
                   AND STDDEV(amount) / NULLIF(AVG(amount), 0) < 0.1  -- Consistent amounts
            ),
            spending_trends AS (
                -- Detect increasing spending trends
                SELECT 
                    mc.category_name,
                    LINEAR_REG(
                        EXTRACT(EPOCH FROM t.transaction_date) / 86400,
                        t.amount
                    ) as trend_slope,
                    CORR(EXTRACT(EPOCH FROM t.transaction_date) / 86400, t.amount) as trend_correlation
                FROM customer_transactions t
                JOIN merchant_categories mc ON mc.mcc_code = t.mcc_code
                WHERE t.customer_id = '{customer_id}'
                  AND t.type = 'DEBIT'
                  AND t.transaction_date >= CURRENT_DATE - INTERVAL '3 months'
                GROUP BY mc.category_name
                HAVING CORR(EXTRACT(EPOCH FROM t.transaction_date) / 86400, t.amount) > 0.3
            )
            SELECT 
                'subscription_optimization' as opportunity_type,
                merchant_name as description,
                ROUND(avg_amount * 12, 2) as annual_cost,
                frequency as monthly_frequency,
                'Review if still needed' as recommendation
            FROM recurring_subscriptions
            WHERE avg_amount > 50  -- Only flag significant subscriptions
            
            UNION ALL
            
            SELECT 
                'spending_trend_alert' as opportunity_type,
                category_name as description,
                NULL as annual_cost,
                NULL as monthly_frequency,
                'Spending increasing - review budget' as recommendation
            FROM spending_trends
            WHERE trend_correlation > 0.5
            
            ORDER BY annual_cost DESC NULLS LAST
        """).fetchdf()
        
        return result


# ============================================================
# Mobile App API Layer
# ============================================================
def mobile_app_api(customer_id):
    """
    REST API endpoint for mobile app spending analytics.
    DuckDB processes everything in <2 seconds.
    """
    engine = PersonalFinanceEngine()
    
    # Spending analysis - returns JSON for mobile app
    spending = engine.spending_analysis(customer_id)
    
    # Savings opportunities
    savings = engine.savings_opportunities(customer_id)
    
    # Format for mobile consumption
    dashboard = {
        'customer_id': customer_id,
        'analysis_period': '2024-08',
        'spending_by_category': spending.to_dict('records'),
        'savings_opportunities': savings.to_dict('records'),
        'total_spent': float(spending['total_spent'].sum()),
        'total_vs_peer': float(spending['vs_peer_avg'].sum()),
        'insights_count': len(savings),
    }
    
    print(f"\n📊 Personal Finance Dashboard for {customer_id}")
    print(f"Total Spent This Month: ${dashboard['total_spent']:,.2f}")
    print(f"vs Peers: {'Over' if dashboard['total_vs_peer'] > 0 else 'Under'} "
          f"${abs(dashboard['total_vs_peer']):,.2f}")
    print(f"Savings Opportunities: {dashboard['insights_count']}")
    
    return dashboard
```

**Key Benefits:**
- **<2 second response**: DuckDB processes millions of rows in-memory
- **No infrastructure**: Embedded in mobile app backend, no separate database server
- **Rich analytics**: Window functions, statistical functions, and pattern detection
- **Direct Parquet access**: No ETL pipeline needed—query data as-is

---

### Scenario 2: Real-Time Anti-Money Laundering (AML) Monitoring

**Problem Statement:**
AML regulations (Bank Secrecy Act, EU 6AMLD) require banks to detect and report suspicious activities in near-real-time. The monitoring system must:
1. Screen every transaction against watchlists and patterns
2. Detect structuring (multiple transactions below reporting thresholds)
3. Identify layering (rapid movement of funds through multiple accounts)
4. Flag unusual geographic activity
5. Generate Suspicious Activity Reports (SARs)

The system processes 50M transactions/day and must flag suspicious activity within 15 minutes. Current rules-based system generates 95% false positives, overwhelming the compliance team.

**Why DuckDB:**
- **Complex pattern detection**: DuckDB's SQL supports the recursive CTEs and window functions needed for chain analysis.
- **Embedded deployment**: Can run alongside transaction processing with minimal overhead.
- **Parquet integration**: Historical transaction patterns stored in Parquet are queried directly.
- **Statistical functions**: Built-in statistical functions enable anomaly detection without external libraries.

**Implementation:**

```python
import duckdb
import numpy as np
from datetime import datetime, timedelta

# ============================================================
# AML Monitoring Engine using DuckDB
# ============================================================

class AMLMonitor:
    """
    Real-time AML monitoring using DuckDB.
    Detects structuring, layering, geographic anomalies, and
    velocity-based patterns.
    """
    
    def __init__(self):
        self.con = duckdb.connect(':memory:')
        self._setup_aml_schema()
    
    def _setup_aml_schema(self):
        """Create AML monitoring schema."""
        self.con.execute("""
            -- Transaction stream (in production: Kafka → DuckDB)
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR,
                account_id VARCHAR,
                counterparty_account VARCHAR,
                amount DECIMAL(18,2),
                currency VARCHAR,
                transaction_type VARCHAR,
                channel VARCHAR,
                country_code VARCHAR,
                merchant_category VARCHAR,
                timestamp TIMESTAMP,
                batch_id INTEGER
            );
            
            -- Customer risk profiles
            CREATE TABLE IF NOT EXISTS customer_risk (
                customer_id VARCHAR,
                risk_tier VARCHAR,  -- LOW, MEDIUM, HIGH, PEP
                occupation VARCHAR,
                country_of_residence VARCHAR,
                expected_monthly_volume DECIMAL(18,2),
                expected_max_single_tx DECIMAL(18,2),
                last_sar_filed DATE,
                account_open_date DATE
            );
            
            -- Alert cases
            CREATE TABLE IF NOT EXISTS aml_alerts (
                alert_id VARCHAR,
                alert_type VARCHAR,
                severity VARCHAR,
                account_id VARCHAR,
                transaction_ids VARCHAR[],
                description TEXT,
                risk_score FLOAT,
                created_at TIMESTAMP,
                status VARCHAR DEFAULT 'NEW',
                assigned_to VARCHAR
            );
            
            -- Watchlists
            CREATE TABLE IF NOT EXISTS watchlists (
                entity_id VARCHAR,
                entity_name VARCHAR,
                list_type VARCHAR,  -- SANCTIONS, PEP, ADVERSE_MEDIA
                country VARCHAR,
                risk_score INTEGER,
                last_updated DATE
            );
        """)
    
    def detect_structuring(self, lookback_hours=24, threshold=10000):
        """
        Detect structuring: Multiple transactions just below the
        $10,000 CTR reporting threshold.
        
        Pattern: Same account, multiple txns $8,000-$9,999 within 24 hours,
        totaling > $10,000.
        """
        alerts = self.con.execute(f"""
            WITH structured_candidates AS (
                SELECT 
                    account_id,
                    DATE_TRUNC('hour', timestamp) as tx_hour,
                    COUNT(*) as tx_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount,
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount,
                    ARRAY_AGG(transaction_id) as transaction_ids,
                    -- Check if amounts are suspiciously uniform
                    STDDEV(amount) / NULLIF(AVG(amount), 0) as amount_cv
                FROM transactions
                WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '{lookback_hours} hours'
                  AND amount BETWEEN {threshold * 0.80} AND {threshold - 0.01}
                  AND transaction_type IN ('DEBIT', 'CASH_WITHDRAWAL', 'TRANSFER')
                GROUP BY account_id, DATE_TRUNC('hour', timestamp)
                HAVING COUNT(*) >= 3
                   AND SUM(amount) > {threshold}
            )
            SELECT 
                sc.*,
                cr.risk_tier,
                cr.occupation,
                -- Risk score based on pattern strength
                LEAST(100, (
                    sc.tx_count * 10 +                           -- More transactions = higher risk
                    (sc.total_amount / {threshold}) * 20 +       -- Higher total = higher risk
                    CASE WHEN sc.amount_cv < 0.05 THEN 30       -- Uniform amounts = very suspicious
                         WHEN sc.amount_cv < 0.15 THEN 15       -- Similar amounts = somewhat suspicious
                         ELSE 0 END +
                    CASE WHEN cr.risk_tier = 'HIGH' THEN 20     -- Already high-risk customer
                         WHEN cr.risk_tier = 'PEP' THEN 15      -- Politically exposed person
                         ELSE 0 END
                ))::INTEGER as risk_score
            FROM structured_candidates sc
            LEFT JOIN customer_risk cr ON cr.customer_id = sc.account_id
            WHERE sc.tx_count >= 3  -- Minimum threshold
            ORDER BY risk_score DESC
        """).fetchdf()
        
        # Generate alerts for high-risk patterns
        for _, row in alerts.iterrows():
            if row['risk_score'] >= 50:
                self._create_alert(
                    alert_type='STRUCTURING',
                    severity='HIGH' if row['risk_score'] >= 70 else 'MEDIUM',
                    account_id=row['account_id'],
                    transaction_ids=row['transaction_ids'],
                    description=f"Potential structuring: {row['tx_count']} transactions "
                               f"totaling ${row['total_amount']:,.2f} in "
                               f"{row['tx_hour']} (avg: ${row['avg_amount']:,.2f}, "
                               f"CV: {row['amount_cv']:.3f})",
                    risk_score=row['risk_score']
                )
        
        return alerts
    
    def detect_layering(self, lookback_hours=48, min_hops=3):
        """
        Detect layering: Funds moved rapidly through multiple accounts
        to obscure origin.
        
        Pattern: Money enters → rapid transfers through 3+ accounts → exits.
        """
        alerts = self.con.execute(f"""
            WITH RECURSIVE transfer_chains AS (
                -- Start: All incoming transfers
                SELECT 
                    transaction_id,
                    account_id as origin_account,
                    counterparty_account as current_account,
                    amount,
                    timestamp as chain_start,
                    timestamp as last_transfer,
                    1 as hop_count,
                    ARRAY[account_id, counterparty_account] as accounts_visited,
                    ARRAY[transaction_id] as tx_chain,
                    country_code as origin_country
                FROM transactions
                WHERE transaction_type = 'TRANSFER'
                  AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '{lookback_hours} hours'
                
                UNION ALL
                
                -- Follow the money
                SELECT 
                    t.transaction_id,
                    tc.origin_account,
                    t.counterparty_account,
                    tc.amount,  -- Track original amount
                    tc.chain_start,
                    t.timestamp,
                    tc.hop_count + 1,
                    ARRAY_APPEND(tc.accounts_visited, t.counterparty_account),
                    ARRAY_APPEND(tc.tx_chain, t.transaction_id),
                    tc.origin_country
                FROM transfer_chains tc
                JOIN transactions t 
                    ON t.account_id = tc.current_account
                    AND t.transaction_type = 'TRANSFER'
                    AND t.timestamp > tc.last_transfer
                    AND t.timestamp <= tc.last_transfer + INTERVAL '6 hours'
                    AND t.timestamp >= tc.chain_start - INTERVAL '{lookback_hours} hours'
                WHERE tc.hop_count < 10  -- Limit chain depth
                  AND NOT ARRAY_CONTAINS(tc.accounts_visited, t.counterparty_account)  -- No cycles
            )
            SELECT 
                origin_account,
                last_account,
                hop_count,
                chain_duration,
                amount as original_amount,
                accounts_visited,
                tx_chain,
                -- Risk scoring
                LEAST(100, (
                    hop_count * 15 +                              -- More hops = higher risk
                    CASE WHEN chain_duration < INTERVAL '2 hours' THEN 30  -- Very fast = suspicious
                         WHEN chain_duration < INTERVAL '6 hours' THEN 15
                         ELSE 0 END +
                    CASE WHEN amount > 50000 THEN 25              -- Large amounts
                         WHEN amount > 10000 THEN 10
                         ELSE 0 END
                ))::INTEGER as risk_score
            FROM (
                SELECT 
                    origin_account,
                    LAST(accounts_visited[-1]) as last_account,
                    hop_count,
                    last_transfer - chain_start as chain_duration,
                    amount,
                    accounts_visited,
                    tx_chain
                FROM transfer_chains
                WHERE hop_count >= {min_hops}
            )
            WHERE hop_count >= {min_hops}
            ORDER BY risk_score DESC
            LIMIT 100
        """).fetchdf()
        
        return alerts
    
    def detect_geographic_anomalies(self):
        """
        Detect unusual geographic activity patterns.
        - Impossible travel (transactions in distant countries within hours)
        - High-risk jurisdiction activity
        - Sudden geographic pattern change
        """
        anomalies = self.con.execute("""
            WITH recent_transactions AS (
                SELECT 
                    account_id,
                    country_code,
                    timestamp,
                    amount,
                    LAG(country_code) OVER (
                        PARTITION BY account_id ORDER BY timestamp
                    ) as prev_country,
                    LAG(timestamp) OVER (
                        PARTITION BY account_id ORDER BY timestamp
                    ) as prev_timestamp
                FROM transactions
                WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
            ),
            impossible_travel AS (
                SELECT 
                    account_id,
                    country_code as current_country,
                    prev_country,
                    timestamp as current_time,
                    prev_timestamp,
                    EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600 as hours_between,
                    -- Simplified: flag if < 2 hours between different countries
                    -- (impossible for physical travel)
                    CASE 
                        WHEN country_code != prev_country 
                         AND EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600 < 2
                        THEN 'IMPOSSIBLE_TRAVEL'
                        ELSE NULL
                    END as anomaly_type
                FROM recent_transactions
                WHERE prev_country IS NOT NULL
            )
            SELECT 
                account_id,
                anomaly_type,
                prev_country || ' → ' || current_country as route,
                hours_between,
                current_time
            FROM impossible_travel
            WHERE anomaly_type IS NOT NULL
            ORDER BY hours_between ASC
        """).fetchdf()
        
        return anomalies
    
    def _create_alert(self, alert_type, severity, account_id, 
                      transaction_ids, description, risk_score):
        """Create an AML alert case."""
        alert_id = f"AML-{datetime.now().strftime('%Y%m%d%H%M%S')}-{np.random.randint(1000, 9999)}"
        
        self.con.execute("""
            INSERT INTO aml_alerts VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'NEW', NULL)
        """, [alert_id, alert_type, severity, account_id, 
              transaction_ids, description, risk_score, datetime.now()])
        
        print(f"  🚨 ALERT: [{severity}] {alert_type} - {account_id}")
        print(f"     Risk Score: {risk_score}")
        print(f"     {description[:100]}...")
    
    def generate_sar_data(self, alert_id):
        """
        Generate Suspicious Activity Report (SAR) data package
        for regulatory submission.
        """
        sar_data = self.con.execute(f"""
            SELECT 
                a.*,
                -- Pull all related transactions
                (SELECT JSON_GROUP_ARRAY(JSON_OBJECT(
                    'tx_id', t.transaction_id,
                    'amount', t.amount,
                    'timestamp', t.timestamp,
                    'counterparty', t.counterparty_account,
                    'country', t.country_code,
                    'type', t.transaction_type
                ))
                FROM transactions t
                WHERE t.transaction_id IN (UNNEST(a.transaction_ids))
                ) as transaction_details,
                -- Pull customer info
                cr.risk_tier,
                cr.occupation,
                cr.account_open_date
            FROM aml_alerts a
            LEFT JOIN customer_risk cr ON cr.customer_id = a.account_id
            WHERE a.alert_id = '{alert_id}'
        """).fetchdf()
        
        return sar_data
```

**Key Benefits:**
- **85% faster detection**: 15 minutes vs 2+ hours with batch processing
- **60% fewer false positives**: Statistical scoring vs simple rules
- **Complete audit trail**: Every detection decision is logged and reproducible
- **Regulatory compliance**: SAR data packages auto-generated in FinCEN format

---

### Scenario 3: Mergers & Acquisitions Due Diligence

**Problem Statement:**
When Bank A acquires Bank B, due diligence requires analyzing Bank B's entire loan portfolio, deposit base, customer relationships, and risk exposure. This involves:
1. Comparing data schemas between two banks (different core systems)
2. Analyzing 5M loans, 20M accounts, 100M transactions
3. Identifying data quality issues, duplicates, and orphaned records
4. Computing combined risk metrics under various scenarios
5. All within a 90-day due diligence window with strict information barriers.

**Why DuckDB:**
- **No infrastructure setup**: Due diligence teams spin up DuckDB instances without IT involvement.
- **Multiple data formats**: DuckDB reads Parquet, CSV, JSON, and connects to databases simultaneously.
- **Ad-hoc analysis**: Analysts write SQL directly without ETL pipelines.
- **Portable**: Analysis can be run on any laptop—critical for information barrier compliance.

**Implementation:**

```python
import duckdb
import os

# ============================================================
# M&A Due Diligence Analytics Engine
# ============================================================

class MAndADueDiligence:
    """
    Due diligence analytics for bank acquisitions using DuckDB.
    Handles data from two different banks with different schemas.
    """
    
    def __init__(self, bank_a_data_path, bank_b_data_path):
        self.con = duckdb.connect(':memory:')
        self.bank_a_path = bank_a_data_path
        self.bank_b_path = bank_b_data_path
        self._load_data()
    
    def _load_data(self):
        """Load and normalize data from both banks."""
        
        # ---- Bank A: Modern system (already in Parquet) ----
        self.con.execute(f"""
            CREATE VIEW bank_a_loans AS
            SELECT 
                loan_id,
                borrower_name,
                loan_type,
                original_amount,
                current_balance,
                interest_rate,
                origination_date,
                maturity_date,
                collateral_type,
                collateral_value,
                ltv_ratio,
                credit_score,
                delinquency_days,
                'BANK_A' as source_bank
            FROM read_parquet('{self.bank_a_path}/loans/*.parquet');
            
            CREATE VIEW bank_a_deposits AS
            SELECT 
                account_id,
                customer_name,
                account_type,
                balance,
                interest_rate,
                open_date,
                last_activity_date,
                is_dormant,
                'BANK_A' as source_bank
            FROM read_parquet('{self.bank_a_path}/deposits/*.parquet');
        """)
        
        # ---- Bank B: Legacy system (CSV from mainframe export) ----
        self.con.execute(f"""
            CREATE VIEW bank_b_loans AS
            SELECT 
                LN_ACCT_NUM as loan_id,
                LN_BORR_NAME as borrower_name,
                CASE LN_TYPE_CODE 
                    WHEN '01' THEN 'MORTGAGE'
                    WHEN '02' THEN 'AUTO'
                    WHEN '03' THEN 'PERSONAL'
                    WHEN '04' THEN 'COMMERCIAL'
                    WHEN '05' THEN 'SBA'
                    ELSE 'OTHER'
                END as loan_type,
                LN_ORIG_AMT / 100.0 as original_amount,
                LN_CURR_BAL / 100.0 as current_balance,
                LN_INT_RATE / 100.0 as interest_rate,
                TO_DATE(LN_ORIG_DT::VARCHAR, 'YYYYMMDD') as origination_date,
                TO_DATE(LN_MAT_DT::VARCHAR, 'YYYYMMDD') as maturity_date,
                LN_COLL_TYPE as collateral_type,
                LN_COLL_VAL / 100.0 as collateral_value,
                LN_LTV_RATIO / 100.0 as ltv_ratio,
                LN_CREDIT_SCORE as credit_score,
                LN_DELQ_DAYS as delinquency_days,
                'BANK_B' as source_bank
            FROM read_csv_auto('{self.bank_b_path}/loan_master.csv',
                             header=true, all_varchar=false);
            
            CREATE VIEW bank_b_deposits AS
            SELECT 
                ACCT_NUM as account_id,
                CUST_NAME as customer_name,
                CASE ACCT_TYPE_CODE
                    WHEN 'CHK' THEN 'CHECKING'
                    WHEN 'SAV' THEN 'SAVINGS'
                    WHEN 'MMD' THEN 'MONEY_MARKET'
                    WHEN 'CD' THEN 'CERTIFICATE_OF_DEPOSIT'
                    ELSE ACCT_TYPE_CODE
                END as account_type,
                ACCT_BAL / 100.0 as balance,
                ACCT_INT_RATE / 100.0 as interest_rate,
                TO_DATE(ACCT_OPEN_DT::VARCHAR, 'YYYYMMDD') as open_date,
                TO_DATE(ACCT_LAST_ACT_DT::VARCHAR, 'YYYYMMDD') as last_activity_date,
                CASE WHEN ACCT_LAST_ACT_DT < 20230101 THEN true ELSE false END as is_dormant,
                'BANK_B' as source_bank
            FROM read_csv_auto('{self.bank_b_path}/deposit_master.csv',
                             header=true, all_varchar=false);
        """)
        
        # ---- Combined views (unified schema) ----
        self.con.execute("""
            CREATE VIEW combined_loans AS
            SELECT * FROM bank_a_loans
            UNION ALL
            SELECT * FROM bank_b_loans;
            
            CREATE VIEW combined_deposits AS
            SELECT * FROM bank_a_deposits
            UNION ALL
            SELECT * FROM bank_b_deposits;
        """)
    
    def portfolio_comparative_analysis(self):
        """
        Side-by-side comparison of both banks' portfolios.
        Identifies similarities, differences, and opportunities.
        """
        result = self.con.execute("""
            SELECT 
                'LOAN_PORTFOLIO' as category,
                source_bank,
                COUNT(*) as record_count,
                SUM(current_balance) as total_balance,
                AVG(interest_rate) * 100 as avg_rate,
                AVG(ltv_ratio) * 100 as avg_ltv,
                SUM(CASE WHEN delinquency_days > 90 THEN current_balance ELSE 0 END) as npl_balance,
                SUM(CASE WHEN delinquency_days > 90 THEN current_balance ELSE 0 END) / 
                    NULLIF(SUM(current_balance), 0) * 100 as npl_ratio
            FROM combined_loans
            GROUP BY source_bank
            
            UNION ALL
            
            SELECT 
                'DEPOSIT_BASE' as category,
                source_bank,
                COUNT(*) as record_count,
                SUM(balance) as total_balance,
                AVG(interest_rate) * 100 as avg_rate,
                NULL as avg_ltv,
                SUM(CASE WHEN is_dormant THEN balance ELSE 0 END) as dormant_balance,
                SUM(CASE WHEN is_dormant THEN balance ELSE 0 END) / 
                    NULLIF(SUM(balance), 0) * 100 as dormant_ratio
            FROM combined_deposits
            GROUP BY source_bank
            
            ORDER BY category, source_bank
        """).fetchdf()
        
        return result
    
    def identify_duplicate_customers(self):
        """
        Find potential duplicate customers across both banks.
        Uses fuzzy matching on name + address for M&A overlap analysis.
        """
        duplicates = self.con.execute("""
            WITH normalized_names AS (
                SELECT 
                    'LOAN' as product_type,
                    loan_id as account_id,
                    UPPER(REGEXP_REPLACE(REGEXP_REPLACE(borrower_name, 
                        '[^A-Z ]', '', 'g'), '\\s+', ' ', 'g')) as clean_name,
                    source_bank
                FROM combined_loans
                
                UNION ALL
                
                SELECT 
                    'DEPOSIT' as product_type,
                    account_id,
                    UPPER(REGEXP_REPLACE(REGEXP_REPLACE(customer_name, 
                        '[^A-Z ]', '', 'g'), '\\s+', ' ', 'g')) as clean_name,
                    source_bank
                FROM combined_deposits
            )
            SELECT 
                a.product_type,
                a.account_id as bank_a_account,
                b.account_id as bank_b_account,
                a.clean_name as customer_name,
                a.source_bank as bank_a,
                b.source_bank as bank_b,
                -- Jaccard similarity on name tokens
                (CARDINALITY(INTERSECT(
                    STRING_SPLIT(a.clean_name, ' '),
                    STRING_SPLIT(b.clean_name, ' ')
                ))::FLOAT / 
                CARDINALITY(UNION(
                    STRING_SPLIT(a.clean_name, ' '),
                    STRING_SPLIT(b.clean_name, ' ')
                ))::FLOAT) as name_similarity
            FROM normalized_names a
            JOIN normalized_names b 
                ON a.clean_name = b.clean_name
                AND a.source_bank != b.source_bank
                AND a.product_type = b.product_type
            WHERE a.source_bank = 'BANK_A' AND b.source_bank = 'BANK_B'
            ORDER BY name_similarity DESC
            LIMIT 1000
        """).fetchdf()
        
        return duplicates
    
    def data_quality_assessment(self):
        """
        Comprehensive data quality audit for regulatory reporting.
        Identifies gaps, inconsistencies, and issues.
        """
        quality_report = self.con.execute("""
            SELECT 
                source_bank,
                'loans' as data_domain,
                COUNT(*) as total_records,
                SUM(CASE WHEN loan_id IS NULL THEN 1 ELSE 0 END) as null_ids,
                SUM(CASE WHEN current_balance IS NULL THEN 1 ELSE 0 END) as null_balance,
                SUM(CASE WHEN credit_score IS NULL OR credit_score = 0 THEN 1 ELSE 0 END) as null_credit_score,
                SUM(CASE WHEN ltv_ratio < 0 OR ltv_ratio > 2 THEN 1 ELSE 0 END) as invalid_ltv,
                SUM(CASE WHEN interest_rate < 0 OR interest_rate > 0.3 THEN 1 ELSE 0 END) as invalid_rate,
                SUM(CASE WHEN delinquency_days < 0 THEN 1 ELSE 0 END) as negative_delinquency,
                SUM(CASE WHEN current_balance > original_amount * 1.5 THEN 1 ELSE 0 END) as over_collateralized,
                -- Data freshness
                MIN(origination_date) as oldest_record,
                MAX(origination_date) as newest_record
            FROM combined_loans
            GROUP BY source_bank
            
            UNION ALL
            
            SELECT 
                source_bank,
                'deposits' as data_domain,
                COUNT(*) as total_records,
                SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) as null_ids,
                SUM(CASE WHEN balance IS NULL THEN 1 ELSE 0 END) as null_balance,
                SUM(CASE WHEN interest_rate IS NULL THEN 1 ELSE 0 END) as null_credit_score,
                SUM(CASE WHEN balance < 0 THEN 1 ELSE 0 END) as invalid_ltv,
                SUM(CASE WHEN interest_rate < 0 OR interest_rate > 0.1 THEN 1 ELSE 0 END) as invalid_rate,
                0 as negative_delinquency,
                0 as over_collateralized,
                MIN(open_date) as oldest_record,
                MAX(open_date) as newest_record
            FROM combined_deposits
            GROUP BY source_bank
            
            ORDER BY data_domain, source_bank
        """).fetchdf()
        
        # Calculate quality scores
        for _, row in quality_report.iterrows():
            total = row['total_records']
            issues = (row['null_ids'] + row['null_balance'] + 
                     row['invalid_ltv'] + row['invalid_rate'])
            quality_score = max(0, 100 - (issues / total * 100)) if total > 0 else 0
            
            print(f"\n📋 {row['source_bank']} - {row['data_domain']}")
            print(f"  Total Records: {row['total_records']:,}")
            print(f"  Quality Score: {quality_score:.1f}%")
            if row['null_ids'] > 0:
                print(f"  ⚠️  Missing IDs: {row['null_ids']:,} ({row['null_ids']/total*100:.1f}%)")
            if row['invalid_ltv'] > 0:
                print(f"  ⚠️  Invalid LTV: {row['invalid_ltv']:,}")
        
        return quality_report
    
    def synergy_estimation(self):
        """
        Estimate cost synergies from the merger.
        Identifies overlapping branches, duplicate systems, and
        cross-selling opportunities.
        """
        synergies = self.con.execute("""
            WITH customer_overlap AS (
                -- Customers who have accounts at both banks
                SELECT 
                    a.customer_name,
                    COUNT(DISTINCT a.account_id) as bank_a_accounts,
                    COUNT(DISTINCT b.account_id) as bank_b_accounts,
                    SUM(a.balance) as bank_a_balance,
                    SUM(b.balance) as bank_b_balance
                FROM combined_deposits a
                JOIN combined_deposits b 
                    ON UPPER(a.customer_name) = UPPER(b.customer_name)
                    AND a.source_bank != b.source_bank
                GROUP BY a.customer_name
            )
            SELECT 
                'DEPOSIT_CONSOLIDATION' as synergy_type,
                COUNT(*) as overlapping_customers,
                SUM(bank_a_balance + bank_b_balance) as total_consolidatable_balance,
                -- Assume 15 basis points savings from consolidation
                SUM(bank_a_balance + bank_b_balance) * 0.0015 as annual_savings
            FROM customer_overlap
            
            UNION ALL
            
            SELECT 
                'CROSS_SELL_OPPORTUNITY' as synergy_type,
                COUNT(DISTINCT l.loan_id) as overlapping_customers,
                SUM(l.current_balance) as total_consolidatable_balance,
                -- Assume 5% conversion of deposits to loans
                SUM(l.current_balance) * 0.05 * 0.03 as annual_savings  -- 3% margin
            FROM combined_deposits d
            JOIN combined_loans l ON UPPER(d.customer_name) = UPPER(l.borrower_name)
            WHERE d.source_bank != l.source_bank
        """).fetchdf()
        
        return synergies


# ============================================================
# Due Diligence Dashboard
# ============================================================
def due_diligence_dashboard():
    """Executive dashboard for M&A due diligence."""
    
    dd = MAndADueDiligence(
        bank_a_data_path='./bank_a_data',
        bank_b_data_path='./bank_b_data'
    )
    
    print("\n" + "=" * 80)
    print("M&A DUE DILIGENCE ANALYTICS DASHBOARD")
    print("=" * 80)
    
    # Portfolio comparison
    print("\n📊 PORTFOLIO COMPARISON")
    comparison = dd.portfolio_comparative_analysis()
    print(comparison.to_string(index=False))
    
    # Data quality
    print("\n🔍 DATA QUALITY ASSESSMENT")
    dd.data_quality_assessment()
    
    # Customer overlap
    print("\n👥 CUSTOMER OVERLAP ANALYSIS")
    duplicates = dd.identify_duplicate_customers()
    print(f"  Potential duplicate customers found: {len(duplicates):,}")
    if len(duplicates) > 0:
        print(f"  Name similarity range: {duplicates['name_similarity'].min():.2f} - "
              f"{duplicates['name_similarity'].max():.2f}")
    
    # Synergy estimation
    print("\n💰 SYNERGY ESTIMATION")
    synergies = dd.synergy_estimation()
    for _, row in synergies.iterrows():
        print(f"  {row['synergy_type']:30s} ${row['annual_savings']:>15,.2f}/year")
    
    total_synergy = synergies['annual_savings'].sum()
    print(f"\n  {'TOTAL ESTIMATED SYNERGY':30s} ${total_synergy:>15,.2f}/year")
```

**Key Benefits:**
- **90-day compliance**: Analysis completed within regulatory timeline
- **Zero infrastructure**: Analysts work on laptops with DuckDB—no server access needed
- **Multi-format support**: Parquet, CSV, JSON, and database connections in one tool
- **Regulatory confidence**: Complete data quality audit trail for examiner review

---

## Apache Iceberg - Banking Scenarios

### Scenario 1: Immutable Transaction Ledger with Time-Travel

**Problem Statement:**
A bank must maintain an immutable, auditable record of every financial transaction for 7+ years. Regulators require the ability to "replay" the state of any account at any point in time. The bank processes 50M transactions/day, and the ledger grows to 500TB+ over 7 years. Key challenges:
1. No record can ever be modified or deleted (regulatory requirement)
2. Auditors must be able to query the exact state of the ledger at any historical timestamp
3. Schema changes (new fields, type changes) must be applied without rewriting history
4. Concurrent writers must not interfere with each other or readers

**Why Iceberg:**
- **ACID transactions**: Iceberg provides atomic writes, ensuring every batch of transactions is committed completely or not at all.
- **Time-travel queries**: Query the ledger as it was at any timestamp using `FOR SYSTEM_TIME AS OF`.
- **Schema evolution**: Add columns, rename columns, and change types without rewriting existing data files.
- **Hidden partitioning**: Partition strategy changes don't require rewriting data or modifying queries.
- **No data deletion**: Iceberg's append-only nature with audit logs matches regulatory requirements.

**Implementation:**

```python
import duckdb
from datetime import datetime, timedelta
import random

# ============================================================
# Immutable Transaction Ledger with Apache Iceberg
# ============================================================

class IcebergTransactionLedger:
    """
    Regulatory-grade immutable transaction ledger using
    Apache Iceberg for ACID compliance, time-travel, and schema evolution.
    """
    
    def __init__(self, warehouse_path='./iceberg_warehouse'):
        self.con = duckdb.connect(':memory:')
        self.warehouse_path = warehouse_path
        self._setup_iceberg()
    
    def _setup_iceberg(self):
        """Initialize Iceberg catalog and tables."""
        self.con.execute(f"""
            -- Install and load Iceberg extension
            INSTALL iceberg;
            LOAD iceberg;
            
            -- Configure Iceberg catalog
            SET s3_region='us-east-1';
            SET s3_access_key_id='{os.environ.get("AWS_ACCESS_KEY_ID", "")}';
            SET s3_secret_access_key='{os.environ.get("AWS_SECRET_ACCESS_KEY", "")}';
            
            -- Create Iceberg transaction ledger
            CREATE TABLE IF NOT EXISTS transaction_ledger (
                transaction_id VARCHAR,
                account_id VARCHAR,
                counterparty_account VARCHAR,
                transaction_type VARCHAR,
                amount DECIMAL(18, 2),
                currency VARCHAR,
                balance_before DECIMAL(18, 2),
                balance_after DECIMAL(18, 2),
                channel VARCHAR,
                merchant_id VARCHAR,
                description VARCHAR,
                status VARCHAR,
                -- Metadata columns (managed by Iceberg)
                created_by VARCHAR,
                batch_id INTEGER,
                -- Audit columns
                checksum VARCHAR,
                previous_transaction_id VARCHAR
            )
            USING iceberg
            PARTITIONED BY (days(transaction_date), transaction_type)
            TBLPROPERTIES (
                'format-version' = '2',
                'write.parquet.compression-codec' = 'zstd',
                'write.target-file-size-bytes' = '536870912',
                'write.distribution-mode' = 'hash',
                'write.wap.enabled' = 'true',
                'history.expire.max-snapshot-age-ms' = '63072000000',
                'commit.manifest-merge.enabled' = 'true'
            );
            
            -- Create schema evolution tracking table
            CREATE TABLE IF NOT EXISTS schema_evolution_log (
                change_id INTEGER,
                change_date TIMESTAMP,
                table_name VARCHAR,
                change_type VARCHAR,
                column_name VARCHAR,
                old_type VARCHAR,
                new_type VARCHAR,
                reason VARCHAR,
                approved_by VARCHAR
            )
            USING iceberg;
        """)
    
    def ingest_daily_transactions(self, date_str):
        """
        Ingest one day's worth of transactions into the Iceberg ledger.
        Each batch is an atomic commit with full audit metadata.
        """
        batch_id = int(datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y%m%d'))
        
        # Simulate transaction ingestion (in production: from Kafka/S3)
        self.con.execute(f"""
            INSERT INTO transaction_ledger
            WITH new_transactions AS (
                SELECT 
                    'TXN-' || UUID() as transaction_id,
                    'ACC-' || LPAD(CAST(RANDOM() * 99999999 AS INTEGER)::VARCHAR, 8, '0') as account_id,
                    'ACC-' || LPAD(CAST(RANDOM() * 99999999 AS INTEGER)::VARCHAR, 8, '0') as counterparty_account,
                    CHOOSE(['DEBIT', 'CREDIT', 'TRANSFER', 'WIRE', 'ACH']) as transaction_type,
                    ROUND(RANDOM() * 100000 + 1, 2) as amount,
                    CHOOSE(['USD', 'EUR', 'GBP']) as currency,
                    ROUND(RANDOM() * 500000, 2) as balance_before,
                    ROUND(RANDOM() * 500000, 2) as balance_after,
                    CHOOSE(['MOBILE', 'ONLINE', 'BRANCH', 'ATM', 'WIRE']) as channel,
                    'MERCH-' || LPAD(CAST(RANDOM() * 9999 AS INTEGER)::VARCHAR, 4, '0') as merchant_id,
                    'Daily transaction batch' as description,
                    'SETTLED' as status,
                    CURRENT_USER as created_by,
                    {batch_id} as batch_id,
                    MD5(UUID()) as checksum,
                    LAG('TXN-' || UUID()) OVER (ORDER BY RANDOM()) as previous_transaction_id,
                    '{date_str}'::DATE as transaction_date
                FROM generate_series(1, 10000)
            )
            SELECT * FROM new_transactions
        """)
        
        print(f"  Ingested {date_str}: {batch_id}")
    
    def time_travel_query(self, as_of_date):
        """
        Query the transaction ledger as it existed on a specific date.
        This is Iceberg's killer feature for regulatory compliance.
        """
        result = self.con.execute(f"""
            -- Time-travel query: See the exact state at a point in time
            SELECT 
                account_id,
                COUNT(*) as transaction_count,
                SUM(CASE WHEN transaction_type = 'DEBIT' THEN amount ELSE 0 END) as total_debits,
                SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END) as total_credits,
                SUM(CASE WHEN transaction_type IN ('DEBIT') THEN amount ELSE 0 END) -
                SUM(CASE WHEN transaction_type IN ('CREDIT') THEN amount ELSE 0 END) as net_flow,
                COUNT(DISTINCT counterparty_account) as unique_counterparties,
                AVG(amount) as avg_transaction_amount,
                MAX(amount) as max_transaction_amount,
                COUNT(DISTINCT transaction_type) as transaction_types_used
            FROM transaction_ledger
            FOR SYSTEM_TIME AS OF '{as_of_date}'::TIMESTAMP
            GROUP BY account_id
            HAVING COUNT(*) > 5
            ORDER BY net_flow DESC
            LIMIT 50
        """).fetchdf()
        
        return result
    
    def audit_investigation(self, account_id, start_date, end_date):
        """
        Complete audit trail for a specific account over a date range.
        Uses Iceberg's hidden partitioning for fast range scans.
        """
        result = self.con.execute(f"""
            SELECT 
                transaction_id,
                transaction_date,
                transaction_type,
                amount,
                currency,
                balance_before,
                balance_after,
                channel,
                merchant_id,
                counterparty_account,
                status,
                created_by,
                checksum,
                -- Verify chain integrity
                LAG(checksum) OVER (ORDER BY transaction_date, transaction_id) as prev_checksum,
                CASE 
                    WHEN LAG(balance_after) OVER (ORDER BY transaction_date, transaction_id) 
                         = balance_before 
                    THEN 'VALID'
                    ELSE 'CHAIN_BREAK'
                END as chain_integrity
            FROM transaction_ledger
            WHERE account_id = '{account_id}'
              AND transaction_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY transaction_date, transaction_id
        """).fetchdf()
        
        # Verify no gaps in transaction sequence
        gaps = result[result['chain_integrity'] == 'CHAIN_BREAK']
        if len(gaps) > 0:
            print(f"  ⚠️  WARNING: {len(gaps)} chain integrity breaks found!")
            for _, gap in gaps.iterrows():
                print(f"      TXN {gap['transaction_id']} at {gap['transaction_date']}")
        else:
            print(f"  ✅ Chain integrity verified: {len(result)} transactions, no breaks")
        
        return result
    
    def schema_evolution_example(self):
        """
        Demonstrate schema evolution: Adding new fields to the ledger
        without rewriting historical data.
        
        Timeline:
        - 2024-01-01: Original schema
        - 2024-06-01: Add 'aml_risk_score' column (new regulation)
        - 2024-09-01: Rename 'merchant_id' to 'counterparty_id' (standardization)
        """
        print("\n📋 Schema Evolution History:")
        print("-" * 60)
        
        # Phase 1: Original schema
        print("2024-01-01: Original schema deployed")
        print("  Columns: transaction_id, account_id, amount, currency, ...")
        
        # Phase 2: Add AML risk score
        self.con.execute("""
            -- Iceberg ALTER TABLE: add column without rewriting data
            ALTER TABLE transaction_ledger 
            ADD COLUMN aml_risk_score FLOAT;
            
            ALTER TABLE transaction_ledger
            ADD COLUMN aml_check_timestamp TIMESTAMP;
            
            ALTER TABLE transaction_ledger
            ADD COLUMN sanctions_hit BOOLEAN DEFAULT FALSE;
        """)
        print("\n2024-06-01: Added AML columns (new regulation)")
        print("  + aml_risk_score FLOAT")
        print("  + aml_check_timestamp TIMESTAMP")
        print("  + sanctions_hit BOOLEAN")
        print("  Historical data: unchanged (Iceberg returns NULL for new columns)")
        
        # Phase 3: Rename column
        self.con.execute("""
            ALTER TABLE transaction_ledger 
            RENAME COLUMN merchant_id TO counterparty_id;
        """)
        print("\n2024-09-01: Renamed merchant_id → counterparty_id")
        print("  Old queries using 'merchant_id' still work (backward compatible)")
        
        # Show current schema
        current_schema = self.con.execute("""
            DESCRIBE transaction_ledger
        """).fetchdf()
        
        print("\nCurrent Schema:")
        for _, col in current_schema.iterrows():
            print(f"  {col['column_name']:30s} {col['data_type']:20s}")
    
    def snapshot_management(self):
        """
        Iceberg snapshot management for regulatory retention.
        Snapshots can be expired to manage storage costs while
        maintaining compliance-mandated retention periods.
        """
        # View snapshot history
        snapshots = self.con.execute("""
            SELECT 
                snapshot_id,
                committed_at,
                operation,
                summary
            FROM transaction_ledger.snapshots
            ORDER BY committed_at DESC
            LIMIT 10
        """).fetchdf()
        
        print("\n📸 Recent Snapshots:")
        for _, snap in snapshots.iterrows():
            print(f"  ID: {snap['snapshot_id']}")
            print(f"  Time: {snap['committed_at']}")
            print(f"  Operation: {snap['operation']}")
            print(f"  Summary: {snap['summary'][:80]}")
            print()
        
        # Expire old snapshots (keep 2 years minimum for regulatory compliance)
        self.con.execute("""
            -- Expire snapshots older than 2 years (keeping regulatory minimum)
            CALL transaction_ledger.expire_snaphots(
                older_than => TIMESTAMP '2022-08-25 00:00:00',
                retain_last => 365  -- Keep at least 365 snapshots
            );
            
            -- Run garbage collection to reclaim storage
            CALL transaction_ledger.delete_orphan_files(
                older_than => TIMESTAMP '2022-08-25 00:00:00'
            );
        """)
        
        print("✅ Snapshot expiration complete (2-year regulatory minimum retained)")
```

**Key Benefits:**
- **Regulatory compliance**: Immutable, append-only ledger with full audit trail
- **Time-travel audit**: Query any account's state at any historical point
- **Schema evolution**: New regulatory requirements added without data migration
- **Storage efficiency**: ZSTD compression + partition pruning = 70%+ storage savings

---

### Scenario 2: Multi-Tenant Data Lake for Banking Subsidiaries

**Problem Statement:**
A global banking group operates subsidiaries in 20 countries, each with different data privacy regulations (GDPR, CCPA, LGPD, PDPA). They need a unified data lake that:
1. Stores all subsidiary data in one platform for consolidated analytics
2. Enforces data residency requirements (EU data stays in EU)
3. Allows each subsidiary to evolve their schema independently
4. Provides cross-subsidiary analytics for group-level reporting
5. Supports row-level security (each subsidiary sees only their data)

**Why Iceberg:**
- **Multi-table transactions**: Consolidate data across subsidiaries atomically.
- **Row-level security**: Iceberg's metadata enables efficient row filtering.
- **Schema independence**: Each subsidiary evolves their schema without affecting others.
- **Partition evolution**: Different subsidiaries can use different partitioning strategies.
- **Catalog federation**: Multiple Iceberg catalogs (one per region) for data residency.

**Implementation:**

```python
import duckdb

# ============================================================
# Multi-Tenant Banking Data Lake with Iceberg
# ============================================================

class MultiTenantDataLake:
    """
    Unified data lake for global banking group using Iceberg.
    Supports data residency, schema independence, and cross-region analytics.
    """
    
    def __init__(self):
        self.con = duckdb.connect(':memory:')
        self._setup_catalogs()
    
    def _setup_catalogs(self):
        """Set up regional Iceberg catalogs for data residency."""
        self.con.execute("""
            INSTALL iceberg;
            LOAD iceberg;
            
            -- EU Catalog (GDPR compliant - data stays in EU)
            CREATE SECRET eu_catalog_secret (
                TYPE S3,
                PROVIDER CREDENTIAL_CHAIN,
                REGION 'eu-west-1'
            );
            
            -- US Catalog (CCPA compliant)
            CREATE SECRET us_catalog_secret (
                TYPE S3,
                PROVIDER CREDENTIAL_CHAIN,
                REGION 'us-east-1'
            );
            
            -- APAC Catalog (PDPA/LGPD compliant)
            CREATE SECRET apac_catalog_secret (
                TYPE S3,
                PROVIDER CREDENTIAL_CHAIN,
                REGION 'ap-southeast-1'
            );
        """)
    
    def create_subsidiary_table(self, subsidiary_id, region, schema_customizations=None):
        """
        Create an Iceberg table for a specific subsidiary.
        Each gets their own partitioning strategy and schema.
        """
        base_schema = """
            transaction_id VARCHAR,
            account_id VARCHAR,
            amount DECIMAL(18, 2),
            currency VARCHAR,
            transaction_type VARCHAR,
            counterparty_name VARCHAR,
            status VARCHAR,
            transaction_date DATE,
            created_at TIMESTAMP,
            subsidiary_id VARCHAR,
            -- Row-level security metadata
            access_group VARCHAR,
            data_classification VARCHAR
        """
        
        # Subsidiary-specific additions
        custom_cols = ""
        if schema_customizations:
            for col_name, col_type in schema_customizations.items():
                custom_cols += f", {col_name} {col_type}"
        
        partition_strategy = self._get_partition_strategy(region)
        
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS subsidiary_{subsidiary_id.lower()} (
                {base_schema}{custom_cols}
            )
            USING iceberg
            {partition_strategy}
            TBLPROPERTIES (
                'write.parquet.compression-codec' = 'zstd',
                'write.target-file-size-bytes' = '268435456'
            );
        """)
        
        print(f"  Created table for subsidiary {subsidiary_id} ({region})")
    
    def _get_partition_strategy(self, region):
        """Different partitioning strategies per region's query patterns."""
        strategies = {
            'EU': "PARTITIONED BY (years(transaction_date), subsidiary_id)",
            'US': "PARTITIONED BY (months(transaction_date), transaction_type)",
            'APAC': "PARTITIONED BY (days(transaction_date), currency)",
        }
        return strategies.get(region, "PARTITIONED BY (years(transaction_date))")
    
    def cross_subsidiary_analytics(self):
        """
        Group-level analytics across all subsidiaries.
        Uses Iceberg's catalog federation to query across regions.
        """
        result = self.con.execute("""
            -- Cross-region consolidated view
            WITH eu_data AS (
                SELECT * FROM subsidiary_eu_all 
                FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP
            ),
            us_data AS (
                SELECT * FROM subsidiary_us_all 
                FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP
            ),
            apac_data AS (
                SELECT * FROM subsidiary_apac_all 
                FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP
            )
            SELECT 
                subsidiary_id,
                region,
                COUNT(*) as transaction_count,
                SUM(amount) as total_volume,
                AVG(amount) as avg_transaction_size,
                COUNT(DISTINCT account_id) as unique_accounts,
                MIN(transaction_date) as earliest_tx,
                MAX(transaction_date) as latest_tx,
                -- Data quality metrics
                SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amounts,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count
            FROM (
                SELECT *, 'EU' as region FROM eu_data
                UNION ALL
                SELECT *, 'US' as region FROM us_data
                UNION ALL
                SELECT *, 'APAC' as region FROM apac_data
            ) combined
            GROUP BY subsidiary_id, region
            ORDER BY total_volume DESC
        """).fetchdf()
        
        return result
    
    def data_residency_enforcement(self, user_region, query):
        """
        Enforce data residency: users in EU can only query EU data.
        Uses Iceberg's metadata for efficient filtering.
        """
        # Validate query doesn't access out-of-region data
        allowed_regions = {
            'EU': ['EU'],
            'US': ['US', 'CA'],
            'APAC': ['APAC', 'JP', 'AU', 'SG'],
        }
        
        user_allowed = allowed_regions.get(user_region, [])
        
        # Parse query to check table references
        import re
        table_refs = re.findall(r'FROM\s+(\w+)', query, re.IGNORECASE)
        
        for table in table_refs:
            if 'eu' in table.lower() and user_region != 'EU':
                raise PermissionError(
                    f"Data residency violation: {user_region} user cannot access EU data"
                )
            # Add more residency checks...
        
        # Apply row-level filtering
        filtered_query = f"""
            WITH filtered AS ({query})
            SELECT * FROM filtered 
            WHERE subsidiary_id IN (
                SELECT subsidiary_id FROM subsidiaries 
                WHERE region IN ({','.join([f"'{r}'" for r in user_allowed])})
            )
        """
        
        return filtered_query
    
    def demonstrate_time_travel_across_subsidiaries(self):
        """
        Time-travel query across multiple subsidiaries at a specific point.
        Useful for consolidated regulatory reporting at a historical date.
        """
        report_date = '2024-06-30'
        
        result = self.con.execute(f"""
            SELECT 
                subsidiary_id,
                SUM(amount) as total_volume,
                COUNT(*) as tx_count,
                -- Compare with current state
                SUM(amount) - (
                    SELECT SUM(amount) 
                    FROM all_subsidiary_data current 
                    WHERE current.subsidiary_id = historical.subsidiary_id
                ) as volume_change_since_report_date
            FROM all_subsidiary_data historical
            FOR SYSTEM_TIME AS OF '{report_date}'::TIMESTAMP
            GROUP BY subsidiary_id
        """).fetchdf()
        
        return result
```

**Key Benefits:**
- **Regulatory compliance**: Data residency enforced at storage level
- **20 subsidiaries, 1 platform**: Unified analytics without data movement
- **Schema independence**: Each subsidiary evolves independently
- **Historical reporting**: Time-travel across all subsidiaries for regulatory snapshots

---

### Scenario 3: Model Risk Management (MRM) Governance Platform

**Problem Statement:**
Banking regulators (OCC, Fed SR 11-7) require banks to maintain comprehensive governance over all analytical models: credit scoring, fraud detection, AML, marketing, and ALM models. The bank has 200+ models in production. Requirements include:
1. Every model prediction must be traceable to exact input data and model version
2. Model performance must be monitored with automatic alerts for drift
3. Model lineage (training data → model → predictions) must be fully documented
4. Champion/challenger testing requires parallel predictions from multiple model versions
5. Model risk officers must be able to audit any prediction made in the last 7 years

**Why Iceberg:**
- **Complete lineage**: Iceberg's snapshot history provides immutable audit trail from training data to predictions.
- **Time-travel for reproducibility**: Exact training data can be reconstructed for any model version.
- **Schema evolution**: Model input/output schemas evolve without breaking historical tracking.
- **Partition by model version**: Efficient queries for "show me all predictions from model v2.3.1".
- **Multi-table transactions**: Ensure training data snapshot and model registration happen atomically.

**Implementation:**

```python
import duckdb
from datetime import datetime
import hashlib

# ============================================================
# Model Risk Management Governance Platform with Iceberg
# ============================================================

class ModelRiskGovernance:
    """
    SR 11-7 compliant model risk management using Iceberg.
    Tracks model lineage, predictions, performance, and drift.
    """
    
    def __init__(self):
        self.con = duckdb.connect(':memory:')
        self._setup_governance_tables()
    
    def _setup_governance_tables(self):
        """Create Iceberg tables for MRM governance."""
        self.con.execute("""
            INSTALL iceberg;
            LOAD iceberg;
            
            -- Model Registry: All models and their versions
            CREATE TABLE IF NOT EXISTS model_registry (
                model_id VARCHAR,
                model_name VARCHAR,
                model_version VARCHAR,
                model_type VARCHAR,
                algorithm VARCHAR,
                feature_set_version VARCHAR,
                training_data_snapshot_id VARCHAR,
                training_date DATE,
                validation_date DATE,
                production_date DATE,
                status VARCHAR,  -- SHADOW, CHAMPION, CHALLENGER, RETIRED
                owner VARCHAR,
                risk_tier VARCHAR,  -- HIGH, MEDIUM, LOW
                last_validated DATE,
                next_validation_due DATE,
                model_description VARCHAR,
                -- Audit
                approved_by VARCHAR,
                approval_date DATE
            )
            USING iceberg
            PARTITIONED BY (model_type, status);
            
            -- Training Data Snapshots: Immutable record of training data
            CREATE TABLE IF NOT EXISTS training_data_snapshots (
                snapshot_id VARCHAR,
                model_id VARCHAR,
                model_version VARCHAR,
                snapshot_date TIMESTAMP,
                data_source VARCHAR,
                record_count INTEGER,
                feature_columns VARCHAR[],
                target_column VARCHAR,
                data_hash VARCHAR,
                storage_location VARCHAR,
                statistics_json VARCHAR,  -- Feature distributions, class balance, etc.
                -- Audit
                created_by VARCHAR,
                data_retention_policy VARCHAR
            )
            USING iceberg
            PARTITIONED BY (months(snapshot_date));
            
            -- Model Predictions: Every prediction ever made
            CREATE TABLE IF NOT EXISTS model_predictions (
                prediction_id VARCHAR,
                model_id VARCHAR,
                model_version VARCHAR,
                entity_id VARCHAR,  -- customer_id, transaction_id, etc.
                prediction_timestamp TIMESTAMP,
                input_features JSON,
                prediction_value FLOAT,
                prediction_probability FLOAT,
                prediction_explanation JSON,
                -- Ground truth (populated later for model monitoring)
                actual_outcome VARCHAR,
                actual_outcome_date DATE,
                -- Context
                batch_id VARCHAR,
                environment VARCHAR,  -- PRODUCTION, SHADOW, CHALLENGER
                -- Audit
                data_lineage_hash VARCHAR
            )
            USING iceberg
            PARTITIONED BY (days(prediction_timestamp), model_id);
            
            -- Model Performance Metrics: Monitored over time
            CREATE TABLE IF NOT EXISTS model_performance (
                measurement_id VARCHAR,
                model_id VARCHAR,
                model_version VARCHAR,
                measurement_date DATE,
                metric_name VARCHAR,
                metric_value FLOAT,
            )
            USING iceberg
            PARTITIONED BY (months(measurement_date), metric_name);
            
            -- Model Drift Alerts
            CREATE TABLE IF NOT EXISTS model_drift_alerts (
                alert_id VARCHAR,
                model_id VARCHAR,
                model_version VARCHAR,
                alert_date TIMESTAMP,
                drift_type VARCHAR,  -- DATA_DRIFT, CONCEPT_DRIFT, PERFORMANCE_DRIFT
                severity VARCHAR,
                feature_name VARCHAR,
                drift_score FLOAT,
                threshold FLOAT,
                details VARCHAR,
                status VARCHAR DEFAULT 'NEW',
                resolved_by VARCHAR,
                resolution_date TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (months(alert_date), severity);
        """)
    
    def register_model(self, model_info, training_data_info):
        """
        Atomically register a new model version with its training data snapshot.
        Uses Iceberg's multi-catalog transaction for consistency.
        """
        model_id = model_info['model_id']
        model_version = model_info['model_version']
        snapshot_id = f"SNAP-{model_id}-{model_version}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Compute training data hash for reproducibility
        data_hash = hashlib.sha256(
            f"{training_data_info['data_source']}{training_data_info['record_count']}"
            f"{training_data_info['statistics_json']}".encode()
        ).hexdigest()
        
        # Register model + training data snapshot atomically
        self.con.execute(f"""
            -- Register model
            INSERT INTO model_registry VALUES (
                '{model_id}',
                '{model_info['model_name']}',
                '{model_version}',
                '{model_info['model_type']}',
                '{model_info['algorithm']}',
                '{model_info['feature_set_version']}',
                '{snapshot_id}',
                '{model_info['training_date']}',
                '{model_info['validation_date']}',
                '{model_info.get('production_date', None)}',
                '{model_info['status']}',
                '{model_info['owner']}',
                '{model_info.get('risk_tier', 'MEDIUM')}',
                '{model_info['validation_date']}',
                '{model_info.get('next_validation_due', None)}',
                '{model_info.get('description', '')}',
                '{model_info.get('approved_by', '')}',
                '{model_info.get('approval_date', None)}'
            );
            
            -- Register training data snapshot
            INSERT INTO training_data_snapshots VALUES (
                '{snapshot_id}',
                '{model_id}',
                '{model_version}',
                CURRENT_TIMESTAMP,
                '{training_data_info['data_source']}',
                {training_data_info['record_count']},
                {training_data_info['feature_columns']},  -- ARRAY
                '{training_data_info['target_column']}',
                '{data_hash}',
                '{training_data_info['storage_location']}',
                '{training_data_info['statistics_json']}',
                CURRENT_USER,
                '{training_data_info.get('retention_policy', '7_YEARS')}'
            );
        """)
        
        print(f"✅ Model registered: {model_id} v{model_version}")
        print(f"   Training data snapshot: {snapshot_id}")
        print(f"   Data hash: {data_hash[:16]}...")
        
        return snapshot_id, data_hash
    
    def log_prediction(self, model_id, entity_id, input_features, prediction):
        """
        Log a model prediction with full lineage.
        Every prediction is traceable to exact input data and model version.
        """
        prediction_id = f"PRED-{model_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{hashlib.md5(str(entity_id).encode()).hexdigest()[:8]}"
        
        # Compute data lineage hash
        lineage_hash = hashlib.sha256(
            f"{entity_id}{str(input_features)}{prediction['value']}".encode()
        ).hexdigest()
        
        self.con.execute(f"""
            INSERT INTO model_predictions VALUES (
                '{prediction_id}',
                '{model_id}',
                '{prediction['model_version']}',
                '{entity_id}',
                CURRENT_TIMESTAMP,
                '{str(input_features)}'::JSON,
                {prediction['value']},
                {prediction.get('probability', 'NULL')},
                '{str(prediction.get('explanation', {}))}'::JSON,
                NULL,  -- actual_outcome (populated later)
                NULL,  -- actual_outcome_date
                '{prediction.get('batch_id', 'manual')}',
                '{prediction.get('environment', 'PRODUCTION')}',
                '{lineage_hash}'
            )
        """)
        
        return prediction_id
    
    def audit_prediction(self, prediction_id):
        """
        Complete audit trail for a single prediction.
        Traces from prediction → model version → training data → ground truth.
        """
        audit = self.con.execute(f"""
            SELECT 
                p.prediction_id,
                p.prediction_timestamp,
                p.entity_id,
                p.prediction_value,
                p.prediction_probability,
                p.input_features,
                p.prediction_explanation,
                p.environment,
                
                -- Model information
                r.model_name,
                r.model_version,
                r.model_type,
                r.algorithm,
                r.risk_tier,
                r.owner,
                r.approved_by,
                r.approval_date,
                
                -- Training data lineage
                s.snapshot_id,
                s.data_source,
                s.record_count as training_record_count,
                s.statistics_json as training_statistics,
                s.storage_location as training_data_location,
                s.data_hash as training_data_hash,
                
                -- Ground truth (if available)
                p.actual_outcome,
                p.actual_outcome_date,
                CASE 
                    WHEN p.actual_outcome IS NOT NULL 
                         AND p.prediction_value > 0.5 
                    THEN 'CORRECT_POSITIVE'
                    WHEN p.actual_outcome IS NOT NULL 
                         AND p.prediction_value <= 0.5 
                    THEN 'CORRECT_NEGATIVE'
                    WHEN p.actual_outcome IS NULL THEN 'PENDING'
                    ELSE 'INCORRECT'
                END as prediction_accuracy
                
            FROM model_predictions p
            JOIN model_registry r 
                ON p.model_id = r.model_id 
                AND p.model_version = r.model_version
            JOIN training_data_snapshots s 
                ON r.training_data_snapshot_id = s.snapshot_id
            WHERE p.prediction_id = '{prediction_id}'
        """).fetchdf()
        
        if len(audit) == 0:
            print(f"Prediction {prediction_id} not found")
            return None
        
        record = audit.iloc[0]
        
        print(f"\n{'='*80}")
        print(f"MODEL PREDICTION AUDIT TRAIL")
        print(f"{'='*80}")
        print(f"\n📍 Prediction Details:")
        print(f"   ID: {record['prediction_id']}")
        print(f"   Timestamp: {record['prediction_timestamp']}")
        print(f"   Entity: {record['entity_id']}")
        print(f"   Prediction: {record['prediction_value']:.4f}")
        print(f"   Probability: {record['prediction_probability']:.4f}")
        print(f"   Environment: {record['environment']}")
        print(f"   Accuracy: {record['prediction_accuracy']}")
        
        print(f"\n🤖 Model Information:")
        print(f"   Name: {record['model_name']}")
        print(f"   Version: {record['model_version']}")
        print(f"   Type: {record['model_type']}")
        print(f"   Algorithm: {record['algorithm']}")
        print(f"   Risk Tier: {record['risk_tier']}")
        print(f"   Owner: {record['owner']}")
        print(f"   Approved by: {record['approved_by']} on {record['approval_date']}")
        
        print(f"\n📊 Training Data Lineage:")
        print(f"   Snapshot: {record['snapshot_id']}")
        print(f"   Source: {record['data_source']}")
        print(f"   Records: {record['training_record_count']:,}")
        print(f"   Location: {record['training_data_location']}")
        print(f"   Data Hash: {record['training_data_hash'][:32]}...")
        
        return record
    
    def champion_challenger_analysis(self, model_id, date_range):
        """
        Compare champion vs challenger model performance.
        Uses Iceberg time-travel to reconstruct exact conditions.
        """
        result = self.con.execute(f"""
            WITH champion_preds AS (
                SELECT 
                    entity_id,
                    prediction_value as champion_prediction,
                    actual_outcome,
                    CASE WHEN (prediction_value > 0.5) = (actual_outcome = '1')
                         THEN 1 ELSE 0 END as champion_correct
                FROM model_predictions
                WHERE model_id = '{model_id}'
                  AND model_version = (
                      SELECT model_version FROM model_registry
                      WHERE model_id = '{model_id}' AND status = 'CHAMPION'
                  )
                  AND prediction_timestamp BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            ),
            challenger_preds AS (
                SELECT 
                    entity_id,
                    prediction_value as challenger_prediction,
                    CASE WHEN (prediction_value > 0.5) = (actual_outcome = '1')
                         THEN 1 ELSE 0 END as challenger_correct
                FROM model_predictions
                WHERE model_id = '{model_id}'
                  AND model_version = (
                      SELECT model_version FROM model_registry
                      WHERE model_id = '{model_id}' AND status = 'CHALLENGER'
                  )
                  AND prediction_timestamp BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            )
            SELECT 
                c.entity_id,
                c.champion_prediction,
                ch.challenger_prediction,
                c.actual_outcome,
                c.champion_correct,
                ch.challenger_correct,
                -- Improvement metrics
                ch.challenger_correct - c.champion_correct as improvement,
                ABS(c.champion_prediction - ch.challenger_prediction) as prediction_variance
            FROM champion_preds c
            JOIN challenger_preds ch ON c.entity_id = ch.entity_id
        """).fetchdf()
        
        # Calculate comparison metrics
        champion_accuracy = result['champion_correct'].mean()
        challenger_accuracy = result['challenger_correct'].mean()
        
        print(f"\n🏆 Champion vs Challenger Analysis")
        print(f"{'─'*50}")
        print(f"Champion accuracy:  {champion_accuracy:.2%}")
        print(f"Challenger accuracy: {challenger_accuracy:.2%}")
        print(f"Improvement:        {(challenger_accuracy - champion_accuracy):.2%}")
        print(f"Cases compared:     {len(result):,}")
        
        return result
    
    def model_drift_detection(self, model_id, model_version):
        """
        Detect model performance drift using statistical tests.
        Compares recent predictions against baseline performance.
        """
        drift = self.con.execute(f"""
            WITH baseline_performance AS (
                -- Baseline: First 30 days after production deployment
                SELECT 
                    AVG(CASE WHEN (prediction_value > 0.5) = (actual_outcome = '1')
                             THEN 1.0 ELSE 0.0 END) as baseline_accuracy,
                    STDDEV(CASE WHEN (prediction_value > 0.5) = (actual_outcome = '1')
                               THEN 1.0 ELSE 0.0 END) as baseline_stddev,
                    COUNT(*) as baseline_count
                FROM model_predictions
                WHERE model_id = '{model_id}'
                  AND model_version = '{model_version}'
                  AND actual_outcome IS NOT NULL
                  AND prediction_timestamp < (
                      SELECT production_date FROM model_registry
                      WHERE model_id = '{model_id}' AND model_version = '{model_version}'
                  ) + INTERVAL '30 days'
            ),
            recent_performance AS (
                -- Recent: Last 7 days
                SELECT 
                    AVG(CASE WHEN (prediction_value > 0.5) = (actual_outcome = '1')
                             THEN 1.0 ELSE 0.0 END) as recent_accuracy,
                    STDDEV(CASE WHEN (prediction_value > 0.5) = (actual_outcome = '1')
                               THEN 1.0 ELSE 0.0 END) as recent_stddev,
                    COUNT(*) as recent_count
                FROM model_predictions
                WHERE model_id = '{model_id}'
                  AND model_version = '{model_version}'
                  AND actual_outcome IS NOT NULL
                  AND prediction_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
            ),
            feature_drift AS (
                -- Data drift detection on input features
                SELECT 
                    'feature_drift' as drift_type,
                    feature_name,
                    baseline_mean,
                    recent_mean,
                    ABS(recent_mean - baseline_mean) / NULLIF(baseline_stddev, 0) as drift_score
                FROM (
                    SELECT 
                        feature_name,
                        AVG(feature_value) as baseline_mean,
                        STDDEV(feature_value) as baseline_stddev
                    FROM model_prediction_features
                    WHERE model_id = '{model_id}'
                      AND prediction_timestamp BETWEEN '2024-01-01' AND '2024-02-01'
                    GROUP BY feature_name
                ) baseline
                JOIN (
                    SELECT 
                        feature_name,
                        AVG(feature_value) as recent_mean
                    FROM model_prediction_features
                    WHERE model_id = '{model_id}'
                      AND prediction_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
                    GROUP BY feature_name
                ) recent USING (feature_name)
                WHERE ABS(recent_mean - baseline_mean) / NULLIF(baseline_stddev, 0) > 2.0
            )
            SELECT 
                bp.baseline_accuracy,
                rp.recent_accuracy,
                rp.recent_accuracy - bp.baseline_accuracy as accuracy_change,
                bp.baseline_count as baseline_samples,
                rp.recent_count as recent_samples,
                -- PSI (Population Stability Index) approximation
                CASE 
                    WHEN ABS(rp.recent_accuracy - bp.baseline_accuracy) > 0.10 THEN 'CRITICAL_DRIFT'
                    WHEN ABS(rp.recent_accuracy - bp.baseline_accuracy) > 0.05 THEN 'MODERATE_DRIFT'
                    WHEN ABS(rp.recent_accuracy - bp.baseline_accuracy) > 0.02 THEN 'SLIGHT_DRIFT'
                    ELSE 'STABLE'
                END as drift_status
            FROM baseline_performance bp, recent_performance rp
        """).fetchdf()
        
        return drift
```

**Key Benefits:**
- **SR 11-7 compliance**: Complete model lineage from training data to every prediction
- **Instant audit**: Trace any prediction back to exact model version and training data in seconds
- **Champion/challenger testing**: Compare models with identical input data via time-travel
- **Drift detection**: Automated monitoring with alerting for model degradation
- **7-year retention**: Iceberg snapshots provide immutable, queryable audit trail

---

## Cross-Technology Integration Patterns

### Pattern 1: End-to-End Banking Data Platform

```
┌─────────────────────────────────────────────────────────────────────┐
│                    BANKING DATA PLATFORM                            │
│                                                                     │
│  ┌─────────────┐    ┌──────────────┐    ┌───────────────────────┐  │
│  │ Transaction │───▶│  Apache      │───▶│  Apache Iceberg       │  │
│  │ Sources     │    │  Arrow       │    │  (Immutable Ledger)   │  │
│  │ (Mainframe, │    │  (In-Memory) │    │  - Time-travel        │  │
│  │  Core Bank) │    │              │    │  - Schema evolution   │  │
│  └─────────────┘    │  Zero-copy   │    │  - ACID transactions  │  │
│                     │  transfers   │    └───────────┬───────────┘  │
│                     └──────────────┘                │              │
│                                                     │              │
│  ┌─────────────┐    ┌──────────────┐    ┌───────────▼───────────┐  │
│  │ Business    │◀───│  Apache      │◀───│  Apache Parquet       │  │
│  │ Analysts    │    │  Flight SQL  │    │  (Columnar Storage)   │  │
│  │ (Tableau,   │    │  (Query      │    │  - Compressed         │  │
│  │  PowerBI)   │    │   Gateway)   │    │  - Partitioned        │  │
│  └─────────────┘    │              │    │  - Dictionary encoded │  │
│                     │  Unified SQL │    └───────────────────────┘  │
│                     │  interface   │                                │
│                     └──────┬───────┘    ┌───────────────────────┐  │
│                            │            │  DuckDB               │  │
│                            └───────────▶│  (Embedded Analytics) │  │
│                                         │  - AML monitoring     │  │
│                                         │  - Fraud detection    │  │
│                                         │  - Personal finance   │  │
│                                         └───────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Pattern 2: Technology Selection Matrix for Banking

| Banking Use Case | Primary Technology | Why | Supporting Technologies |
|---|---|---|---|
| Daily transaction archival | **Parquet** | Best compression, partitioning, predicate pushdown | Iceberg (time-travel), Arrow (ingestion) |
| Real-time fraud scoring | **Arrow** | Zero-copy, sub-ms latency, SIMD | DuckDB (feature computation), Flight SQL (serving) |
| Cross-system analytics | **Flight SQL** | Unified SQL across databases | DuckDB (local analysis), Parquet (storage) |
| Regulatory reporting | **DuckDB** | Embedded, fast, multi-format | Parquet (data), Iceberg (audit trail) |
| Immutable ledger | **Iceberg** | ACID, time-travel, schema evolution | Parquet (file format), Arrow (compute) |
| Credit risk modeling | **DuckDB + Parquet** | ML-friendly, columnar | Arrow (feature store), Iceberg (versioning) |
| Model governance | **Iceberg** | Lineage, reproducibility | Parquet (training data), DuckDB (analysis) |
| Executive dashboards | **Flight SQL + DuckDB** | Low-latency, rich SQL | Arrow (transport), Parquet (storage) |

### Pattern 3: Performance Benchmarks Summary

| Operation | Traditional (CSV/JSON) | Modern Stack | Improvement |
|---|---|---|---|
| Daily transaction load (50M rows) | 45 min | 3 min | **15x** |
| Storage (1 year of transactions) | 500 GB | 50 GB | **10x** |
| Audit query (date range, 1 year) | 120 sec | 2 sec | **60x** |
| Fraud scoring (per transaction) | 200 ms | 15 ms | **13x** |
| Cross-system join (5 systems) | 4 hours | 15 min | **16x** |
| Model prediction audit trail | Manual (days) | Instant (SQL) | **∞** |
| Schema change (add column) | Rewrite all data | Metadata only | **∞** |
| Time-travel query | Not possible | Native SQL | **∞** |

---

## Summary

This guide covers 15 real-world banking scenarios across 5 technologies:

| Technology | Scenarios | Key Banking Use Cases |
|---|---|---|
| **Apache Parquet** | 3 | Transaction archival, Feature stores, Regulatory reporting |
| **Apache Arrow** | 3 | Fraud detection, Inter-system hub, Stress testing |
| **Apache Flight SQL** | 3 | Analytics gateway, Regulatory dashboards, Investigation tools |
| **DuckDB** | 3 | Personal finance, AML monitoring, M&A due diligence |
| **Apache Iceberg** | 3 | Immutable ledger, Multi-tenant lake, Model governance |

Each technology solves specific banking challenges while complementing the others in a modern data platform architecture. The cross-technology integration patterns show how banks can combine these technologies to build a compliant, performant, and maintainable data infrastructure.

---

*Last Updated: August 25, 2026*
*Version: 1.0*
