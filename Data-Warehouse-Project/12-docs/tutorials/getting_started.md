# Getting Started Tutorial

## Banking Data Warehouse

### Prerequisites

- Docker installed
- PostgreSQL client (psql or pgAdmin)
- Python 3.8+ (for dbt)

### Step 1: Start the Platform

```bash
# Navigate to project
cd Data-Warehouse-Project

# Start PostgreSQL and pgAdmin
docker-compose -f 01-docker-setup/docker-compose.yml up -d

# Wait for services to start
sleep 10

# Verify services are running
docker-compose -f 01-docker-setup/docker-compose.yml ps
```

### Step 2: Create Source Databases

```bash
# Connect to PostgreSQL
psql -h localhost -p 5432 -U postgres

# Create source databases
CREATE DATABASE core_banking;
CREATE DATABASE cards_system;
CREATE DATABASE loans_system;

# Run schema scripts
\i 02-source-systems/core-banking/schema.sql
\i 02-source-systems/cards-system/schema.sql
\i 02-source-systems/loans-system/schema.sql
```

### Step 3: Create Data Warehouse

```bash
# Create DW database
CREATE DATABASE banking_dw;

# Run star schema
\i 03-star-schema/scripts/create_schema.sql
\i 03-star-schema/dimensions/dim_customer.sql
\i 03-star-schema/dimensions/dim_account.sql
\i 03-star-schema/dimensions/dim_date.sql
\i 03-star-schema/dimensions/dim_branch.sql
\i 03-star-schema/dimensions/dim_product.sql
\i 03-star-schema/facts/fact_transactions.sql
\i 03-star-schema/facts/fact_account_balance.sql
\i 03-star-schema/facts/fact_loan_payment.sql
```

### Step 4: Run ETL

```bash
# Extract source data
python 04-etl-pipelines/airflow/dags/extract_source_data.py

# Load dimensions
python 04-etl-pipelines/airflow/dags/load_dimensions.py

# Load facts
python 04-etl-pipelines/airflow/dags/load_facts.py
```

### Step 5: Verify Data

```bash
# Check row counts
SELECT 'dim_customer' as table_name, COUNT(*) as rows FROM gold.dim_customer
UNION ALL
SELECT 'dim_account', COUNT(*) FROM gold.dim_account
UNION ALL
SELECT 'fact_transactions', COUNT(*) FROM gold.fact_transactions;
```

### Step 6: Access pgAdmin

- Open browser: http://localhost:5050
- Login: admin@banking.com / admin
- Add server: PostgreSQL (host: postgres, port: 5432)

## Next Steps

1. Run dbt models: `cd 06-dbt-models && dbt run`
2. Check data quality: `dbt test`
3. Explore banking scenarios in `05-banking-scenarios/`
4. Set up monitoring in `08-monitoring/`

---

*Back to: [Main README](../README.md)*
