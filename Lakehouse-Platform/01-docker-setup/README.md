# Docker Setup for Banking Data Platform

## Prerequisites

- Docker Desktop (Windows/Mac) or Docker Engine (Linux)
- Docker Compose v2.0+
- 16GB RAM minimum (recommended: 32GB)
- 50GB free disk space

## Quick Start

### 1. Clone and Navigate to Project

```bash
cd Lakehouse-Platform/01-docker-setup
```

### 2. Start All Services

```bash
# Start Core Banking (PostgreSQL)
docker compose up -d postgres-core-banking

# Wait for PostgreSQL to be ready (30 seconds)
sleep 30

# Initialize Core Banking Database
docker compose exec postgres-core-banking psql -U postgres -d core_banking -f /docker-entrypoint-initdb.d/01-schema.sql
docker compose exec postgres-core-banking psql -U postgres -d core_banking -f /docker-entrypoint-initdb.d/02-seed-data.sql

# Start Credit Cards (MySQL)
docker compose up -d mysql-credit-cards

# Wait for MySQL to be ready (30 seconds)
sleep 30

# Start Dremio (Data Virtualization)
docker compose up -d dremio-master dremio-executor

# Wait for Dremio to be ready (2-3 minutes)
sleep 180
```

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Dremio UI** | http://localhost:9047 | admin / admin123 |
| **PostgreSQL** | localhost:5432 | postgres / postgres |
| **MySQL** | localhost:3306 | root / password |
| **MinIO (S3)** | http://localhost:9000 | admin / password |
| **Spark Master** | http://localhost:8080 | - |

### 4. Configure Dremio Connections

1. Open Dremio UI: http://localhost:9047
2. Login with admin / admin123
3. Add Source Connections:
   - Click **+** → **Add Source**
   - Select **PostgreSQL**
   - Name: `banking-postgres`
   - Host: `postgres-core-banking`
   - Port: `5432`
   - Database: `core_banking`
   - Username: `postgres`
   - Password: `postgres`
   - Click **Save**

4. Add MySQL Source:
   - Click **+** → **Add Source**
   - Select **MySQL**
   - Name: `banking-mysql`
   - Host: `mysql-credit-cards`
   - Port: `3306`
   - Database: `credit_cards`
   - Username: `root`
   - Password: `password`
   - Click **Save**

### 5. Run Dremio SQL Scripts

```bash
# Copy SQL scripts to Dremio container
docker cp ../03-dremio-sql dremio-master:/opt/dremio/sql

# Execute SQL scripts
docker compose exec dremio-master /opt/dremio/bin/dremio-client \
    --user admin --password admin123 \
    /opt/dremio/sql/01-source-connections.sql
```

## Service Management

### Start Services

```bash
# Start all services
docker compose up -d

# Start specific service
docker compose up -d postgres-core-banking

# Start with logs
docker compose up -d -f docker-compose.yml --remove-orphans
```

### Stop Services

```bash
# Stop all services
docker compose down

# Stop and remove volumes
docker compose down -v

# Stop specific service
docker compose stop postgres-core-banking
```

### View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f postgres-core-banking

# Last 100 lines
docker compose logs --tail 100 dremio-master
```

## Troubleshooting

### Common Issues

#### 1. Port Already in Use

```bash
# Check what's using the port
netstat -ano | findstr :5432

# Stop the conflicting service or change port in docker-compose.yml
```

#### 2. Insufficient Memory

```bash
# Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory → 8GB+
```

#### 3. Dremio Won't Start

```bash
# Check Dremio logs
docker compose logs dremio-master

# Common fix: Remove Dremio data and restart
docker compose down -v
docker compose up -d
```

#### 4. PostgreSQL Connection Refused

```bash
# Wait for PostgreSQL to be fully started
docker compose exec postgres-core-banking pg_isready -U postgres

# Check if database exists
docker compose exec postgres-core-banking psql -U postgres -l
```

#### 5. MySQL Authentication Plugin Error

```bash
# Fix MySQL authentication
docker compose exec mysql-credit-cards mysql -u root -ppassword -e "
ALTER USER 'root' IDENTIFIED WITH mysql_native_password BY 'password';
FLUSH PRIVILEGES;
"
```

## Data Verification

### Verify PostgreSQL Data

```bash
docker compose exec postgres-core-banking psql -U postgres -d core_banking -c "
SELECT 
    'customers' as table_name, COUNT(*) as row_count FROM customers
UNION ALL
SELECT 'accounts', COUNT(*) FROM accounts
UNION ALL
SELECT 'transactions', COUNT(*) FROM transactions
UNION ALL
SELECT 'loan_accounts', COUNT(*) FROM loan_accounts
UNION ALL
SELECT 'loan_payments', COUNT(*) FROM loan_payments;
"
```

### Verify MySQL Data

```bash
docker compose exec mysql-credit-cards mysql -u root -ppassword credit_cards -e "
SELECT 
    'credit_cards' as table_name, COUNT(*) as row_count FROM credit_cards
UNION ALL
SELECT 'card_transactions', COUNT(*) FROM card_transactions
UNION ALL
SELECT 'card_billing', COUNT(*) FROM card_billing;
"
```

### Verify Dremio Virtual Datasets

```bash
# Query Dremio using CLI
docker compose exec dremio-master /opt/dremio/bin/dremio-client \
    --user admin --password admin123 \
    -c "SELECT COUNT(*) FROM \"banking-vault\".\"virtual.customer_360\";"
```

## Performance Tuning

### Dremio Memory Settings

Edit `docker-compose.yml` to adjust Dremio memory:

```yaml
dremio-master:
  environment:
    - DREMIO_MAX_MEMORY=8G  # Increase for large datasets
    - DREMIO_EXECUTOR_MEMORY=16G
```

### PostgreSQL Performance

Edit PostgreSQL configuration:

```bash
docker compose exec postgres-core-banking psql -U postgres -d core_banking -c "
ALTER SYSTEM SET shared_buffers = '2GB';
ALTER SYSTEM SET work_mem = '256MB';
SELECT pg_reload_conf();
"
```

## Cleanup

### Remove Everything

```bash
# Stop and remove all containers, volumes, networks
docker compose down -v --remove-orphans

# Remove Docker images
docker rmi $(docker images -q | head -20)

# Remove project data
rm -rf ../data
```

### Backup Data

```bash
# Backup PostgreSQL
docker compose exec postgres-core-banking pg_dump -U postgres core_banking > backup.sql

# Backup MySQL
docker compose exec mysql-credit-cards mysqldump -u root -ppassword credit_cards > backup.sql

# Backup Dremio
docker cp dremio-master:/opt/dremio/data ./dremio-backup
```

## Next Steps

After successful setup:

1. **Open Dremio UI**: http://localhost:9047
2. **Create Source Connections**: Follow step 4 above
3. **Run SQL Scripts**: Execute the banking queries
4. **Explore Virtual Datasets**: Navigate the data
5. **Create Dashboards**: Connect BI tools to Dremio

For detailed SQL examples, see `../03-dremio-sql/` directory.