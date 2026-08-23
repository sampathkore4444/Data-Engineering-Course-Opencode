#!/bin/bash
# Banking Data Platform - Quick Start Script
# ==========================================

set -e

echo "=========================================="
echo "  Banking Data Platform - Quick Start"
echo "=========================================="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker Desktop.${NC}"
    exit 1
fi

if ! command -v docker compose &> /dev/null; then
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose v2.${NC}"
    exit 1
fi

echo -e "${GREEN}Prerequisites check passed!${NC}"

# Navigate to docker-setup directory
cd 01-docker-setup

echo ""
echo "Step 1/5: Starting Core Banking (PostgreSQL)..."
docker compose up -d postgres-core-banking
echo -e "${GREEN}Core Banking started!${NC}"

echo ""
echo "Step 2/5: Waiting for PostgreSQL to initialize..."
sleep 30

echo "Initializing database schema..."
docker compose exec postgres-core-banking psql -U postgres -d core_banking -c "
CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";
"
docker compose exec -T postgres-core-banking psql -U postgres -d core_banking < ../02-source-systems/core-banking/schema.sql
docker compose exec -T postgres-core-banking psql -U postgres -d core_banking < ../02-source-systems/core-banking/seed-data.sql
echo -e "${GREEN}Database initialized with sample data!${NC}"

echo ""
echo "Step 3/5: Starting Credit Cards System (MySQL)..."
docker compose up -d mysql-credit-cards
echo -e "${GREEN}Credit Cards system started!${NC}"

echo ""
echo "Step 4/5: Waiting for MySQL to initialize..."
sleep 30

echo "Initializing Credit Cards database..."
docker compose exec -T mysql-credit-cards mysql -u root -ppassword credit_cards < ../02-source-systems/credit-cards/schema.sql
docker compose exec -T mysql-credit-cards mysql -u root -ppassword credit_cards < ../02-source-systems/credit-cards/seed-data.sql
echo -e "${GREEN}Credit Cards database initialized!${NC}"

echo ""
echo "Step 5/5: Starting Dremio (Data Virtualization)..."
docker compose up -d dremio-master dremio-executor
echo -e "${GREEN}Dremio started!${NC}"

echo ""
echo "=========================================="
echo -e "${GREEN}Setup Complete!${NC}"
echo "=========================================="
echo ""
echo "Services are starting up. This may take 2-3 minutes."
echo ""
echo "Access Points:"
echo "  - Dremio UI:        http://localhost:9047 (admin / admin123)"
echo "  - PostgreSQL:       localhost:5432 (postgres / postgres)"
echo "  - MySQL:            localhost:3306 (root / password)"
echo ""
echo "Next Steps:"
echo "  1. Open Dremio UI in your browser"
echo "  2. Add PostgreSQL source connection (banking-postgres)"
echo "  3. Add MySQL source connection (banking-mysql)"
echo "  4. Run SQL scripts from ../03-dremio-sql/"
echo ""
echo "For detailed instructions, see README.md in this directory."
echo ""
echo "=========================================="