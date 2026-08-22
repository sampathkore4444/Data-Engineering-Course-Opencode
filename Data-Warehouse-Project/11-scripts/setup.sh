#!/bin/bash
# =============================================================================
# ONE-CLICK SETUP SCRIPT - Banking Data Warehouse
# =============================================================================
# Usage: ./setup.sh [environment]
# =============================================================================

set -e

ENVIRONMENT=${1:-dev}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_prerequisites() {
    log_info "Checking prerequisites..."
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed."
        exit 1
    fi
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed."
        exit 1
    fi
    log_success "Prerequisites check completed"
}

start_services() {
    log_info "Starting services..."
    cd "$PROJECT_DIR/01-docker-setup"
    docker-compose up -d
    log_info "Waiting for PostgreSQL to be ready..."
    sleep 15
    log_success "All services started"
}

create_dw_schema() {
    log_info "Creating Data Warehouse schema..."
    docker exec postgres-dw psql -U dw_admin -d banking_dw -c "
        CREATE SCHEMA IF NOT EXISTS dw;
        CREATE SCHEMA IF NOT EXISTS staging;
    "
    log_success "DW schema created"
}

load_sample_data() {
    log_info "Loading sample data..."
    docker exec postgres-source psql -U source_admin -d banking_source -f /docker-entrypoint-initdb.d/core-banking/schema.sql
    docker exec postgres-source psql -U source_admin -d banking_source -f /docker-entrypoint-initdb.d/cards-system/schema.sql
    docker exec postgres-source psql -U source_admin -d banking_source -f /docker-entrypoint-initdb.d/loans-system/schema.sql
    log_success "Sample data loaded"
}

create_star_schema() {
    log_info "Creating star schema..."
    # Dimensions
    docker exec postgres-dw psql -U dw_admin -d banking_dw -f /docker-entrypoint-initdb.d/star-schema/dimensions/dim_date.sql
    docker exec postgres-dw psql -U dw_admin -d banking_dw -f /docker-entrypoint-initdb.d/star-schema/dimensions/dim_customer.sql
    docker exec postgres-dw psql -U dw_admin -d banking_dw -f /docker-entrypoint-initdb.d/star-schema/dimensions/dim_account.sql
    docker exec postgres-dw psql -U dw_admin -d banking_dw -f /docker-entrypoint-initdb.d/star-schema/dimensions/dim_branch.sql
    docker exec postgres-dw psql -U dw_admin -d banking_dw -f /docker-entrypoint-initdb.d/star-schema/dimensions/dim_product.sql
    # Facts
    docker exec postgres-dw psql -U dw_admin -d banking_dw -f /docker-entrypoint-initdb.d/star-schema/facts/fact_transactions.sql
    docker exec postgres-dw psql -U dw_admin -d banking_dw -f /docker-entrypoint-initdb.d/star-schema/facts/fact_account_balance.sql
    docker exec postgres-dw psql -U dw_admin -d banking_dw -f /docker-entrypoint-initdb.d/star-schema/facts/fact_loan_payment.sql
    log_success "Star schema created"
}

print_summary() {
    log_success "=================================================="
    log_success "BANKING DATA WAREHOUSE SETUP COMPLETE"
    log_success "=================================================="
    log_success ""
    log_success "Services:"
    log_success "  - PostgreSQL DW: localhost:5432"
    log_success "  - PostgreSQL Source: localhost:5433"
    log_success "  - pgAdmin: http://localhost:5050"
    log_success ""
    log_success "Default Credentials:"
    log_success "  - DW PostgreSQL: dw_admin / Dw@123"
    log_success "  - Source PostgreSQL: source_admin / Source@123"
    log_success "  - pgAdmin: admin@bank.com / Admin@123"
    log_success ""
    log_success "Next Steps:"
    log_success "  1. Open pgAdmin and explore the star schema"
    log_success "  2. Run sample queries from 05-banking-scenarios/"
    log_success "  3. Set up Airflow for ETL pipelines"
    log_success "=================================================="
}

main() {
    log_info "Starting Banking Data Warehouse setup..."
    check_prerequisites
    start_services
    create_dw_schema
    load_sample_data
    create_star_schema
    print_summary
}

main "$@"
