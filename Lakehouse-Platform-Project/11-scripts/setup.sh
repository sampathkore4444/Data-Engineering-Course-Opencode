#!/bin/bash
# =============================================================================
# ONE-CLICK SETUP SCRIPT - Banking Data Platform
# =============================================================================
# Purpose: Set up the complete banking data platform
# Usage: ./setup.sh [environment]
# Environment: dev (default), staging, production
# =============================================================================

set -e  # Exit on error

# Configuration
ENVIRONMENT=${1:-dev}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check kubectl (optional)
    if command -v kubectl &> /dev/null; then
        log_info "kubectl detected: $(kubectl version --client --short)"
    fi
    
    # Check helm (optional)
    if command -v helm &> /dev/null; then
        log_info "helm detected: $(helm version --short)"
    fi
    
    log_success "Prerequisites check completed"
}

# Create directory structure
create_directories() {
    log_info "Creating directory structure..."
    
    mkdir -p "$PROJECT_DIR"/{data,logs,backups,config}
    mkdir -p "$PROJECT_DIR"/data/{bronze,silver,gold}
    mkdir -p "$PROJECT_DIR"/logs/{dremio,kafka,airflow}
    mkdir -p "$PROJECT_DIR"/backups/{daily,weekly,monthly}
    mkdir -p "$PROJECT_DIR"/config/{dremio,kafka,airflow}
    
    log_success "Directory structure created"
}

# Generate environment file
generate_env_file() {
    log_info "Generating .env file..."
    
    cat > "$PROJECT_DIR/.env" << EOF
# =============================================================================
# ENVIRONMENT CONFIGURATION - Banking Data Platform
# =============================================================================
# Environment: $ENVIRONMENT
# Generated: $(date)
# =============================================================================

# General
ENVIRONMENT=$ENVIRONMENT
COMPOSE_PROJECT_NAME=banking-data-platform

# Dremio
DREMIO_VERSION=24.1.0
DREMIO_ADMIN_USER=admin
DREMIO_ADMIN_PASSWORD=Admin@123
DREMIO_ROOT_PATH=/opt/dremio/data

# Kafka
KAFKA_VERSION=3.5.0
KAFKA_BROKER_COUNT=3
KAFKA_ZOOKEEPER_COUNT=3

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=Minio@123
MINIO_BUCKET=banking-lake

# PostgreSQL
POSTGRES_USER=banking
POSTGRES_PASSWORD=Banking@123
POSTGRES_DB=banking

# Airflow
AIRFLOW_VERSION=2.8.0
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=Airflow@123
AIRFLOW_ADMIN_EMAIL=admin@bank.com

# Monitoring
GRAFANA_VERSION=10.2.0
PROMETHEUS_VERSION=2.48.0

# Security
SSL_ENABLED=false
TLS_CERT_PATH=/etc/ssl/certs
TLS_KEY_PATH=/etc/ssl/private

# Backup
BACKUP_RETENTION_DAYS=30
BACKUP_S3_BUCKET=banking-backups

# Logging
LOG_LEVEL=INFO
LOG_RETENTION_DAYS=90
EOF
    
    log_success ".env file generated"
}

# Start Docker Compose
start_services() {
    log_info "Starting services..."
    
    cd "$PROJECT_DIR"
    
    # Start base services first
    log_info "Starting base services (MinIO, PostgreSQL, Redis)..."
    docker-compose -f docker-compose.yml up -d minio postgres redis
    
    # Wait for base services to be ready
    log_info "Waiting for base services to be ready..."
    sleep 30
    
    # Start Kafka cluster
    log_info "Starting Kafka cluster..."
    docker-compose -f docker-compose.yml up -d zookeeper kafka
    
    # Wait for Kafka to be ready
    log_info "Waiting for Kafka to be ready..."
    sleep 30
    
    # Start Dremio
    log_info "Starting Dremio..."
    docker-compose -f docker-compose.yml up -d dremio-master dremio-executor-1 dremio-executor-2
    
    # Wait for Dremio to be ready
    log_info "Waiting for Dremio to be ready..."
    sleep 60
    
    # Start Airflow
    log_info "Starting Airflow..."
    docker-compose -f docker-compose.yml up -d airflow-webserver airflow-scheduler airflow-worker
    
    # Start monitoring
    log_info "Starting monitoring stack..."
    docker-compose -f docker-compose.yml up -d prometheus grafana
    
    log_success "All services started"
}

# Initialize MinIO buckets
init_minio() {
    log_info "Initializing MinIO buckets..."
    
    # Wait for MinIO to be ready
    sleep 10
    
    # Create buckets
    docker exec minio mc alias set local http://localhost:9000 minioadmin Minio@123
    docker exec minio mc mb local/banking-lake --ignore-existing
    docker exec minio mc mb local/banking-lake/bronze --ignore-existing
    docker exec minio mc mb local/banking-lake/silver --ignore-existing
    docker exec minio mc mb local/banking-lake/gold --ignore-existing
    docker exec minio mc mb local/banking-backups --ignore-existing
    
    # Set bucket policies
    docker exec minio mc anonymous set download local/banking-lake
    
    log_success "MinIO buckets initialized"
}

# Initialize Dremio
init_dremio() {
    log_info "Initializing Dremio..."
    
    # Wait for Dremio to be ready
    sleep 30
    
    # Create source connection to MinIO
    curl -X POST "http://localhost:9047/apiv2/source" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $(curl -s -X POST 'http://localhost:9047/apiv2/login' \
        -H 'Content-Type: application/json' \
        -d '{"userName":"admin","password":"Admin@123"}' | jq -r '.token')" \
      -d '{
        "name": "minio-s3",
        "type": "S3",
        "config": {
          "credentialType": "ACCESS_KEY",
          "accessKey": "minioadmin",
          "secretKey": "Minio@123",
          "endpoint": "http://minio:9000",
          "region": "us-east-1",
          "pathStyleAccess": true
        }
      }'
    
    log_success "Dremio initialized"
}

# Initialize Kafka topics
init_kafka() {
    log_info "Initializing Kafka topics..."
    
    # Wait for Kafka to be ready
    sleep 20
    
    # Create topics
    docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic accounts \
      --partitions 12 --replication-factor 3
    
    docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic customers \
      --partitions 6 --replication-factor 3
    
    docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic transactions \
      --partitions 24 --replication-factor 3
    
    docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic cards \
      --partitions 6 --replication-factor 3
    
    docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic card_transactions \
      --partitions 24 --replication-factor 3
    
    docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic loan_accounts \
      --partitions 6 --replication-factor 3
    
    docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic loan_payments \
      --partitions 12 --replication-factor 3
    
    docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic fraud-alerts \
      --partitions 6 --replication-factor 3
    
    docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic aml-alerts \
      --partitions 6 --replication-factor 3
    
    log_success "Kafka topics initialized"
}

# Initialize PostgreSQL
init_postgres() {
    log_info "Initializing PostgreSQL..."
    
    # Wait for PostgreSQL to be ready
    sleep 10
    
    # Create database and schemas
    docker exec postgres psql -U banking -d banking -c "
      CREATE SCHEMA IF NOT EXISTS banking_raw;
      CREATE SCHEMA IF NOT EXISTS banking_cleansed;
      CREATE SCHEMA IF NOT EXISTS banking_gold;
      CREATE SCHEMA IF NOT EXISTS banking_audit;
      CREATE SCHEMA IF NOT EXISTS banking_security;
    "
    
    # Create audit tables
    docker exec postgres psql -U banking -d banking -f /opt/sql/audit-config.sql
    
    log_success "PostgreSQL initialized"
}

# Initialize Airflow
init_airflow() {
    log_info "Initializing Airflow..."
    
    # Wait for Airflow to be ready
    sleep 30
    
    # Create admin user
    docker exec airflow-webserver airflow users create \
      --username admin \
      --password Admin@123 \
      --firstname Admin \
      --lastname User \
      --role Admin \
      --email admin@bank.com
    
    log_success "Airflow initialized"
}

# Initialize Grafana
init_grafana() {
    log_info "Initializing Grafana..."
    
    # Wait for Grafana to be ready
    sleep 20
    
    # Add Prometheus datasource
    curl -X POST "http://localhost:3000/api/datasources" \
      -H "Content-Type: application/json" \
      -H "Authorization: Basic $(echo -n 'admin:admin' | base64)" \
      -d '{
        "name": "Prometheus",
        "type": "prometheus",
        "url": "http://prometheus:9090",
        "access": "proxy",
        "isDefault": true
      }'
    
    log_success "Grafana initialized"
}

# Load sample data
load_sample_data() {
    log_info "Loading sample data..."
    
    # Load core banking data
    docker exec postgres psql -U banking -d banking -f /opt/sql/seed-data.sql
    
    # Load credit card data
    docker exec postgres psql -U banking -d banking -f /opt/sql/credit-card-seed-data.sql
    
    # Load loan data
    docker exec postgres psql -U banking -d banking -f /opt/sql/loan-seed-data.sql
    
    log_success "Sample data loaded"
}

# Print summary
print_summary() {
    log_success "=================================================="
    log_success "BANKING DATA PLATFORM SETUP COMPLETE"
    log_success "=================================================="
    log_success ""
    log_success "Environment: $ENVIRONMENT"
    log_success ""
    log_success "Services:"
    log_success "  - Dremio: http://localhost:9047"
    log_success "  - Airflow: http://localhost:8080"
    log_success "  - Grafana: http://localhost:3000"
    log_success "  - MinIO: http://localhost:9001"
    log_success "  - Prometheus: http://localhost:9090"
    log_success ""
    log_success "Default Credentials:"
    log_success "  - Dremio: admin / Admin@123"
    log_success "  - Airflow: admin / Admin@123"
    log_success "  - Grafana: admin / admin"
    log_success "  - MinIO: minioadmin / Minio@123"
    log_success ""
    log_success "Next Steps:"
    log_success "  1. Open Dremio and create spaces"
    log_success "  2. Configure source connections"
    log_success "  3. Enable reflections"
    log_success "  4. Run sample queries"
    log_success "  5. Set up monitoring dashboards"
    log_success ""
    log_success "Documentation: $PROJECT_DIR/12-docs/"
    log_success "=================================================="
}

# Main execution
main() {
    log_info "Starting Banking Data Platform setup..."
    log_info "Environment: $ENVIRONMENT"
    log_info ""
    
    check_prerequisites
    create_directories
    generate_env_file
    start_services
    init_minio
    init_dremio
    init_kafka
    init_postgres
    init_airflow
    init_grafana
    load_sample_data
    print_summary
}

# Run main function
main "$@"
