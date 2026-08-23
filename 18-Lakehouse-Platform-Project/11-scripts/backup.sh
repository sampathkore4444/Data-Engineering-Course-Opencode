#!/bin/bash
# =============================================================================
# BACKUP SCRIPT - Banking Data Platform
# =============================================================================
# Purpose: Backup banking data platform components
# Usage: ./backup.sh [component] [retention_days]
# Components: all, dremio, kafka, minio, postgres, airflow
# =============================================================================

set -e

COMPONENT=${1:-all}
RETENTION_DAYS=${2:-30}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BACKUP_DIR="$PROJECT_DIR/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Create backup directory
create_backup_dir() {
    mkdir -p "$BACKUP_DIR/daily"
    mkdir -p "$BACKUP_DIR/weekly"
    mkdir -p "$BACKUP_DIR/monthly"
}

# Backup Dremio metadata
backup_dremio() {
    log_info "Backing up Dremio metadata..."
    
    # Export Dremio metadata
    curl -X GET "http://localhost:9047/apiv2/catalog" \
      -H "Authorization: Bearer $(curl -s -X POST 'http://localhost:9047/apiv2/login' \
        -H 'Content-Type: application/json' \
        -d '{"userName":"admin","password":"Admin@123"}' | jq -r '.token')" \
      > "$BACKUP_DIR/daily/dremio_catalog_$TIMESTAMP.json"
    
    # Export reflections
    curl -X GET "http://localhost:9047/apiv3/reflection" \
      -H "Authorization: Bearer $(curl -s -X POST 'http://localhost:9047/apiv2/login' \
        -H 'Content-Type: application/json' \
        -d '{"userName":"admin","password":"Admin@123"}' | jq -r '.token')" \
      > "$BACKUP_DIR/daily/dremio_reflections_$TIMESTAMP.json"
    
    # Export jobs
    curl -X GET "http://localhost:9047/apiv2/job" \
      -H "Authorization: Bearer $(curl -s -X POST 'http://localhost:9047/apiv2/login' \
        -H 'Content-Type: application/json' \
        -d '{"userName":"admin","password":"Admin@123"}' | jq -r '.token')" \
      > "$BACKUP_DIR/daily/dremio_jobs_$TIMESTAMP.json"
    
    log_success "Dremio backup completed"
}

# Backup Kafka topics
backup_kafka() {
    log_info "Backing up Kafka topics..."
    
    # Get list of topics
    TOPICS=$(docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list)
    
    for TOPIC in $TOPICS; do
        log_info "Backing up topic: $TOPIC"
        
        # Export topic data
        docker exec kafka-1 kafka-console-consumer \
          --bootstrap-server localhost:9092 \
          --topic "$TOPIC" \
          --from-beginning \
          --timeout-ms 10000 \
          > "$BACKUP_DIR/daily/kafka_${TOPIC}_$TIMESTAMP.json"
        
        # Export topic configuration
        docker exec kafka-1 kafka-configs --bootstrap-server localhost:9092 \
          --entity-type topics --entity-name "$TOPIC" --describe \
          > "$BACKUP_DIR/daily/kafka_${TOPIC}_config_$TIMESTAMP.txt"
    done
    
    # Export topic list
    docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list \
      > "$BACKUP_DIR/daily/kafka_topics_$TIMESTAMP.txt"
    
    log_success "Kafka backup completed"
}

# Backup MinIO data
backup_minio() {
    log_info "Backing up MinIO data..."
    
    # Create backup bucket if not exists
    docker exec minio mc alias set local http://localhost:9000 minioadmin Minio@123
    docker exec minio mc mb local/banking-backups --ignore-existing
    
    # Copy data to backup bucket
    docker exec minio mc cp --recursive local/banking-lake/ local/banking-backups/daily_$TIMESTAMP/
    
    # Create backup manifest
    docker exec minio mc ls --recursive local/banking-lake/ \
      > "$BACKUP_DIR/daily/minio_manifest_$TIMESTAMP.txt"
    
    log_success "MinIO backup completed"
}

# Backup PostgreSQL
backup_postgres() {
    log_info "Backing up PostgreSQL..."
    
    # Backup all databases
    docker exec postgres pg_dumpall -U banking > "$BACKUP_DIR/daily/postgres_all_$TIMESTAMP.sql"
    
    # Backup specific database
    docker exec postgres pg_dump -U banking -d banking > "$BACKUP_DIR/daily/postgres_banking_$TIMESTAMP.sql"
    
    # Backup schemas
    for SCHEMA in banking_raw banking_cleansed banking_gold banking_audit banking_security; do
        docker exec postgres pg_dump -U banking -d banking -n "$SCHEMA" \
          > "$BACKUP_DIR/daily/postgres_${SCHEMA}_$TIMESTAMP.sql"
    done
    
    log_success "PostgreSQL backup completed"
}

# Backup Airflow
backup_airflow() {
    log_info "Backing up Airflow..."
    
    # Backup DAGs
    docker cp airflow-webserver:/opt/airflow/dags "$BACKUP_DIR/daily/airflow_dags_$TIMESTAMP"
    
    # Backup configuration
    docker cp airflow-webserver:/opt/airflow/airflow.cfg "$BACKUP_DIR/daily/airflow_config_$TIMESTAMP.cfg"
    
    # Backup plugins
    docker cp airflow-webserver:/opt/airflow/plugins "$BACKUP_DIR/daily/airflow_plugins_$TIMESTAMP"
    
    log_success "Airflow backup completed"
}

# Cleanup old backups
cleanup_old_backups() {
    log_info "Cleaning up backups older than $RETENTION_DAYS days..."
    
    # Remove old daily backups
    find "$BACKUP_DIR/daily" -type f -mtime +$RETENTION_DAYS -delete
    
    # Remove old weekly backups
    find "$BACKUP_DIR/weekly" -type f -mtime +$((RETENTION_DAYS * 7)) -delete
    
    # Remove old monthly backups
    find "$BACKUP_DIR/monthly" -type f -mtime +$((RETENTION_DAYS * 30)) -delete
    
    log_success "Old backups cleaned up"
}

# Create backup summary
create_summary() {
    log_info "Creating backup summary..."
    
    SUMMARY_FILE="$BACKUP_DIR/daily/backup_summary_$TIMESTAMP.txt"
    
    cat > "$SUMMARY_FILE" << EOF
BACKUP SUMMARY
=============
Timestamp: $TIMESTAMP
Component: $COMPONENT
Retention: $RETENTION_DAYS days

Files backed up:
$(find "$BACKUP_DIR/daily" -name "*$TIMESTAMP*" -type f | wc -l) files

Total size:
$(du -sh "$BACKUP_DIR/daily" | cut -f1)

Disk usage:
$(df -h "$BACKUP_DIR" | tail -1 | awk '{print $5}')
EOF
    
    log_success "Backup summary created: $SUMMARY_FILE"
}

# Main execution
main() {
    log_info "Starting backup..."
    log_info "Component: $COMPONENT"
    log_info "Retention: $RETENTION_DAYS days"
    
    create_backup_dir
    
    case $COMPONENT in
        all)
            backup_dremio
            backup_kafka
            backup_minio
            backup_postgres
            backup_airflow
            ;;
        dremio)
            backup_dremio
            ;;
        kafka)
            backup_kafka
            ;;
        minio)
            backup_minio
            ;;
        postgres)
            backup_postgres
            ;;
        airflow)
            backup_airflow
            ;;
        *)
            log_error "Unknown component: $COMPONENT"
            log_info "Available components: all, dremio, kafka, minio, postgres, airflow"
            exit 1
            ;;
    esac
    
    cleanup_old_backups
    create_summary
    
    log_success "Backup completed successfully!"
}

# Run main function
main "$@"
