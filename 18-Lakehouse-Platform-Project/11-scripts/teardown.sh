#!/bin/bash
# =============================================================================
# TEARDOWN SCRIPT - Banking Data Platform
# =============================================================================
# Purpose: Stop and clean up the banking data platform
# Usage: ./teardown.sh [environment] [--keep-data]
# =============================================================================

set -e

ENVIRONMENT=${1:-dev}
KEEP_DATA=${2:-"--keep-data"}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

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

# Confirmation prompt
confirm_teardown() {
    log_warning "This will stop all services and potentially delete data!"
    log_warning "Environment: $ENVIRONMENT"
    log_warning "Keep data: $KEEP_DATA"
    echo ""
    read -p "Are you sure you want to continue? (yes/no): " confirm
    
    if [[ "$confirm" != "yes" ]]; then
        log_info "Teardown cancelled"
        exit 0
    fi
}

# Stop services in reverse order
stop_services() {
    log_info "Stopping services..."
    
    cd "$PROJECT_DIR"
    
    # Stop monitoring
    log_info "Stopping monitoring stack..."
    docker-compose -f docker-compose.yml down prometheus grafana
    
    # Stop Airflow
    log_info "Stopping Airflow..."
    docker-compose -f docker-compose.yml down airflow-webserver airflow-scheduler airflow-worker
    
    # Stop Dremio
    log_info "Stopping Dremio..."
    docker-compose -f docker-compose.yml down dremio-master dremio-executor-1 dremio-executor-2
    
    # Stop Kafka
    log_info "Stopping Kafka cluster..."
    docker-compose -f docker-compose.yml down kafka zookeeper
    
    # Stop base services
    log_info "Stopping base services..."
    docker-compose -f docker-compose.yml down minio postgres redis
    
    log_success "All services stopped"
}

# Clean up containers
cleanup_containers() {
    log_info "Cleaning up containers..."
    
    # Remove stopped containers
    docker container prune -f
    
    # Remove unused networks
    docker network prune -f
    
    log_success "Containers cleaned up"
}

# Clean up volumes (optional)
cleanup_volumes() {
    if [[ "$KEEP_DATA" != "--keep-data" ]]; then
        log_warning "Removing all data volumes..."
        
        # Remove project volumes
        docker volume ls | grep banking-data-platform | awk '{print $2}' | xargs -r docker volume rm
        
        # Remove Docker Compose volumes
        cd "$PROJECT_DIR"
        docker-compose -f docker-compose.yml down -v
        
        log_success "Data volumes removed"
    else
        log_info "Keeping data volumes (--keep-data flag set)"
    fi
}

# Clean up images (optional)
cleanup_images() {
    if [[ "$KEEP_DATA" != "--keep-data" ]]; then
        log_warning "Removing Docker images..."
        
        # Remove project images
        docker images | grep banking-data-platform | awk '{print $3}' | xargs -r docker rmi -f
        
        log_success "Docker images removed"
    else
        log_info "Keeping Docker images (--keep-data flag set)"
    fi
}

# Clean up local files (optional)
cleanup_files() {
    if [[ "$KEEP_DATA" != "--keep-data" ]]; then
        log_warning "Removing local data files..."
        
        # Remove data directories
        rm -rf "$PROJECT_DIR/data"
        rm -rf "$PROJECT_DIR/logs"
        rm -rf "$PROJECT_DIR/backups"
        
        log_success "Local files removed"
    else
        log_info "Keeping local files (--keep-data flag set)"
    fi
}

# Print summary
print_summary() {
    log_success "=================================================="
    log_success "BANKING DATA PLATFORM TEARDOWN COMPLETE"
    log_success "=================================================="
    log_success ""
    log_success "Environment: $ENVIRONMENT"
    log_success "Keep data: $KEEP_DATA"
    log_success ""
    log_success "Actions performed:"
    log_success "  - All services stopped"
    log_success "  - Containers cleaned up"
    
    if [[ "$KEEP_DATA" != "--keep-data" ]]; then
        log_success "  - Data volumes removed"
        log_success "  - Docker images removed"
        log_success "  - Local files removed"
    else
        log_info "  - Data preserved (--keep-data flag)"
    fi
    
    log_success ""
    log_success "To restart: ./setup.sh $ENVIRONMENT"
    log_success "=================================================="
}

# Main execution
main() {
    log_info "Starting Banking Data Platform teardown..."
    
    confirm_teardown
    stop_services
    cleanup_containers
    cleanup_volumes
    cleanup_images
    cleanup_files
    print_summary
}

# Run main function
main "$@"
