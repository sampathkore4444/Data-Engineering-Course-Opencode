#!/bin/bash
# =============================================================================
# Debezium CDC Setup Script
# Purpose: Set up Debezium connectors for all source databases
# =============================================================================

set -e

echo "=========================================="
echo "Setting up Debezium CDC Connectors"
echo "=========================================="

# Configuration
CONNECT_URL="http://localhost:8083"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CDC_DIR="$(dirname "$SCRIPT_DIR")"

# Function to check if Kafka Connect is ready
check_connect() {
    echo "Checking if Kafka Connect is ready..."
    for i in {1..30}; do
        if curl -s "$CONNECT_URL/connectors" > /dev/null 2>&1; then
            echo "✅ Kafka Connect is ready!"
            return 0
        fi
        echo "Waiting for Kafka Connect... ($i/30)"
        sleep 2
    done
    echo "❌ Kafka Connect is not ready after 60 seconds"
    return 1
}

# Function to create connector
create_connector() {
    local connector_file=$1
    local connector_name=$(basename "$connector_file" .json)
    
    echo "Creating connector: $connector_name"
    
    # Check if connector already exists
    if curl -s "$CONNECT_URL/connectors/$connector_name" > /dev/null 2>&1; then
        echo "⚠️  Connector $connector_name already exists, updating..."
        curl -X PUT \
            -H "Content-Type: application/json" \
            -d @"$connector_file" \
            "$CONNECT_URL/connectors/$connector_name/config"
    else
        echo "Creating new connector: $connector_name"
        curl -X POST \
            -H "Content-Type: application/json" \
            -d @"$connector_file" \
            "$CONNECT_URL/connectors"
    fi
    
    if [ $? -eq 0 ]; then
        echo "✅ Connector $connector_name created/updated successfully"
    else
        echo "❌ Failed to create connector $connector_name"
        return 1
    fi
}

# Function to check connector status
check_connector_status() {
    local connector_name=$1
    
    echo "Checking status of connector: $connector_name"
    STATUS=$(curl -s "$CONNECT_URL/connectors/$connector_name/status")
    echo "$STATUS" | python -m json.tool 2>/dev/null || echo "$STATUS"
}

# Main execution
main() {
    echo ""
    echo "Step 1: Checking Kafka Connect..."
    check_connect
    
    echo ""
    echo "Step 2: Creating Core Banking Connector..."
    create_connector "$CDC_DIR/debezium/core-banking-connector.json"
    
    echo ""
    echo "Step 3: Creating Cards System Connector..."
    create_connector "$CDC_DIR/debezium/cards-connector.json"
    
    echo ""
    echo "Step 4: Creating Loans System Connector..."
    create_connector "$CDC_DIR/debezium/loans-connector.json"
    
    echo ""
    echo "Step 5: Checking Connector Status..."
    check_connector_status "core-banking-connector"
    check_connector_status "cards-system-connector"
    check_connector_status "loans-system-connector"
    
    echo ""
    echo "=========================================="
    echo "✅ Debezium CDC Setup Complete!"
    echo "=========================================="
    echo ""
    echo "Kafka Topics Created:"
    echo "  - cdc.customers"
    echo "  - cdc.accounts"
    echo "  - cdc.transactions"
    echo "  - cdc.cards"
    echo "  - cdc.card_transactions"
    echo "  - cdc.loans"
    echo "  - cdc.loan_payments"
    echo "  - schema-changes.*"
    echo ""
    echo "Monitor connectors at: $CONNECT_URL"
    echo ""
}

# Run main function
main "$@"
