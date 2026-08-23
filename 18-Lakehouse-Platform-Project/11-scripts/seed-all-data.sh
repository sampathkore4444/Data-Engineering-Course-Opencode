#!/bin/bash
# =============================================================================
# SEED ALL DATA SCRIPT - Banking Data Platform
# =============================================================================
# Purpose: Load all sample banking data into the platform
# Usage: ./seed-all-data.sh
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Wait for services
wait_for_services() {
    log_info "Waiting for services to be ready..."
    
    # Wait for PostgreSQL
    until docker exec postgres pg_isready -U banking; do
        log_info "Waiting for PostgreSQL..."
        sleep 2
    done
    
    # Wait for MinIO
    until curl -s http://localhost:9000/minio/health/live > /dev/null; do
        log_info "Waiting for MinIO..."
        sleep 2
    done
    
    # Wait for Kafka
    until docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
        log_info "Waiting for Kafka..."
        sleep 2
    done
    
    log_success "All services are ready"
}

# Load core banking data
load_core_banking() {
    log_info "Loading core banking data..."
    
    # Load customers
    docker exec postgres psql -U banking -d banking -c "
      INSERT INTO banking_raw.customers (customer_id, customer_name, dob, gender, nationality, pan_number, email, phone, address_line1, city, state, pin_code, created_date, last_updated)
      VALUES
        ('CUST-001', 'Nguyen Van A', '1985-01-15', 'MALE', 'VIETNAMESE', 'PAN001', 'nguyena@email.com', '0901234567', '123 Le Loi', 'Ho Chi Minh', 'HCM', '700000', '2020-01-01', CURRENT_TIMESTAMP),
        ('CUST-002', 'Tran Thi B', '1990-05-20', 'FEMALE', 'VIETNAMESE', 'PAN002', 'tranb@email.com', '0912345678', '456 Nguyen Hue', 'Ha Noi', 'HN', '100000', '2020-02-15', CURRENT_TIMESTAMP),
        ('CUST-003', 'Le Minh C', '1988-03-10', 'MALE', 'VIETNAMESE', 'PAN003', 'lec@email.com', '0923456789', '789 Tran Hung Dao', 'Da Nang', 'DN', '550000', '2020-03-20', CURRENT_TIMESTAMP),
        ('CUST-004', 'Pham Hong D', '1992-07-25', 'FEMALE', 'VIETNAMESE', 'PAN004', 'phamd@email.com', '0934567890', '321 Hai Ba Trung', 'Can Tho', 'CT', '900000', '2020-04-10', CURRENT_TIMESTAMP),
        ('CUST-005', 'Hoang Van E', '1987-11-30', 'MALE', 'VIETNAMESE', 'PAN005', 'hoangne@email.com', '0945678901', '654 Ly Thuong Kiet', 'Hai Phong', 'HP', '610000', '2020-05-05', CURRENT_TIMESTAMP);
    "
    
    # Load accounts
    docker exec postgres psql -U banking -d banking -c "
      INSERT INTO banking_raw.accounts (account_id, customer_id, account_type, currency, opening_date, current_balance, available_balance, status, branch_code, last_updated)
      VALUES
        ('ACC-001', 'CUST-001', 'SAVINGS', 'VND', '2020-01-01', 150000000, 145000000, 'ACTIVE', 'BR001', CURRENT_TIMESTAMP),
        ('ACC-002', 'CUST-001', 'CURRENT', 'VND', '2020-01-01', 25000000, 25000000, 'ACTIVE', 'BR001', CURRENT_TIMESTAMP),
        ('ACC-003', 'CUST-002', 'SAVINGS', 'VND', '2020-02-15', 250000000, 245000000, 'ACTIVE', 'BR002', CURRENT_TIMESTAMP),
        ('ACC-004', 'CUST-003', 'SAVINGS', 'VND', '2020-03-20', 75000000, 70000000, 'ACTIVE', 'BR003', CURRENT_TIMESTAMP),
        ('ACC-005', 'CUST-004', 'CURRENT', 'VND', '2020-04-10', 50000000, 50000000, 'ACTIVE', 'BR004', CURRENT_TIMESTAMP),
        ('ACC-006', 'CUST-005', 'SAVINGS', 'VND', '2020-05-05', 100000000, 95000000, 'ACTIVE', 'BR005', CURRENT_TIMESTAMP);
    "
    
    # Load transactions
    docker exec postgres psql -U banking -d banking -c "
      INSERT INTO banking_raw.transactions (txn_id, account_id, txn_type, amount, currency, txn_date, txn_timestamp, description, reference, channel, status)
      VALUES
        (1, 'ACC-001', 'CREDIT', 50000000, 'VND', '2024-01-15', '2024-01-15 10:30:00', 'Salary', 'REF001', 'BANK_TRANSFER', 'SUCCESS'),
        (2, 'ACC-001', 'DEBIT', 2500000, 'VND', '2024-01-15', '2024-01-15 14:20:00', 'Bill Payment', 'REF002', 'MOBILE', 'SUCCESS'),
        (3, 'ACC-002', 'CREDIT', 10000000, 'VND', '2024-01-15', '2024-01-15 09:15:00', 'Transfer', 'REF003', 'ONLINE', 'SUCCESS'),
        (4, 'ACC-003', 'DEBIT', 5000000, 'VND', '2024-01-15', '2024-01-15 11:45:00', 'ATM Withdrawal', 'REF004', 'ATM', 'SUCCESS'),
        (5, 'ACC-004', 'CREDIT', 15000000, 'VND', '2024-01-15', '2024-01-15 16:30:00', 'Deposit', 'REF005', 'BRANCH', 'SUCCESS'),
        (6, 'ACC-005', 'DEBIT', 3000000, 'VND', '2024-01-15', '2024-01-15 13:10:00', 'Payment', 'REF006', 'MOBILE', 'SUCCESS'),
        (7, 'ACC-006', 'CREDIT', 20000000, 'VND', '2024-01-15', '2024-01-15 15:25:00', 'Transfer', 'REF007', 'ONLINE', 'SUCCESS');
    "
    
    log_success "Core banking data loaded"
}

# Load credit card data
load_credit_cards() {
    log_info "Loading credit card data..."
    
    docker exec postgres psql -U banking -d banking -c "
      INSERT INTO banking_raw.cards (card_number, customer_id, card_type, card_limit, credit_used, issuance_date, expiry_date, status, last_updated)
      VALUES
        ('4532015112830366', 'CUST-001', 'VISA', 50000000, 30000000, '2022-01-01', '2027-01-01', 'ACTIVE', CURRENT_TIMESTAMP),
        ('5425233430109903', 'CUST-001', 'MASTERCARD', 30000000, 7500000, '2023-06-15', '2028-06-15', 'ACTIVE', CURRENT_TIMESTAMP),
        ('4916338506082832', 'CUST-002', 'VISA', 80000000, 45000000, '2021-03-10', '2026-03-10', 'ACTIVE', CURRENT_TIMESTAMP),
        ('5123456789012346', 'CUST-003', 'MASTERCARD', 40000000, 20000000, '2022-09-20', '2027-09-20', 'ACTIVE', CURRENT_TIMESTAMP),
        ('4024007103939509', 'CUST-004', 'VISA', 60000000, 35000000, '2023-01-05', '2028-01-05', 'ACTIVE', CURRENT_TIMESTAMP),
        ('5555666677778884', 'CUST-005', 'MASTERCARD', 25000000, 12500000, '2022-05-15', '2027-05-15', 'ACTIVE', CURRENT_TIMESTAMP);
    "
    
    docker exec postgres psql -U banking -d banking -c "
      INSERT INTO banking_raw.card_transactions (txn_id, card_number, merchant_id, merchant_name, merchant_category, amount, currency, txn_date, txn_timestamp, txn_type, status)
      VALUES
        (101, '4532015112830366', 'M001', 'Coffee Shop', 'FOOD_AND_DRINK', 150000, 'VND', '2024-01-15', '2024-01-15 08:30:00', 'PURCHASE', 'SUCCESS'),
        (102, '4532015112830366', 'M002', 'Electronics Store', 'ELECTRONICS', 5000000, 'VND', '2024-01-15', '2024-01-15 14:15:00', 'PURCHASE', 'SUCCESS'),
        (103, '5425233430109903', 'M003', 'Restaurant', 'FOOD_AND_DRINK', 500000, 'VND', '2024-01-15', '2024-01-15 19:45:00', 'PURCHASE', 'SUCCESS'),
        (104, '4916338506082832', 'M004', 'Online Shopping', 'ONLINE', 2500000, 'VND', '2024-01-15', '2024-01-15 11:20:00', 'PURCHASE', 'SUCCESS'),
        (105, '5123456789012346', 'M005', 'Gas Station', 'TRANSPORTATION', 800000, 'VND', '2024-01-15', '2024-01-15 16:30:00', 'PURCHASE', 'SUCCESS'),
        (106, '4024007103939509', 'M006', 'Supermarket', 'GROCERY', 1200000, 'VND', '2024-01-15', '2024-01-15 10:00:00', 'PURCHASE', 'SUCCESS'),
        (107, '5555666677778884', 'M007', 'Clothing Store', 'SHOPPING', 3000000, 'VND', '2024-01-15', '2024-01-15 15:45:00', 'PURCHASE', 'SUCCESS');
    "
    
    log_success "Credit card data loaded"
}

# Load loan data
load_loans() {
    log_info "Loading loan data..."
    
    docker exec postgres psql -U banking -d banking -c "
      INSERT INTO banking_raw.loan_accounts (loan_id, customer_id, loan_type, principal_amount, principal_outstanding, interest_rate, tenure_months, emi_amount, disbursement_date, maturity_date, last_updated)
      VALUES
        ('HL-001', 'CUST-001', 'HOME_LOAN', 2000000000, 1500000000, 9.5, 240, 16800000, '2020-01-01', '2040-01-01', CURRENT_TIMESTAMP),
        ('PL-001', 'CUST-002', 'PERSONAL_LOAN', 100000000, 50000000, 12.0, 60, 2224000, '2022-06-01', '2027-06-01', CURRENT_TIMESTAMP),
        ('CL-001', 'CUST-003', 'CAR_LOAN', 500000000, 300000000, 8.5, 84, 6500000, '2021-03-01', '2028-03-01', CURRENT_TIMESTAMP),
        ('BL-001', 'CUST-004', 'BUSINESS_LOAN', 1000000000, 800000000, 10.0, 120, 13200000, '2020-09-01', '2030-09-01', CURRENT_TIMESTAMP),
        ('PL-002', 'CUST-005', 'PERSONAL_LOAN', 200000000, 150000000, 11.5, 48, 5000000, '2023-01-01', '2027-01-01', CURRENT_TIMESTAMP);
    "
    
    docker exec postgres psql -U banking -d banking -c "
      INSERT INTO banking_raw.loan_payments (payment_id, loan_id, payment_date, amount, payment_mode, status, reference_number)
      VALUES
        (1001, 'HL-001', '2024-01-05', 16800000, 'AUTO_DEBIT', 'SUCCESS', 'PAY-001'),
        (1002, 'HL-001', '2024-01-05', 16800000, 'AUTO_DEBIT', 'SUCCESS', 'PAY-002'),
        (1003, 'PL-001', '2024-01-10', 2224000, 'BANK_TRANSFER', 'SUCCESS', 'PAY-003'),
        (1004, 'CL-001', '2024-01-15', 6500000, 'AUTO_DEBIT', 'SUCCESS', 'PAY-004'),
        (1005, 'BL-001', '2024-01-20', 13200000, 'BANK_TRANSFER', 'SUCCESS', 'PAY-005'),
        (1006, 'PL-002', '2024-01-25', 5000000, 'AUTO_DEBIT', 'SUCCESS', 'PAY-006');
    "
    
    log_success "Loan data loaded"
}

# Create Dremio spaces and sources
init_dremio_spaces() {
    log_info "Initializing Dremio spaces..."
    
    # Get auth token
    TOKEN=$(curl -s -X POST 'http://localhost:9047/apiv2/login' \
      -H 'Content-Type: application/json' \
      -d '{"userName":"admin","password":"Admin@123"}' | jq -r '.token')
    
    # Create spaces
    curl -X POST "http://localhost:9047/api/v3/catalog" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d '{"entityType":"space","path":["banking-raw"],"tag":"raw"}'
    
    curl -X POST "http://localhost:9047/api/v3/catalog" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d '{"entityType":"space","path":["banking-cleansed"],"tag":"cleansed"}'
    
    curl -X POST "http://localhost:9047/api/v3/catalog" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d '{"entityType":"space","path":["banking-gold"],"tag":"gold"}'
    
    log_success "Dremio spaces created"
}

# Main execution
main() {
    log_info "Starting data seeding..."
    
    wait_for_services
    load_core_banking
    load_credit_cards
    load_loans
    init_dremio_spaces
    
    log_success "=================================================="
    log_success "ALL SAMPLE DATA LOADED SUCCESSFULLY"
    log_success "=================================================="
    log_success ""
    log_success "Data loaded:"
    log_success "  - 5 customers"
    log_success "  - 6 accounts"
    log_success "  - 7 transactions"
    log_success "  - 6 credit cards"
    log_success "  - 7 card transactions"
    log_success "  - 5 loans"
    log_success "  - 6 loan payments"
    log_success ""
    log_success "Next steps:"
    log_success "  1. Open Dremio: http://localhost:9047"
    log_success "  2. Create source connection to MinIO"
    log_success "  3. Run sample queries from 03-dremio-sql/"
    log_success "=================================================="
}

# Run main function
main "$@"
