-- Credit Cards System Seed Data

-- Credit Cards
INSERT INTO credit_cards (card_id, customer_id, card_number, card_type, credit_limit, outstanding, card_status, issue_date, expiry_date, billing_cycle, reward_points, annual_fee) VALUES
('CARD-001', 'CUST-001', '4111****1234', 'VISA', 500000.00, 125000.00, 'ACTIVE', '2020-01-15', '2027-01-15', 1, 15000, 1000.00),
('CARD-002', 'CUST-002', '5222****5678', 'MASTERCARD', 750000.00, 280000.00, 'ACTIVE', '2019-06-20', '2026-06-20', 15, 22000, 2500.00),
('CARD-003', 'CUST-003', '3782****9012', 'AMEX', 1000000.00, 450000.00, 'ACTIVE', '2018-03-10', '2025-03-10', 1, 35000, 5000.00),
('CARD-004', 'CUST-004', '4111****3456', 'VISA', 300000.00, 85000.00, 'ACTIVE', '2021-09-05', '2028-09-05', 15, 8000, 500.00),
('CARD-005', 'CUST-005', '5222****7890', 'MASTERCARD', 400000.00, 0.00, 'BLOCKED', '2020-12-01', '2027-12-01', 1, 5000, 1000.00),
('CARD-006', 'CUST-006', '4111****2345', 'VISA', 200000.00, 45000.00, 'ACTIVE', '2022-05-18', '2029-05-18', 1, 3000, 0.00),
('CARD-007', 'CUST-007', '5222****6789', 'MASTERCARD', 600000.00, 180000.00, 'ACTIVE', '2019-11-25', '2026-11-25', 15, 18000, 2000.00),
('CARD-008', 'CUST-008', '3782****0123', 'AMEX', 800000.00, 320000.00, 'ACTIVE', '2020-07-30', '2027-07-30', 1, 25000, 4000.00),
('CARD-009', 'CUST-009', '4111****4567', 'VISA', 150000.00, 25000.00, 'ACTIVE', '2023-02-14', '2030-02-14', 15, 1500, 0.00),
('CARD-010', 'CUST-010', '5222****8901', 'MASTERCARD', 350000.00, 0.00, 'CANCELLED', '2018-08-20', '2025-08-20', 1, 0, 1000.00);

-- Card Transactions
INSERT INTO card_transactions (transaction_id, card_id, transaction_amount, transaction_type, merchant_name, merchant_category, transaction_date, posting_date, description, status) VALUES
('TXN-CARD-001', 'CARD-001', 15000.00, 'PURCHASE', 'Amazon India', 'E-COMMERCE', '2025-01-10 14:30:00', '2025-01-10', 'Online Shopping', 'POSTED'),
('TXN-CARD-002', 'CARD-001', 32000.00, 'PURCHASE', 'Flipkart', 'E-COMMERCE', '2025-01-15 09:15:00', '2025-01-15', 'Electronics Purchase', 'POSTED'),
('TXN-CARD-003', 'CARD-002', 85000.00, 'PURCHASE', 'Taj Hotels', 'TRAVEL', '2025-01-20 18:00:00', '2025-01-20', 'Hotel Booking', 'POSTED'),
('TXN-CARD-004', 'CARD-002', 12000.00, 'PURCHASE', 'Big Bazaar', 'RETAIL', '2025-01-25 12:30:00', '2025-01-25', 'Grocery Shopping', 'POSTED'),
('TXN-CARD-005', 'CARD-003', 250000.00, 'PURCHASE', 'Reliance Digital', 'ELECTRONICS', '2025-02-01 16:45:00', '2025-02-01', 'Laptop Purchase', 'POSTED'),
('TXN-CARD-006', 'CARD-003', 45000.00, 'PURCHASE', 'Swiggy', 'FOOD', '2025-02-05 20:00:00', '2025-02-05', 'Food Orders', 'POSTED'),
('TXN-CARD-007', 'CARD-004', 8000.00, 'PURCHASE', 'Zomato', 'FOOD', '2025-02-10 13:30:00', '2025-02-10', 'Food Orders', 'POSTED'),
('TXN-CARD-008', 'CARD-007', 95000.00, 'PURCHASE', 'Myntra', 'FASHION', '2025-02-15 11:00:00', '2025-02-15', 'Clothing Purchase', 'POSTED'),
('TXN-CARD-009', 'CARD-008', 180000.00, 'PURCHASE', 'Croma', 'ELECTRONICS', '2025-02-20 15:30:00', '2025-02-20', 'TV Purchase', 'POSTED'),
('TXN-CARD-010', 'CARD-001', 25000.00, 'PAYMENT', 'Bank Transfer', 'PAYMENT', '2025-01-28 10:00:00', '2025-01-28', 'Credit Card Payment', 'POSTED'),
('TXN-CARD-011', 'CARD-002', 150000.00, 'PAYMENT', 'NEFT Transfer', 'PAYMENT', '2025-01-30 14:00:00', '2025-01-30', 'Credit Card Payment', 'POSTED'),
('TXN-CARD-012', 'CARD-003', 300000.00, 'PAYMENT', 'Bank Transfer', 'PAYMENT', '2025-02-08 09:30:00', '2025-02-08', 'Credit Card Payment', 'POSTED');

-- Card Billing
INSERT INTO card_billing (billing_id, card_id, billing_month, opening_balance, total_purchases, total_payments, total_fees, total_interest, closing_balance, minimum_payment, due_date, payment_status) VALUES
('BILL-001', 'CARD-001', '2025-01-01', 98000.00, 47000.00, 25000.00, 150.00, 1375.00, 121525.00, 3645.75, '2025-01-28', 'PAID'),
('BILL-002', 'CARD-002', '2025-01-01', 217000.00, 97000.00, 150000.00, 250.00, 3200.00, 167450.00, 5023.50, '2025-01-30', 'PAID'),
('BILL-003', 'CARD-003', '2025-02-01', 200000.00, 295000.00, 300000.00, 500.00, 2750.00, 198250.00, 5947.50, '2025-02-08', 'PAID'),
('BILL-004', 'CARD-004', '2025-02-01', 77000.00, 8000.00, 0.00, 50.00, 1155.00, 86205.00, 2586.15, '2025-02-15', 'UNPAID'),
('BILL-005', 'CARD-007', '2025-02-01', 85000.00, 95000.00, 0.00, 200.00, 2750.00, 182950.00, 5488.50, '2025-02-28', 'UNPAID');