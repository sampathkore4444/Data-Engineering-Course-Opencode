-- Loans System Seed Data

-- Loan Accounts
INSERT INTO loan_accounts (loan_id, customer_id, loan_type, loan_amount, principal_outstanding, interest_rate, tenure_months, emi_amount, disbursement_date, first_emi_date, maturity_date, loan_status, npa_classification, days_past_due, last_payment_date, property_address, collateral_value, insurance_expiry) VALUES
('LOAN-001', 'CUST-001', 'HOME', 5000000.00, 3200000.00, 8.50, 240, 43391.00, '2020-06-15', '2020-07-01', '2040-06-15', 'ACTIVE', 'STANDARD', 0, '2025-01-05', '12 MG Road, Mumbai 400001', 7500000.00, '2026-06-15'),
('LOAN-002', 'CUST-002', 'HOME', 8000000.00, 5500000.00, 8.25, 300, 58234.00, '2019-03-10', '2019-04-01', '2044-03-10', 'ACTIVE', 'STANDARD', 0, '2025-01-10', '45 Park Street, Kolkata 700016', 12000000.00, '2025-03-10'),
('LOAN-003', 'CUST-003', 'PERSONAL', 1000000.00, 450000.00, 12.00, 60, 22244.00, '2022-01-20', '2022-02-01', '2027-01-20', 'ACTIVE', 'SUB_STANDARD', 45, '2024-08-15', NULL, NULL, '2025-01-20'),
('LOAN-004', 'CUST-004', 'VEHICLE', 1500000.00, 680000.00, 9.00, 84, 23078.00, '2021-09-01', '2021-10-01', '2028-09-01', 'ACTIVE', 'STANDARD', 0, '2025-01-15', NULL, NULL, '2026-09-01'),
('LOAN-005', 'CUST-005', 'HOME', 3000000.00, 2800000.00, 8.75, 240, 32456.00, '2022-08-15', '2022-09-01', '2042-08-15', 'ACTIVE', 'DOUBTFUL', 120, '2024-05-01', '78 Anna Salai, Chennai 600002', 4500000.00, '2025-08-15'),
('LOAN-006', 'CUST-006', 'EDUCATION', 2000000.00, 1500000.00, 10.50, 120, 27078.00, '2020-07-01', '2020-08-01', '2030-07-01', 'ACTIVE', 'STANDARD', 0, '2025-01-20', NULL, NULL, '2030-07-01'),
('LOAN-007', 'CUST-007', 'HOME', 6500000.00, 4200000.00, 8.50, 300, 51234.00, '2019-11-10', '2019-12-01', '2044-11-10', 'ACTIVE', 'STANDARD', 0, '2025-01-08', '23 Banjara Hills, Hyderabad 500034', 9000000.00, '2025-11-10'),
('LOAN-008', 'CUST-008', 'VEHICLE', 2500000.00, 1800000.00, 9.25, 60, 52345.00, '2022-05-15', '2022-06-01', '2027-05-15', 'ACTIVE', 'SUB_STANDARD', 60, '2024-07-01', NULL, NULL, '2025-05-15'),
('LOAN-009', 'CUST-009', 'PERSONAL', 500000.00, 250000.00, 14.00, 36, 17063.00, '2023-06-01', '2023-07-01', '2026-06-01', 'ACTIVE', 'STANDARD', 0, '2025-01-25', NULL, NULL, '2025-06-01'),
('LOAN-010', 'CUST-010', 'HOME', 4000000.00, 0.00, 8.50, 180, 39395.00, '2018-01-15', '2018-02-01', '2033-01-15', 'CLOSED', 'STANDARD', 0, '2023-01-15', '56 Koramangala, Bengaluru 560034', 6000000.00, NULL);

-- Loan Payments
INSERT INTO loan_payments (payment_id, loan_id, payment_date, emi_number, principal_component, interest_component, total_payment, payment_mode, status, late_fee) VALUES
('PAY-001', 'LOAN-001', '2025-01-05', 55, 20391.00, 23000.00, 43391.00, 'EMI_AUTO', 'SUCCESS', 0.00),
('PAY-002', 'LOAN-001', '2025-01-05', 56, 20532.00, 22859.00, 43391.00, 'EMI_AUTO', 'SUCCESS', 0.00),
('PAY-003', 'LOAN-002', '2025-01-10', 70, 21234.00, 37000.00, 58234.00, 'EMI_AUTO', 'SUCCESS', 0.00),
('PAY-004', 'LOAN-003', '2024-08-15', 31, 17244.00, 5000.00, 22244.00, 'NEFT', 'SUCCESS', 0.00),
('PAY-005', 'LOAN-004', '2025-01-15', 41, 17578.00, 5500.00, 23078.00, 'EMI_AUTO', 'SUCCESS', 0.00),
('PAY-006', 'LOAN-005', '2024-05-01', 20, 18456.00, 14000.00, 32456.00, 'CHEQUE', 'SUCCESS', 0.00),
('PAY-007', 'LOAN-006', '2025-01-20', 54, 13578.00, 13500.00, 27078.00, 'EMI_AUTO', 'SUCCESS', 0.00),
('PAY-008', 'LOAN-007', '2025-01-08', 63, 21234.00, 30000.00, 51234.00, 'EMI_AUTO', 'SUCCESS', 0.00),
('PAY-009', 'LOAN-009', '2025-01-25', 31, 14063.00, 3000.00, 17063.00, 'UPI', 'SUCCESS', 0.00),
('PAY-010', 'LOAN-003', '2024-09-15', 32, 17400.00, 4844.00, 22244.00, 'NEFT', 'LATE', 500.00);

-- Loan Overdue
INSERT INTO loan_overdue (overdue_id, loan_id, overdue_date, overdue_amount, overdue_days, collection_agent, collection_status, remarks) VALUES
('OD-001', 'LOAN-003', '2024-09-15', 22244.00, 45, 'Rajesh Kumar', 'CONTACTED', 'Customer facing temporary financial difficulty'),
('OD-002', 'LOAN-005', '2024-06-01', 32456.00, 120, 'Priya Singh', 'RECOVERY_PLAN', 'Property valuation done, restructuring proposal sent'),
('OD-003', 'LOAN-008', '2024-08-01', 52345.00, 60, 'Amit Sharma', 'CONTACTED', 'Customer acknowledged, payment expected this month');