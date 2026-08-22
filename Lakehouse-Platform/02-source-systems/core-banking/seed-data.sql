--##############################################################################
-- Core Banking System - Seed Data
-- Realistic Vietnamese Banking Data
--##############################################################################

--##############################################################################
-- BRANCHES
--##############################################################################
INSERT INTO branches (branch_code, branch_name, branch_type, city, state, region) VALUES
('BR001', 'Ho Chi Minh Main Branch', 'MAIN', 'Ho Chi Minh City', 'Ho Chi Minh', 'South'),
('BR002', 'Hanoi Central Branch', 'MAIN', 'Hanoi', 'Hanoi', 'North'),
('BR003', 'Da Nang Branch', 'SUB', 'Da Nang', 'Da Nang', 'Central'),
('BR004', 'Can Tho Branch', 'SUB', 'Can Tho', 'Can Tho', 'Mekong Delta'),
('BR005', 'Hai Phong Branch', 'SUB', 'Hai Phong', 'Hai Phong', 'North'),
('BR006', 'Bien Hoa Branch', 'EXTENSION', 'Bien Hoa', 'Dong Nai', 'South'),
('BR007', 'Vung Tau Branch', 'EXTENSION', 'Vung Tau', 'Ba Ria-Vung Tau', 'South'),
('BR008', 'Nha Trang Branch', 'SUB', 'Nha Trang', 'Khanh Hoa', 'Central'),
('BR009', 'Hue Branch', 'SUB', 'Hue', 'Thua Thien-Hue', 'Central'),
('BR010', 'Digital Banking Center', 'DIGITAL', 'Ho Chi Minh City', 'Ho Chi Minh', 'South');

--##############################################################################
-- CUSTOMERS (Individual)
--##############################################################################
INSERT INTO customers (customer_id, customer_number, customer_type, first_name, last_name, 
                       date_of_birth, national_id, phone_number, email, city, risk_rating, kyc_status) VALUES
-- High-value individual customers
('CUST0000000001', '100001', 'INDIVIDUAL', 'Nguyen', 'Van Minh', '1975-03-15', '001234567890', '+84901234567', 'minh.nv@email.com', 'Ho Chi Minh City', 'LOW', 'VERIFIED'),
('CUST0000000002', '100002', 'INDIVIDUAL', 'Tran', 'Thi Lan', '1982-07-22', '001234567891', '+84912345678', 'lan.tt@email.com', 'Hanoi', 'LOW', 'VERIFIED'),
('CUST0000000003', '100003', 'INDIVIDUAL', 'Le', 'Hoang Nam', '1988-11-08', '001234567892', '+84923456789', 'nam.lh@email.com', 'Da Nang', 'MEDIUM', 'VERIFIED'),
('CUST0000000004', '100004', 'INDIVIDUAL', 'Pham', 'Minh Chau', '1990-02-14', '001234567893', '+84934567890', 'chau.pm@email.com', 'Ho Chi Minh City', 'LOW', 'VERIFIED'),
('CUST0000000005', '100005', 'INDIVIDUAL', 'Hoang', 'Viet Dung', '1985-09-30', '001234567894', '+84945678901', 'dung.hv@email.com', 'Hai Phong', 'HIGH', 'VERIFIED'),
('CUST0000000006', '100006', 'INDIVIDUAL', 'Vu', 'Thi Mai', '1992-04-18', '001234567895', '+84956789012', 'mai.vt@email.com', 'Can Tho', 'MEDIUM', 'VERIFIED'),
('CUST0000000007', '100007', 'INDIVIDUAL', 'Dang', 'Quoc Bao', '1978-12-05', '001234567896', '+84967890123', 'bao.dq@email.com', 'Ho Chi Minh City', 'LOW', 'VERIFIED'),
('CUST0000000008', '100008', 'INDIVIDUAL', 'Bui', 'Thanh Ha', '1995-06-20', '001234567897', '+84978901234', 'ha.bt@email.com', 'Hanoi', 'LOW', 'VERIFIED'),
('CUST0000000009', '100009', 'INDIVIDUAL', 'Do', 'Anh Tuan', '1980-08-12', '001234567898', '+84989012345', 'tuan.da@email.com', 'Da Nang', 'VERY_HIGH', 'VERIFIED'),
('CUST0000000010', '100010', 'INDIVIDUAL', 'Ngo', 'Phuong Linh', '1993-01-28', '001234567899', '+84990123456', 'linh.np@email.com', 'Ho Chi Minh City', 'MEDIUM', 'VERIFIED'),
-- Medium-value customers
('CUST0000000011', '100011', 'INDIVIDUAL', 'Truong', 'Van Khoa', '1987-05-10', '001234567900', '+84901111111', 'khoa.tv@email.com', 'Bien Hoa', 'LOW', 'VERIFIED'),
('CUST0000000012', '100012', 'INDIVIDUAL', 'Ly', 'Thi Ngoc', '1991-10-25', '001234567901', '+84911111111', 'ngoc.ly@email.com', 'Vung Tau', 'MEDIUM', 'VERIFIED'),
('CUST0000000013', '100013', 'INDIVIDUAL', 'Mai', 'Quoc Dat', '1979-03-08', '001234567902', '+84922222222', 'dat.mq@email.com', 'Nha Trang', 'LOW', 'VERIFIED'),
('CUST0000000014', '100014', 'INDIVIDUAL', 'La', 'Minh Tri', '1994-07-15', '001234567903', '+84933333333', 'tri.lm@email.com', 'Hue', 'LOW', 'VERIFIED'),
('CUST0000000015', '100015', 'INDIVIDUAL', 'Cam', 'Thi Huong', '1986-11-22', '001234567904', '+84944444444', 'huong.ct@email.com', 'Hanoi', 'MEDIUM', 'VERIFIED');

--##############################################################################
-- CORPORATE CUSTOMERS
--##############################################################################
INSERT INTO customers (customer_id, customer_number, customer_type, company_name, 
                       phone_number, email, city, risk_rating, kyc_status) VALUES
('CUST0000000020', '200001', 'CORPORATE', 'Vietnam Technology Corporation', '+842812345678', 'contact@vntechcorp.vn', 'Ho Chi Minh City', 'LOW', 'VERIFIED'),
('CUST0000000021', '200002', 'CORPORATE', 'Saigon Import Export Ltd', '+842823456789', 'info@saigonimport.vn', 'Ho Chi Minh City', 'MEDIUM', 'VERIFIED'),
('CUST0000000022', '200003', 'CORPORATE', 'Hanoi Manufacturing Co', '+842434567890', 'sales@hanoimfg.vn', 'Hanoi', 'LOW', 'VERIFIED'),
('CUST0000000023', '200004', 'CORPORATE', 'Da Nang Tourism Group', '+842364567891', 'info@dntourism.vn', 'Da Nang', 'HIGH', 'VERIFIED'),
('CUST0000000024', '200005', 'CORPORATE', 'Mekong Agriculture Ltd', '+842925678901', 'contact@mekongagri.vn', 'Can Tho', 'MEDIUM', 'VERIFIED');

--##############################################################################
-- SME CUSTOMERS
--##############################################################################
INSERT INTO customers (customer_id, customer_number, customer_type, company_name,
                       phone_number, email, city, risk_rating, kyc_status) VALUES
('CUST0000000030', '300001', 'SME', 'Quick Mart Convenience Stores', '+842866778899', 'ops@quickmart.vn', 'Ho Chi Minh City', 'LOW', 'VERIFIED'),
('CUST0000000031', '300002', 'SME', 'Dragon Coffee Chain', '+842877889900', 'admin@dragoncoffee.vn', 'Ho Chi Minh City', 'MEDIUM', 'VERIFIED'),
('CUST0000000032', '300003', 'SME', 'Blue Star Electronics', '+842488990011', 'sales@bluestar.vn', 'Hanoi', 'LOW', 'VERIFIED'),
('CUST0000000033', '300004', 'SME', 'Red River Construction', '+842369900112', 'info@redriver.vn', 'Da Nang', 'HIGH', 'VERIFIED');

--##############################################################################
-- ACCOUNTS
--##############################################################################
INSERT INTO accounts (account_id, account_number, customer_id, account_type, account_status, 
                      balance, available_balance, interest_rate, opened_date, branch_code) VALUES
-- Savings accounts for individual customers
('ACCT0000000001', '1234567890', 'CUST0000000001', 'SAVINGS', 'ACTIVE', 850000000.00, 850000000.00, 0.0350, '2015-01-15', 'BR001'),
('ACCT0000000002', '1234567891', 'CUST0000000001', 'CURRENT', 'ACTIVE', 1250000000.00, 1250000000.00, 0.0050, '2015-01-15', 'BR001'),
('ACCT0000000003', '1234567892', 'CUST0000000002', 'SAVINGS', 'ACTIVE', 650000000.00, 650000000.00, 0.0350, '2018-06-20', 'BR002'),
('ACCT0000000004', '1234567893', 'CUST0000000003', 'SAVINGS', 'ACTIVE', 420000000.00, 420000000.00, 0.0350, '2019-03-10', 'BR003'),
('ACCT0000000005', '1234567894', 'CUST0000000004', 'CURRENT', 'ACTIVE', 780000000.00, 780000000.00, 0.0050, '2020-01-05', 'BR001'),
('ACCT0000000006', '1234567895', 'CUST0000000005', 'SAVINGS', 'ACTIVE', 95000000.00, 95000000.00, 0.0350, '2021-08-15', 'BR005'),
('ACCT0000000007', '1234567896', 'CUST0000000006', 'SAVINGS', 'ACTIVE', 180000000.00, 180000000.00, 0.0350, '2019-11-20', 'BR004'),
('ACCT0000000008', '1234567897', 'CUST0000000007', 'CURRENT', 'ACTIVE', 2100000000.00, 2100000000.00, 0.0050, '2010-05-10', 'BR001'),
('ACCT0000000009', '1234567898', 'CUST0000000008', 'SAVINGS', 'ACTIVE', 320000000.00, 320000000.00, 0.0350, '2022-02-14', 'BR002'),
('ACCT0000000010', '1234567899', 'CUST0000000009', 'SAVINGS', 'ACTIVE', 75000000.00, 75000000.00, 0.0350, '2020-09-30', 'BR003'),
('ACCT0000000011', '1234567900', 'CUST0000000010', 'SAVINGS', 'ACTIVE', 520000000.00, 520000000.00, 0.0350, '2021-04-18', 'BR001'),
-- Corporate accounts
('ACCT0000000020', '2234567890', 'CUST0000000020', 'CURRENT', 'ACTIVE', 15000000000.00, 15000000000.00, 0.0050, '2012-03-20', 'BR001'),
('ACCT0000000021', '2234567891', 'CUST0000000021', 'CURRENT', 'ACTIVE', 8500000000.00, 8500000000.00, 0.0050, '2015-07-15', 'BR001'),
('ACCT0000000022', '2234567892', 'CUST0000000022', 'CURRENT', 'ACTIVE', 12000000000.00, 12000000000.00, 0.0050, '2010-01-10', 'BR002'),
('ACCT0000000023', '2234567893', 'CUST0000000023', 'CURRENT', 'ACTIVE', 4500000000.00, 4500000000.00, 0.0050, '2018-11-05', 'BR003'),
('ACCT0000000024', '2234567894', 'CUST0000000024', 'CURRENT', 'ACTIVE', 6200000000.00, 6200000000.00, 0.0050, '2016-05-22', 'BR004'),
-- SME accounts
('ACCT0000000030', '3234567890', 'CUST0000000030', 'CURRENT', 'ACTIVE', 2500000000.00, 2500000000.00, 0.0050, '2019-08-12', 'BR001'),
('ACCT0000000031', '3234567891', 'CUST0000000031', 'CURRENT', 'ACTIVE', 1800000000.00, 1800000000.00, 0.0050, '2020-03-25', 'BR001'),
('ACCT0000000032', '3234567892', 'CUST0000000032', 'CURRENT', 'ACTIVE', 3200000000.00, 3200000000.00, 0.0050, '2017-12-08', 'BR002'),
('ACCT0000000033', '3234567893', 'CUST0000000033', 'CURRENT', 'ACTIVE', 950000000.00, 950000000.00, 0.0050, '2021-06-15', 'BR003');

--##############################################################################
-- TRANSACTIONS (Recent 30 days)
--##############################################################################
INSERT INTO transactions (transaction_id, account_id, transaction_type, amount, balance_after, 
                          description, channel, status, transaction_time) VALUES
-- High-value customer transactions
('TXN20240115000001', 'ACCT0000000001', 'CREDIT', 250000000.00, 850000000.00, 'Salary Credit', 'WIRE', 'COMPLETED', '2024-01-15 09:30:00'),
('TXN20240115000002', 'ACCT0000000001', 'DEBIT', 15000000.00, 835000000.00, 'ATM Withdrawal', 'ATM', 'COMPLETED', '2024-01-15 14:22:00'),
('TXN20240116000001', 'ACCT0000000001', 'DEBIT', 45000000.00, 790000000.00, 'Fund Transfer to Savings', 'MOBILE', 'COMPLETED', '2024-01-16 10:15:00'),
('TXN20240117000001', 'ACCT0000000001', 'CREDIT', 180000000.00, 970000000.00, 'Investment Maturity', 'WIRE', 'COMPLETED', '2024-01-17 11:00:00'),
('TXN20240118000001', 'ACCT0000000001', 'DEBIT', 25000000.00, 945000000.00, 'Insurance Premium', 'INTERNET', 'COMPLETED', '2024-01-18 16:45:00'),
('TXN20240119000001', 'ACCT0000000001', 'DEBIT', 120000000.00, 825000000.00, 'Property Down Payment', 'WIRE', 'COMPLETED', '2024-01-19 09:00:00'),
('TXN20240120000001', 'ACCT0000000001', 'CREDIT', 250000000.00, 1075000000.00, 'Salary Credit', 'WIRE', 'COMPLETED', '2024-01-20 09:30:00'),
('TXN20240121000001', 'ACCT0000000001', 'DEBIT', 35000000.00, 1040000000.00, 'Credit Card Payment', 'MOBILE', 'COMPLETED', '2024-01-21 13:20:00'),
('TXN20240122000001', 'ACCT0000000001', 'DEBIT', 8000000.00, 1032000000.00, 'Utility Bill', 'INTERNET', 'COMPLETED', '2024-01-22 10:30:00'),
('TXN20240123000001', 'ACCT0000000001', 'CREDIT', 50000000.00, 1082000000.00, 'Dividend Credit', 'WIRE', 'COMPLETED', '2024-01-23 14:00:00'),

-- Multiple transactions for fraud detection scenario
('TXN20240124000001', 'ACCT0000000006', 'DEBIT', 5000000.00, 90000000.00, 'ATM Withdrawal - HCM', 'ATM', 'COMPLETED', '2024-01-24 08:00:00'),
('TXN20240124000002', 'ACCT0000000006', 'DEBIT', 5000000.00, 85000000.00, 'ATM Withdrawal - HN', 'ATM', 'COMPLETED', '2024-01-24 08:15:00'),
('TXN20240124000003', 'ACCT0000000006', 'DEBIT', 5000000.00, 80000000.00, 'ATM Withdrawal - DN', 'ATM', 'COMPLETED', '2024-01-24 08:30:00'),
('TXN20240124000004', 'ACCT0000000006', 'DEBIT', 10000000.00, 70000000.00, 'ATM Withdrawal - HCM', 'ATM', 'COMPLETED', '2024-01-24 08:45:00'),
('TXN20240124000005', 'ACCT0000000006', 'DEBIT', 10000000.00, 60000000.00, 'ATM Withdrawal - HCM', 'ATM', 'COMPLETED', '2024-01-24 09:00:00'),
('TXN20240124000006', 'ACCT0000000006', 'DEBIT', 15000000.00, 45000000.00, 'ATM Withdrawal - HCM', 'ATM', 'COMPLETED', '2024-01-24 09:15:00'),
-- Fraud flag transactions
('TXN20240124000007', 'ACCT0000000010', 'DEBIT', 95000000.00, 425000000.00, 'Wire Transfer - Suspicious', 'WIRE', 'COMPLETED', '2024-01-24 23:45:00'),

-- Corporate transactions
('TXN20240115000010', 'ACCT0000000020', 'CREDIT', 5000000000.00, 15000000000.00, 'Client Payment - Q1', 'WIRE', 'COMPLETED', '2024-01-15 10:00:00'),
('TXN20240116000010', 'ACCT0000000020', 'DEBIT', 2500000000.00, 12500000000.00, 'Vendor Payment', 'WIRE', 'COMPLETED', '2024-01-16 11:30:00'),
('TXN20240117000010', 'ACCT0000000020', 'CREDIT', 8000000000.00, 20500000000.00, 'Investment Return', 'WIRE', 'COMPLETED', '2024-01-17 14:00:00'),
('TXN20240118000010', 'ACCT0000000020', 'DEBIT', 1200000000.00, 19300000000.00, 'Tax Payment', 'WIRE', 'COMPLETED', '2024-01-18 15:30:00');

--##############################################################################
-- CARDS
--##############################################################################
INSERT INTO cards (card_id, card_number, card_type, card_brand, customer_id, account_id, 
                   card_status, credit_limit, outstanding_amount, available_credit, expiry_date, issued_date) VALUES
-- Credit cards
('CARD0000000001', '4532015112830366', 'CREDIT', 'VISA', 'CUST0000000001', 'ACCT0000000001', 'ACTIVE', 500000000.00, 35000000.00, 465000000.00, '2026-12-31', '2022-01-15'),
('CARD0000000002', '5425233430109903', 'CREDIT', 'MASTERCARD', 'CUST0000000001', 'ACCT0000000002', 'ACTIVE', 1000000000.00, 120000000.00, 880000000.00, '2027-06-30', '2023-06-20'),
('CARD0000000003', '4532015112830377', 'CREDIT', 'VISA', 'CUST0000000002', 'ACCT0000000003', 'ACTIVE', 300000000.00, 45000000.00, 255000000.00, '2026-09-30', '2022-09-10'),
('CARD0000000004', '4916338506082832', 'CREDIT', 'VISA', 'CUST0000000004', 'ACCT0000000005', 'ACTIVE', 400000000.00, 65000000.00, 335000000.00, '2027-03-31', '2023-03-15'),
('CARD0000000005', '5194628432145678', 'CREDIT', 'MASTERCARD', 'CUST0000000007', 'ACCT0000000008', 'ACTIVE', 2000000000.00, 250000000.00, 1750000000.00, '2027-12-31', '2023-01-01'),
-- Debit cards
('CARD0000000006', '4532015112830388', 'DEBIT', 'VISA', 'CUST0000000003', 'ACCT0000000004', 'ACTIVE', NULL, NULL, NULL, '2026-06-30', '2022-06-15'),
('CARD0000000007', '4532015112830399', 'DEBIT', 'VISA', 'CUST0000000005', 'ACCT0000000006', 'ACTIVE', NULL, NULL, NULL, '2025-12-31', '2021-12-10'),
('CARD0000000008', '4916338506082843', 'DEBIT', 'VISA', 'CUST0000000006', 'ACCT0000000007', 'ACTIVE', NULL, NULL, NULL, '2026-03-31', '2022-03-20'),
('CARD0000000009', '5425233430109914', 'DEBIT', 'MASTERCARD', 'CUST0000000008', 'ACCT0000000009', 'ACTIVE', NULL, NULL, NULL, '2027-06-30', '2023-06-15'),
('CARD0000000010', '4532015112830400', 'DEBIT', 'VISA', 'CUST0000000010', 'ACCT0000000011', 'ACTIVE', NULL, NULL, NULL, '2026-09-30', '2022-09-01');

--##############################################################################
-- LOANS
--##############################################################################
INSERT INTO loans (loan_id, loan_number, customer_id, loan_type, loan_status, 
                   principal_amount, principal_outstanding, interest_rate, emi_amount, 
                   tenure_months, emis_paid, disbursement_date, maturity_date, 
                   collateral_type, collateral_value, npa_classification) VALUES
-- Home loans
('LOAN0000000001', 'HL20220001', 'CUST0000000001', 'HOME', 'ACTIVE', 5000000000.00, 4200000000.00, 0.0850, 39000000.00, 240, 24, '2022-01-15', '2042-01-15', 'PROPERTY', 7500000000.00, 'STD'),
('LOAN0000000002', 'HL20210002', 'CUST0000000002', 'HOME', 'ACTIVE', 3500000000.00, 2800000000.00, 0.0875, 28500000.00, 240, 36, '2021-06-20', '2041-06-20', 'PROPERTY', 5200000000.00, 'STD'),
('LOAN0000000003', 'HL20200003', 'CUST0000000004', 'HOME', 'ACTIVE', 2500000000.00, 1900000000.00, 0.0825, 21000000.00, 240, 48, '2020-03-10', '2040-03-10', 'PROPERTY', 4000000000.00, 'STD'),
-- Auto loans
('LOAN0000000004', 'AL20230001', 'CUST0000000003', 'AUTO', 'ACTIVE', 800000000.00, 650000000.00, 0.0750, 16500000.00, 60, 12, '2023-03-01', '2028-03-01', 'VEHICLE', 1000000000.00, 'STD'),
('LOAN0000000005', 'AL20220002', 'CUST0000000006', 'AUTO', 'ACTIVE', 600000000.00, 420000000.00, 0.0775, 12500000.00, 60, 24, '2022-11-15', '2027-11-15', 'VEHICLE', 750000000.00, 'STD'),
-- Personal loans
('LOAN0000000006', 'PL20230001', 'CUST0000000005', 'PERSONAL', 'ACTIVE', 200000000.00, 150000000.00, 0.1200, 4500000.00, 48, 6, '2023-08-01', '2027-08-01', NULL, NULL, 'SUB'),
('LOAN0000000007', 'PL20220002', 'CUST0000000009', 'PERSONAL', 'DEFAULTED', 150000000.00, 140000000.00, 0.1350, 4200000.00, 36, 12, '2022-06-15', '2025-06-15', NULL, NULL, 'DOUBTFUL'),
-- Business loans (SME)
('LOAN0000000008', 'BL20210001', 'CUST0000000030', 'BUSINESS', 'ACTIVE', 5000000000.00, 3800000000.00, 0.0950, 105000000.00, 60, 18, '2021-12-01', '2026-12-01', 'PROPERTY', 7500000000.00, 'STD'),
('LOAN0000000009', 'BL20220002', 'CUST0000000031', 'BUSINESS', 'ACTIVE', 3000000000.00, 2500000000.00, 0.0925, 65000000.00, 60, 12, '2022-06-15', '2027-06-15', 'EQUIPMENT', 4000000000.00, 'STD'),
('LOAN0000000010', 'BL20230003', 'CUST0000000033', 'BUSINESS', 'ACTIVE', 2000000000.00, 1800000000.00, 0.1000, 44000000.00, 60, 6, '2023-09-01', '2028-09-01', 'PROPERTY', 3000000000.00, 'SUB');

--##############################################################################
-- FIXED DEPOSITS
--##############################################################################
INSERT INTO fixed_deposits (fd_id, fd_number, customer_id, account_id, principal_amount, 
                           interest_rate, tenure_months, maturity_amount, deposit_date, maturity_date, status) VALUES
('FD0000000001', 'FD20230001', 'CUST0000000001', 'ACCT0000000001', 2000000000.00, 0.0650, 12, 2130000000.00, '2023-01-15', '2024-01-15', 'MATURED'),
('FD0000000002', 'FD20230002', 'CUST0000000001', 'ACCT0000000001', 5000000000.00, 0.0725, 24, 5725000000.00, '2023-06-20', '2025-06-20', 'ACTIVE'),
('FD0000000003', 'FD20230003', 'CUST0000000002', 'ACCT0000000003', 1500000000.00, 0.0675, 12, 1601250000.00, '2023-03-10', '2024-03-10', 'ACTIVE'),
('FD0000000004', 'FD20230004', 'CUST0000000007', 'ACCT0000000008', 10000000000.00, 0.0750, 36, 12250000000.00, '2023-01-01', '2026-01-01', 'ACTIVE'),
('FD0000000005', 'FD20230005', 'CUST0000000010', 'ACCT0000000011', 500000000.00, 0.0650, 12, 532500000.00, '2023-09-01', '2024-09-01', 'ACTIVE');

--##############################################################################
-- MERCHANTS
--##############################################################################
INSERT INTO merchants (merchant_id, merchant_name, merchant_category, mcc_code, 
                      business_type, risk_rating, settlement_cycle) VALUES
('MERCH00000001', 'VinMart Supermarket', 'RETAIL_GROCERY', '5411', 'Retail', 'LOW', 'T+1'),
('MERCH00000002', 'The Coffee House', 'FOOD_BEVERAGE', '5812', 'Restaurant', 'LOW', 'T+1'),
('MERCH00000003', 'FPT Shop Electronics', 'ELECTRONICS', '5732', 'Retail', 'LOW', 'T+1'),
('MERCH00000004', 'Vietjet Air', 'TRAVEL_AIRLINE', '4511', 'Travel', 'MEDIUM', 'T+2'),
('MERCH00000005', 'Grab Vietnam', 'TRANSPORT_TAXI', '4121', 'Transport', 'LOW', 'T+1'),
('MERCH00000006', 'Lazada Vietnam', 'E_COMMERCE', '5999', 'Online', 'MEDIUM', 'T+2'),
('MERCH00000007', 'Shopee Vietnam', 'E_COMMERCE', '5999', 'Online', 'MEDIUM', 'T+2'),
('MERCH00000008', 'Circle K Vietnam', 'RETAIL_GROCERY', '5411', 'Retail', 'LOW', 'T+1'),
('MERCH00000009', 'Nha Thuoc Long Chau', 'HEALTH_PHARMACY', '5912', 'Healthcare', 'LOW', 'T+1'),
('MERCH00000010', 'Unknown Merchant XYZ', 'UNKNOWN', '9999', 'Unknown', 'HIGH', 'T+3');

--##############################################################################
-- SAMPLE TRANSACTIONS WITH FRAUD PATTERNS
--##############################################################################

-- Pattern 1: Rapid ATM withdrawals (velocity fraud)
INSERT INTO transactions (transaction_id, account_id, transaction_type, amount, balance_after,
                          description, channel, merchant_id, status, fraud_flag, fraud_reason, transaction_time) VALUES
('TXNFRAUD000001', 'ACCT0000000006', 'DEBIT', 5000000.00, 75000000.00, 'ATM Withdrawal District 1', 'ATM', NULL, 'COMPLETED', TRUE, 'High velocity - 6th withdrawal in 2 hours', '2024-01-24 09:30:00'),
('TXNFRAUD000002', 'ACCT0000000006', 'DEBIT', 5000000.00, 70000000.00, 'ATM Withdrawal District 3', 'ATM', NULL, 'COMPLETED', TRUE, 'High velocity - 7th withdrawal in 2 hours', '2024-01-24 09:45:00'),
('TXNFRAUD000003', 'ACCT0000000006', 'DEBIT', 5000000.00, 65000000.00, 'ATM Withdrawal District 7', 'ATM', NULL, 'COMPLETED', TRUE, 'High velocity - 8th withdrawal in 2 hours', '2024-01-24 10:00:00');

-- Pattern 2: Unusual amount (amount fraud)
INSERT INTO transactions (transaction_id, account_id, transaction_type, amount, balance_after,
                          description, channel, merchant_id, status, fraud_flag, fraud_reason, transaction_time) VALUES
('TXNFRAUD000004', 'ACCT0000000010', 'DEBIT', 95000000.00, 425000000.00, 'Wire Transfer - Offshore', 'WIRE', NULL, 'COMPLETED', TRUE, 'Unusual amount - 20x average transaction', '2024-01-24 23:58:00');

-- Pattern 3: Geographic anomaly (geo fraud)
INSERT INTO transactions (transaction_id, account_id, transaction_type, amount, balance_after,
                          description, channel, merchant_id, status, fraud_flag, fraud_reason, transaction_time) VALUES
('TXNFRAUD000005', 'ACCT0000000001', 'DEBIT', 25000000.00, 1007000000.00, 'ATM Withdrawal - Foreign Country', 'ATM', NULL, 'COMPLETED', TRUE, 'Geographic impossibility - 30 min after local transaction', '2024-01-24 15:30:00');

--##############################################################################
-- VERIFICATION QUERIES
--##############################################################################

-- Count records in each table
SELECT 'customers' AS table_name, COUNT(*) AS record_count FROM customers
UNION ALL
SELECT 'accounts', COUNT(*) FROM accounts
UNION ALL
SELECT 'transactions', COUNT(*) FROM transactions
UNION ALL
SELECT 'cards', COUNT(*) FROM cards
UNION ALL
SELECT 'loans', COUNT(*) FROM loans
UNION ALL
SELECT 'fixed_deposits', COUNT(*) FROM fixed_deposits
UNION ALL
SELECT 'branches', COUNT(*) FROM branches
UNION ALL
SELECT 'merchants', COUNT(*) FROM merchants;

-- Total deposits by customer type
SELECT 
    c.customer_type,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    SUM(a.balance) AS total_deposits
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
GROUP BY c.customer_type
ORDER BY total_deposits DESC;

-- Total loan book by type
SELECT 
    loan_type,
    COUNT(*) AS loan_count,
    SUM(principal_amount) AS total_disbursed,
    SUM(principal_outstanding) AS outstanding,
    AVG(interest_rate) AS avg_rate
FROM loans
GROUP BY loan_type
ORDER BY total_disbursed DESC;
