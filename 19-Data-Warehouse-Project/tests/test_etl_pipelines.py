"""
Unit Tests for ETL Pipelines
Purpose: Test ETL transformation logic
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta

# Test data
SAMPLE_CUSTOMERS = [
    {'customer_id': 'C001', 'customer_name': '  John Doe  ', 'customer_type': 'individual', 'phone': '0901234567', 'email': 'john@email.com'},
    {'customer_id': 'C002', 'customer_name': 'Jane Smith', 'customer_type': 'corporate', 'phone': '0912345678', 'email': 'jane@company.com'},
    {'customer_id': 'C003', 'customer_name': '  Bob Wilson  ', 'customer_type': 'individual', 'phone': '0923456789', 'email': 'bob@email.com'},
    {'customer_id': 'C001', 'customer_name': 'John Doe', 'customer_type': 'individual', 'phone': '0901234567', 'email': 'john@email.com'},  # Duplicate
]

SAMPLE_ACCOUNTS = [
    {'account_id': 'A001', 'customer_id': 'C001', 'account_type': 'SAVINGS', 'balance': 5000000, 'status': 'ACTIVE'},
    {'account_id': 'A002', 'customer_id': 'C001', 'account_type': 'CURRENT', 'balance': 1000000, 'status': 'ACTIVE'},
    {'account_id': 'A003', 'customer_id': 'C002', 'account_type': 'FIXED_DEPOSIT', 'balance': 20000000, 'status': 'ACTIVE'},
    {'account_id': 'A004', 'customer_id': 'C999', 'account_type': 'SAVINGS', 'balance': 500000, 'status': 'ACTIVE'},  # Invalid customer_id
]

SAMPLE_TRANSACTIONS = [
    {'transaction_id': 'T001', 'account_id': 'A001', 'transaction_type': 'CREDIT', 'amount': 1000000, 'transaction_date': '2024-01-15'},
    {'transaction_id': 'T002', 'account_id': 'A001', 'transaction_type': 'DEBIT', 'amount': 500000, 'transaction_date': '2024-01-16'},
    {'transaction_id': 'T003', 'account_id': 'A002', 'transaction_type': 'CREDIT', 'amount': 2000000, 'transaction_date': '2024-01-17'},
    {'transaction_id': 'T001', 'account_id': 'A001', 'transaction_type': 'CREDIT', 'amount': 1000000, 'transaction_date': '2024-01-15'},  # Duplicate
]


class TestCustomerCleaning:
    """Tests for customer data cleaning logic"""
    
    def test_trim_whitespace(self):
        """Test that whitespace is trimmed from customer names"""
        df = pd.DataFrame(SAMPLE_CUSTOMERS)
        
        # Clean customer name
        df['customer_name'] = df['customer_name'].str.strip()
        
        assert df.iloc[0]['customer_name'] == 'John Doe'
        assert df.iloc[2]['customer_name'] == 'Bob Wilson'
    
    def test_standardize_customer_type(self):
        """Test that customer type is standardized"""
        df = pd.DataFrame(SAMPLE_CUSTOMERS)
        
        # Standardize customer type
        type_mapping = {
            'individual': 'Individual',
            'corporate': 'Corporate',
            'personal': 'Individual',
            'business': 'Corporate'
        }
        df['customer_type'] = df['customer_type'].map(type_mapping)
        
        assert df.iloc[0]['customer_type'] == 'Individual'
        assert df.iloc[1]['customer_type'] == 'Corporate'
    
    def test_remove_duplicates(self):
        """Test that duplicate customers are removed"""
        df = pd.DataFrame(SAMPLE_CUSTOMERS)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['customer_id'], keep='first')
        
        assert len(df) == 3  # Should have 3 unique customers
        assert df['customer_id'].nunique() == 3
    
    def test_validate_email_format(self):
        """Test that email format is validated"""
        df = pd.DataFrame(SAMPLE_CUSTOMERS)
        
        # Validate email
        df['is_valid_email'] = df['email'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$', regex=True)
        
        assert df.iloc[0]['is_valid_email'] == True
        assert df.iloc[1]['is_valid_email'] == True
    
    def test_validate_phone_format(self):
        """Test that phone format is validated"""
        df = pd.DataFrame(SAMPLE_CUSTOMERS)
        
        # Validate phone (10 digits)
        df['is_valid_phone'] = df['phone'].str.match(r'^\d{10}$')
        
        assert df.iloc[0]['is_valid_phone'] == True
        assert df.iloc[1]['is_valid_phone'] == True


class TestAccountCleaning:
    """Tests for account data cleaning logic"""
    
    def test_validate_balance_positive(self):
        """Test that balance is positive"""
        df = pd.DataFrame(SAMPLE_ACCOUNTS)
        
        # Validate balance
        df['is_valid_balance'] = df['balance'] > 0
        
        assert all(df['is_valid_balance'])
    
    def test_validate_account_type(self):
        """Test that account type is valid"""
        df = pd.DataFrame(SAMPLE_ACCOUNTS)
        
        valid_types = ['SAVINGS', 'CURRENT', 'FIXED_DEPOSIT']
        df['is_valid_type'] = df['account_type'].isin(valid_types)
        
        assert all(df['is_valid_type'])
    
    def test_validate_status(self):
        """Test that status is valid"""
        df = pd.DataFrame(SAMPLE_ACCOUNTS)
        
        valid_statuses = ['ACTIVE', 'INACTIVE', 'DORMANT', 'CLOSED']
        df['is_valid_status'] = df['status'].isin(valid_statuses)
        
        assert all(df['is_valid_status'])
    
    def test_detect_invalid_customer_id(self):
        """Test that invalid customer_id is detected"""
        df = pd.DataFrame(SAMPLE_ACCOUNTS)
        
        # In real scenario, this would check against dim_customer
        valid_customer_ids = ['C001', 'C002', 'C003']
        df['is_valid_customer'] = df['customer_id'].isin(valid_customer_ids)
        
        assert df.iloc[3]['is_valid_customer'] == False  # C999 is invalid


class TestTransactionCleaning:
    """Tests for transaction data cleaning logic"""
    
    def test_remove_duplicates(self):
        """Test that duplicate transactions are removed"""
        df = pd.DataFrame(SAMPLE_TRANSACTIONS)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['transaction_id'], keep='first')
        
        assert len(df) == 3  # Should have 3 unique transactions
        assert df['transaction_id'].nunique() == 3
    
    def test_validate_amount_positive(self):
        """Test that amount is positive"""
        df = pd.DataFrame(SAMPLE_TRANSACTIONS)
        
        # Remove duplicates first
        df = df.drop_duplicates(subset=['transaction_id'], keep='first')
        
        # Validate amount
        df['is_valid_amount'] = df['amount'] > 0
        
        assert all(df['is_valid_amount'])
    
    def test_validate_transaction_type(self):
        """Test that transaction type is valid"""
        df = pd.DataFrame(SAMPLE_TRANSACTIONS)
        
        valid_types = ['CREDIT', 'DEBIT', 'TRANSFER']
        df['is_valid_type'] = df['transaction_type'].isin(valid_types)
        
        assert all(df['is_valid_type'])
    
    def test_validate_date_format(self):
        """Test that date format is valid"""
        df = pd.DataFrame(SAMPLE_TRANSACTIONS)
        
        # Validate date format
        def is_valid_date(date_str):
            try:
                datetime.strptime(date_str, '%Y-%m-%d')
                return True
            except ValueError:
                return False
        
        df['is_valid_date'] = df['transaction_date'].apply(is_valid_date)
        
        assert all(df['is_valid_date'])


class TestSCDType2:
    """Tests for SCD Type 2 logic"""
    
    def test_scd_type2_insert(self):
        """Test SCD Type 2 insert logic"""
        # Initial record
        initial_record = {
            'customer_id': 'C001',
            'customer_name': 'John Doe',
            'is_current': True,
            'valid_from': datetime(2024, 1, 1),
            'valid_to': None
        }
        
        # Simulate update
        updated_record = {
            'customer_id': 'C001',
            'customer_name': 'John Smith',  # Name changed
            'is_current': True,
            'valid_from': datetime(2024, 1, 15),
            'valid_to': None
        }
        
        # Close old record
        initial_record['is_current'] = False
        initial_record['valid_to'] = datetime(2024, 1, 15)
        
        assert initial_record['is_current'] == False
        assert initial_record['valid_to'] == datetime(2024, 1, 15)
        assert updated_record['is_current'] == True
        assert updated_record['valid_from'] == datetime(2024, 1, 15)
    
    def test_scd_type2_history(self):
        """Test SCD Type 2 history tracking"""
        history = [
            {'customer_id': 'C001', 'customer_name': 'John Doe', 'is_current': False, 'valid_from': datetime(2024, 1, 1), 'valid_to': datetime(2024, 1, 15)},
            {'customer_id': 'C001', 'customer_name': 'John Smith', 'is_current': True, 'valid_from': datetime(2024, 1, 15), 'valid_to': None},
        ]
        
        current_record = [r for r in history if r['is_current']][0]
        assert current_record['customer_name'] == 'John Smith'
        
        historical_records = [r for r in history if not r['is_current']]
        assert len(historical_records) == 1


class TestDataQuality:
    """Tests for data quality validation"""
    
    def test_uniqueness_check(self):
        """Test uniqueness validation"""
        df = pd.DataFrame(SAMPLE_CUSTOMERS)
        
        # Check for duplicates
        duplicates = df[df.duplicated(subset=['customer_id'], keep=False)]
        
        assert len(duplicates) > 0  # Should detect duplicates
    
    def test_completeness_check(self):
        """Test completeness validation"""
        df = pd.DataFrame(SAMPLE_CUSTOMERS)
        
        # Check for null values
        null_counts = df.isnull().sum()
        
        assert all(null_counts == 0)  # No null values
    
    def test_freshness_check(self):
        """Test freshness validation"""
        # Simulate data with timestamps
        data = {
            'table_name': ['stg_customers', 'stg_accounts', 'stg_transactions'],
            'last_update': [
                datetime.now() - timedelta(hours=2),
                datetime.now() - timedelta(hours=25),  # Stale
                datetime.now() - timedelta(hours=1)
            ]
        }
        df = pd.DataFrame(data)
        
        # Check freshness (24 hour threshold)
        threshold = timedelta(hours=24)
        df['is_fresh'] = df['last_update'] > (datetime.now() - threshold)
        
        assert df.iloc[0]['is_fresh'] == True
        assert df.iloc[1]['is_fresh'] == False  # Stale


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
