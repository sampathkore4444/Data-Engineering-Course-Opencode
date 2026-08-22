"""
Banking ETL Pipeline: Extract from Source Systems, Load to Dremio
================================================================

This script extracts data from PostgreSQL (Core Banking) and MySQL (Credit Cards)
and loads it into Dremio's source connections for virtualization.

Usage:
    python extract_load.py --source core_banking --target dremio
    python extract_load.py --source credit_cards --target dremio
    python extract_load.py --all --target dremio
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
from contextlib import contextmanager

import psycopg2
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    user: str
    password: str
    engine: Optional[object] = None


class BankingETLPipeline:
    """
    ETL Pipeline for Banking Data
    Extracts from source systems and loads to Dremio-compatible storage
    """
    
    def __init__(self):
        self.core_banking_config = DatabaseConfig(
            host=os.getenv('CORE_BANKING_HOST', 'localhost'),
            port=int(os.getenv('CORE_BANKING_PORT', 5432)),
            database=os.getenv('CORE_BANKING_DB', 'core_banking'),
            user=os.getenv('CORE_BANKING_USER', 'postgres'),
            password=os.getenv('CORE_BANKING_PASSWORD', 'postgres')
        )
        
        self.credit_cards_config = DatabaseConfig(
            host=os.getenv('CREDIT_CARDS_HOST', 'localhost'),
            port=int(os.getenv('CREDIT_CARDS_PORT', 3306)),
            database=os.getenv('CREDIT_CARDS_DB', 'credit_cards'),
            user=os.getenv('CREDIT_CARDS_USER', 'root'),
            password=os.getenv('CREDIT_CARDS_PASSWORD', 'password')
        )
        
        # Lakehouse storage paths
        self.bronze_path = os.getenv('LAKEHOUSE_BRONZE', '/lake/bronze')
        self.silver_path = os.getenv('LAKEHOUSE_SILVER', '/lake/silver')
        self.gold_path = os.getenv('LAKEHOUSE_GOLD', '/lake/gold')
        
        self.execution_date = datetime.now().strftime('%Y-%m-%d')
        
    @contextmanager
    def get_connection(self, config: DatabaseConfig):
        """Context manager for database connections"""
        engine = None
        try:
            if config.host and ':' in config.host:
                # MySQL connection
                connection_string = (
                    f"mysql+mysqlconnector://{config.user}:{config.password}"
                    f"@{config.host}:{config.port}/{config.database}"
                )
            else:
                # PostgreSQL connection
                connection_string = (
                    f"postgresql+psycopg2://{config.user}:{config.password}"
                    f"@{config.host}:{config.port}/{config.database}"
                )
            
            engine = create_engine(connection_string)
            with engine.connect() as connection:
                yield connection
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise
        finally:
            if engine:
                engine.dispose()
    
    def extract_data(self, config: DatabaseConfig, query: str, table_name: str) -> pd.DataFrame:
        """Extract data from source system"""
        logger.info(f"Extracting data from {table_name}...")
        try:
            with self.get_connection(config) as connection:
                df = pd.read_sql(query, connection)
                logger.info(f"Extracted {len(df)} rows from {table_name}")
                return df
        except Exception as e:
            logger.error(f"Extraction failed for {table_name}: {e}")
            raise
    
    def load_to_bronze(self, df: pd.DataFrame, source_name: str, table_name: str):
        """Load raw data to Bronze layer (Parquet format)"""
        output_path = f"{self.bronze_path}/{source_name}/{table_name}/{self.execution_date}"
        os.makedirs(output_path, exist_ok=True)
        
        logger.info(f"Loading {len(df)} rows to Bronze: {output_path}")
        df.to_parquet(f"{output_path}/data.parquet", index=False, compression='snappy')
        
        # Write metadata
        metadata = {
            'source': source_name,
            'table': table_name,
            'execution_date': self.execution_date,
            'row_count': len(df),
            'columns': list(df.columns),
            'loaded_at': datetime.now().isoformat()
        }
        
        pd.DataFrame([metadata]).to_json(
            f"{output_path}/metadata.json", 
            orient='records', 
            indent=2
        )
        
        logger.info(f"Bronze load complete: {len(df)} rows")
    
    def extract_core_banking(self):
        """Extract all Core Banking tables"""
        queries = {
            'customers': """
                SELECT customer_id, customer_name, customer_type, email, phone,
                       address, city, state, pincode, pan_number, aadhaar_number,
                       kyc_status, customer_since, risk_category, created_at
                FROM customers
            """,
            'accounts': """
                SELECT account_id, customer_id, account_type, balance, status,
                       opened_date, branch_code, ifsc_code, created_at
                FROM accounts
            """,
            'transactions': """
                SELECT transaction_id, customer_id, account_id, transaction_type,
                       amount, balance_after, description, transaction_date, 
                       status, channel, created_at
                FROM transactions
                WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
            """,
            'loan_accounts': """
                SELECT loan_id, customer_id, loan_type, loan_amount, 
                       principal_outstanding, interest_rate, tenure_months,
                       emi_amount, disbursement_date, first_emi_date, maturity_date,
                       loan_status, npa_classification, days_past_due,
                       last_payment_date, property_address, collateral_value,
                       insurance_expiry, created_at
                FROM loan_accounts
            """,
            'loan_payments': """
                SELECT payment_id, loan_id, payment_date, emi_number,
                       principal_component, interest_component, total_payment,
                       payment_mode, status, late_fee, created_at
                FROM loan_payments
                WHERE payment_date >= CURRENT_DATE - INTERVAL '90 days'
            """
        }
        
        for table_name, query in queries.items():
            try:
                df = self.extract_data(self.core_banking_config, query, table_name)
                self.load_to_bronze(df, 'core_banking', table_name)
            except Exception as e:
                logger.error(f"Failed to extract {table_name}: {e}")
                raise
    
    def extract_credit_cards(self):
        """Extract all Credit Cards tables"""
        queries = {
            'credit_cards': """
                SELECT card_id, customer_id, card_number, card_type,
                       credit_limit, outstanding, card_status, issue_date,
                       expiry_date, billing_cycle, reward_points, annual_fee,
                       last_transaction_date, created_at
                FROM credit_cards
            """,
            'card_transactions': """
                SELECT transaction_id, card_id, transaction_amount,
                       transaction_type, merchant_name, merchant_category,
                       transaction_date, posting_date, description, status
                FROM card_transactions
                WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
            """,
            'card_billing': """
                SELECT billing_id, card_id, billing_month, opening_balance,
                       total_purchases, total_payments, total_fees,
                       total_interest, closing_balance, minimum_payment,
                       due_date, payment_status
                FROM card_billing
                WHERE billing_month >= DATE_SUB(CURRENT_DATE, INTERVAL '6 MONTH')
            """
        }
        
        for table_name, query in queries.items():
            try:
                df = self.extract_data(self.credit_cards_config, query, table_name)
                self.load_to_bronze(df, 'credit_cards', table_name)
            except Exception as e:
                logger.error(f"Failed to extract {table_name}: {e}")
                raise
    
    def run_incremental_extract(self, source: str, days_back: int = 1):
        """Run incremental extraction for specific tables"""
        logger.info(f"Running incremental extract for {source}, last {days_back} days")
        
        if source == 'core_banking':
            # Only extract recent transactions and payments
            queries = {
                'transactions': f"""
                    SELECT * FROM transactions
                    WHERE transaction_date >= CURRENT_DATE - INTERVAL '{days_back} days'
                """,
                'loan_payments': f"""
                    SELECT * FROM loan_payments
                    WHERE payment_date >= CURRENT_DATE - INTERVAL '{days_back} days'
                """
            }
            config = self.core_banking_config
            source_name = 'core_banking'
            
        elif source == 'credit_cards':
            queries = {
                'card_transactions': f"""
                    SELECT * FROM card_transactions
                    WHERE transaction_date >= CURRENT_DATE - INTERVAL '{days_back} days'
                """
            }
            config = self.credit_cards_config
            source_name = 'credit_cards'
        else:
            raise ValueError(f"Unknown source: {source}")
        
        for table_name, query in queries.items():
            try:
                df = self.extract_data(config, query, table_name)
                if len(df) > 0:
                    self.load_to_bronze(df, source_name, table_name)
                else:
                    logger.info(f"No new data for {table_name}")
            except Exception as e:
                logger.error(f"Failed incremental extract for {table_name}: {e}")
                raise
    
    def run_full_extract(self):
        """Run full extraction from all source systems"""
        logger.info("Starting full extraction from all source systems...")
        
        start_time = datetime.now()
        
        try:
            # Extract from Core Banking
            logger.info("Extracting Core Banking data...")
            self.extract_core_banking()
            
            # Extract from Credit Cards
            logger.info("Extracting Credit Cards data...")
            self.extract_credit_cards()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"Full extraction completed in {duration:.2f} seconds")
            
            return {
                'status': 'success',
                'execution_date': self.execution_date,
                'duration_seconds': duration,
                'sources': ['core_banking', 'credit_cards']
            }
            
        except Exception as e:
            logger.error(f"Full extraction failed: {e}")
            raise
    
    def validate_extraction(self, source: str, table_name: str) -> bool:
        """Validate extracted data"""
        bronze_path = f"{self.bronze_path}/{source}/{table_name}/{self.execution_date}"
        
        if not os.path.exists(f"{bronze_path}/data.parquet"):
            logger.error(f"Parquet file not found: {bronze_path}")
            return False
        
        df = pd.read_parquet(f"{bronze_path}/data.parquet")
        
        # Check row count
        if len(df) == 0:
            logger.warning(f"Empty dataframe for {table_name}")
            return False
        
        # Check for nulls in required columns
        null_counts = df.isnull().sum()
        high_null_columns = null_counts[null_counts > len(df) * 0.5]
        
        if not high_null_columns.empty:
            logger.warning(f"High null counts in {table_name}: {high_null_columns.to_dict()}")
        
        logger.info(f"Validation passed for {table_name}: {len(df)} rows")
        return True


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Banking ETL Pipeline')
    parser.add_argument('--source', choices=['core_banking', 'credit_cards'], 
                       help='Source system to extract from')
    parser.add_argument('--all', action='store_true', 
                       help='Extract from all source systems')
    parser.add_argument('--incremental', action='store_true',
                       help='Run incremental extraction')
    parser.add_argument('--days', type=int, default=1,
                       help='Days back for incremental extraction')
    parser.add_argument('--validate', action='store_true',
                       help='Validate extracted data')
    
    args = parser.parse_args()
    
    pipeline = BankingETLPipeline()
    
    try:
        if args.all:
            result = pipeline.run_full_extract()
        elif args.source:
            if args.incremental:
                pipeline.run_incremental_extract(args.source, args.days)
            else:
                if args.source == 'core_banking':
                    pipeline.extract_core_banking()
                elif args.source == 'credit_cards':
                    pipeline.extract_credit_cards()
        else:
            # Default: run full extraction
            result = pipeline.run_full_extract()
        
        logger.info("ETL Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()