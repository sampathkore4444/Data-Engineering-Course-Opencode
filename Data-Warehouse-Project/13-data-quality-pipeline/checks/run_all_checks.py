"""
Data Quality Pipeline - Run All Checks
Banking Data Warehouse

This script runs all data quality checks against the data warehouse.
"""

import psycopg2
import uuid
import json
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import sys

# =====================================================
# CONFIGURATION
# =====================================================
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'banking_dw',
    'user': 'dw_admin',
    'password': 'secure_password'
}

# =====================================================
# QUALITY CHECK FUNCTIONS
# =====================================================

class DataQualityPipeline:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = False
        self.execution_id = str(uuid.uuid4())
        self.results = []
        
    def close(self):
        if self.conn:
            self.conn.close()
    
    def run_check(self, rule_id: int, table_name: str, column_name: str, 
                  rule_type: str, check_sql: str, threshold: float = 100.0) -> Dict:
        """Run a single quality check and store results."""
        cursor = self.conn.cursor()
        started_at = datetime.now()
        
        try:
            # Execute the check SQL
            cursor.execute(check_sql)
            result = cursor.fetchone()
            
            # Parse result based on check type
            if rule_type == 'uniqueness':
                total_rows = result[0] if result else 0
                duplicate_rows = result[1] if result and len(result) > 1 else 0
                passed_rows = total_rows - duplicate_rows
                pass_rate = (passed_rows / total_rows * 100) if total_rows > 0 else 100
                
            elif rule_type == 'completeness':
                total_rows = result[0] if result else 0
                null_rows = result[1] if result and len(result) > 1 else 0
                passed_rows = total_rows - null_rows
                pass_rate = (passed_rows / total_rows * 100) if total_rows > 0 else 100
                
            elif rule_type == 'range':
                total_rows = result[0] if result else 0
                invalid_rows = result[1] if result and len(result) > 1 else 0
                passed_rows = total_rows - invalid_rows
                pass_rate = (passed_rows / total_rows * 100) if total_rows > 0 else 100
                
            elif rule_type == 'freshness':
                hours_old = result[0] if result else 0
                passed_rows = 1 if hours_old <= threshold else 0
                total_rows = 1
                pass_rate = 100 if passed_rows == 1 else 0
                threshold = 100  # Freshness is pass/fail
                
            elif rule_type == 'accepted_values':
                total_rows = result[0] if result else 0
                invalid_rows = result[1] if result and len(result) > 1 else 0
                passed_rows = total_rows - invalid_rows
                pass_rate = (passed_rows / total_rows * 100) if total_rows > 0 else 100
                
            elif rule_type == 'statistical':
                total_rows = result[0] if result else 0
                anomaly_rows = result[1] if result and len(result) > 1 else 0
                passed_rows = total_rows - anomaly_rows
                pass_rate = (passed_rows / total_rows * 100) if total_rows > 0 else 100
                
            else:
                # Generic check
                total_rows = result[0] if result else 0
                passed_rows = total_rows
                pass_rate = 100
            
            # Determine status
            threshold_met = pass_rate >= threshold
            status = 'PASS' if threshold_met else 'FAIL'
            
            completed_at = datetime.now()
            duration_ms = int((completed_at - started_at).total_seconds() * 1000)
            
            # Store result
            insert_sql = """
                INSERT INTO dq.test_results 
                (rule_id, execution_id, table_name, column_name, rule_type, 
                 test_sql, status, total_rows, passed_rows, failed_rows, 
                 pass_rate, threshold_value, actual_value, threshold_met,
                 started_at, completed_at, duration_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING result_id
            """
            cursor.execute(insert_sql, (
                rule_id, self.execution_id, table_name, column_name, rule_type,
                check_sql, status, total_rows, passed_rows, total_rows - passed_rows,
                pass_rate, threshold, pass_rate, threshold_met,
                started_at, completed_at, duration_ms
            ))
            
            result_id = cursor.fetchone()[0]
            self.conn.commit()
            
            return {
                'result_id': result_id,
                'status': status,
                'pass_rate': pass_rate,
                'threshold': threshold,
                'threshold_met': threshold_met
            }
            
        except Exception as e:
            self.conn.rollback()
            completed_at = datetime.now()
            duration_ms = int((completed_at - started_at).total_seconds() * 1000)
            
            # Store error result
            insert_sql = """
                INSERT INTO dq.test_results 
                (rule_id, execution_id, table_name, column_name, rule_type,
                 test_sql, status, error_message, started_at, completed_at, duration_ms)
                VALUES (%s, %s, %s, %s, %s, %s, 'ERROR', %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                rule_id, self.execution_id, table_name, column_name, rule_type,
                check_sql, str(e), started_at, completed_at, duration_ms
            ))
            self.conn.commit()
            
            return {
                'status': 'ERROR',
                'error': str(e)
            }
    
    def run_completeness_check(self, table_name: str, column_name: str, 
                               threshold: float = 100.0) -> Dict:
        """Check for null values in a column."""
        rule_id = self.get_rule_id(table_name, column_name, 'completeness')
        
        check_sql = f"""
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) as null_rows
            FROM {table_name}
        """
        
        return self.run_check(rule_id, table_name, column_name, 'completeness', 
                             check_sql, threshold)
    
    def run_uniqueness_check(self, table_name: str, column_name: str, 
                             threshold: float = 100.0) -> Dict:
        """Check for duplicate values in a column."""
        rule_id = self.get_rule_id(table_name, column_name, 'uniqueness')
        
        check_sql = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(*) - COUNT(DISTINCT {column_name}) as duplicate_rows
            FROM {table_name}
        """
        
        return self.run_check(rule_id, table_name, column_name, 'uniqueness',
                             check_sql, threshold)
    
    def run_range_check(self, table_name: str, column_name: str, 
                        min_value: float, max_value: float,
                        threshold: float = 100.0) -> Dict:
        """Check if values are within a valid range."""
        rule_id = self.get_rule_id(table_name, column_name, 'range')
        
        check_sql = f"""
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN {column_name} < {min_value} OR {column_name} > {max_value} 
                    THEN 1 ELSE 0 END) as invalid_rows
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
        """
        
        return self.run_check(rule_id, table_name, column_name, 'range',
                             check_sql, threshold)
    
    def run_freshness_check(self, table_name: str, column_name: str,
                            max_age_hours: int) -> Dict:
        """Check if data is fresh (updated within specified hours)."""
        rule_id = self.get_rule_id(table_name, column_name, 'freshness')
        
        check_sql = f"""
            SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({column_name}))) / 3600 
            as hours_old
            FROM {table_name}
        """
        
        return self.run_check(rule_id, table_name, column_name, 'freshness',
                             check_sql, max_age_hours)
    
    def run_accepted_values_check(self, table_name: str, column_name: str,
                                  valid_values: List[str], 
                                  threshold: float = 100.0) -> Dict:
        """Check if column values are in the accepted list."""
        rule_id = self.get_rule_id(table_name, column_name, 'accepted_values')
        
        values_str = "', '".join(valid_values)
        check_sql = f"""
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN {column_name} NOT IN ('{values_str}') THEN 1 ELSE 0 END) 
                as invalid_rows
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
        """
        
        return self.run_check(rule_id, table_name, column_name, 'accepted_values',
                             check_sql, threshold)
    
    def run_custom_sql_check(self, rule_id: int, table_name: str,
                            check_sql: str, min_value: float = None,
                            max_value: float = None) -> Dict:
        """Run a custom SQL check."""
        cursor = self.conn.cursor()
        started_at = datetime.now()
        
        try:
            cursor.execute(check_sql)
            result = cursor.fetchone()
            value = result[0] if result else 0
            
            # Determine pass/fail
            if min_value is not None and max_value is not None:
                status = 'PASS' if min_value <= value <= max_value else 'FAIL'
            elif min_value is not None:
                status = 'PASS' if value >= min_value else 'FAIL'
            elif max_value is not None:
                status = 'PASS' if value <= max_value else 'FAIL'
            else:
                status = 'PASS'
            
            completed_at = datetime.now()
            duration_ms = int((completed_at - started_at).total_seconds() * 1000)
            
            # Store result
            insert_sql = """
                INSERT INTO dq.test_results 
                (rule_id, execution_id, table_name, rule_type, test_sql,
                 status, total_rows, pass_rate, threshold_met,
                 started_at, completed_at, duration_ms)
                VALUES (%s, %s, %s, 'custom_sql', %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                rule_id, self.execution_id, table_name, check_sql,
                status, value, 100 if status == 'PASS' else 0, 
                status == 'PASS', started_at, completed_at, duration_ms
            ))
            self.conn.commit()
            
            return {'status': status, 'value': value}
            
        except Exception as e:
            self.conn.rollback()
            return {'status': 'ERROR', 'error': str(e)}
    
    def get_rule_id(self, table_name: str, column_name: str, rule_type: str) -> int:
        """Get rule_id from rule catalog."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT rule_id FROM dq.rule_catalog 
            WHERE table_name = %s AND column_name = %s AND rule_type = %s
        """, (table_name, column_name, rule_type))
        
        result = cursor.fetchone()
        return result[0] if result else None
    
    def run_all_staging_checks(self) -> Dict:
        """Run all checks for staging tables."""
        results = {}
        
        # Staging Customers
        results['stg_customers'] = {
            'uniqueness': self.run_uniqueness_check('staging.stg_customers', 'customer_id'),
            'completeness_id': self.run_completeness_check('staging.stg_customers', 'customer_id', 100),
            'completeness_email': self.run_completeness_check('staging.stg_customers', 'email', 95),
            'freshness': self.run_freshness_check('staging.stg_customers', 'updated_at', 24)
        }
        
        # Staging Accounts
        results['stg_accounts'] = {
            'uniqueness': self.run_uniqueness_check('staging.stg_accounts', 'account_id'),
            'completeness_id': self.run_completeness_check('staging.stg_accounts', 'account_id', 100),
            'completeness_balance': self.run_completeness_check('staging.stg_accounts', 'balance', 100),
            'range_balance': self.run_range_check('staging.stg_accounts', 'balance', 0, 999999999999),
            'freshness': self.run_freshness_check('staging.stg_accounts', 'updated_at', 24)
        }
        
        # Staging Transactions
        results['stg_transactions'] = {
            'uniqueness': self.run_uniqueness_check('staging.stg_transactions', 'transaction_id'),
            'completeness_id': self.run_completeness_check('staging.stg_transactions', 'transaction_id', 100),
            'completeness_amount': self.run_completeness_check('staging.stg_transactions', 'amount', 100),
            'range_amount': self.run_range_check('staging.stg_transactions', 'amount', 0.01, 999999999999),
            'accepted_values_type': self.run_accepted_values_check(
                'staging.stg_transactions', 'transaction_type', 
                ['DEBIT', 'CREDIT', 'TRANSFER']
            ),
            'freshness': self.run_freshness_check('staging.stg_transactions', 'created_at', 1)
        }
        
        return results
    
    def run_all_gold_checks(self) -> Dict:
        """Run all checks for gold tables."""
        results = {}
        
        # Dim Customer
        results['dim_customer'] = {
            'uniqueness_sk': self.run_uniqueness_check('gold.dim_customer', 'customer_sk'),
            'uniqueness_id': self.run_uniqueness_check('gold.dim_customer', 'customer_id'),
            'completeness_name': self.run_completeness_check('gold.dim_customer', 'customer_name', 100),
            'accepted_values_segment': self.run_accepted_values_check(
                'gold.dim_customer', 'customer_segment',
                ['PLATINUM', 'GOLD', 'SILVER', 'STANDARD']
            ),
            'range_balance': self.run_range_check('gold.dim_customer', 'total_balance', 0, 999999999999)
        }
        
        # Fact Transactions
        results['fact_transactions'] = {
            'uniqueness': self.run_uniqueness_check('gold.fact_transactions', 'transaction_id'),
            'completeness_account_sk': self.run_completeness_check('gold.fact_transactions', 'account_sk', 100),
            'completeness_date_sk': self.run_completeness_check('gold.fact_transactions', 'transaction_date_sk', 100),
            'range_amount': self.run_range_check('gold.fact_transactions', 'amount', 0.01, 999999999999),
            'freshness': self.run_freshness_check('gold.fact_transactions', 'created_at', 2)
        }
        
        return results
    
    def run_business_rule_checks(self) -> Dict:
        """Run custom business rule checks."""
        results = {}
        
        # Balance Reconciliation
        balance_check_sql = """
            SELECT COUNT(*)
            FROM (
                SELECT 
                    a.account_id,
                    a.balance as reported_balance,
                    COALESCE(SUM(CASE WHEN t.transaction_type = 'CREDIT' THEN t.amount ELSE 0 END), 0) -
                    COALESCE(SUM(CASE WHEN t.transaction_type = 'DEBIT' THEN t.amount ELSE 0 END), 0) as calculated_balance
                FROM gold.dim_account a
                LEFT JOIN gold.fact_transactions t ON a.account_sk = t.account_sk
                GROUP BY a.account_id, a.balance
                HAVING ABS(a.balance - (
                    COALESCE(SUM(CASE WHEN t.transaction_type = 'CREDIT' THEN t.amount ELSE 0 END), 0) -
                    COALESCE(SUM(CASE WHEN t.transaction_type = 'DEBIT' THEN t.amount ELSE 0 END), 0)
                )) > 0.01
            ) discrepancies
        """
        
        cursor = self.conn.cursor()
        cursor.execute(balance_check_sql)
        discrepancy_count = cursor.fetchone()[0]
        
        results['balance_reconciliation'] = {
            'status': 'PASS' if discrepancy_count == 0 else 'FAIL',
            'discrepancy_count': discrepancy_count
        }
        
        return results
    
    def calculate_quality_scores(self):
        """Calculate and store quality scores."""
        cursor = self.conn.cursor()
        
        # Get test results for today
        cursor.execute("""
            SELECT 
                table_name,
                rule_type,
                status,
                pass_rate
            FROM dq.test_results
            WHERE execution_id = %s
        """, (self.execution_id,))
        
        results = cursor.fetchall()
        
        # Calculate scores by dimension
        completeness_scores = []
        uniqueness_scores = []
        freshness_scores = []
        
        for table_name, rule_type, status, pass_rate in results:
            if rule_type == 'completeness':
                completeness_scores.append(pass_rate)
            elif rule_type == 'uniqueness':
                uniqueness_scores.append(pass_rate)
            elif rule_type == 'freshness':
                freshness_scores.append(100 if status == 'PASS' else 0)
        
        # Calculate averages
        avg_completeness = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 100
        avg_uniqueness = sum(uniqueness_scores) / len(uniqueness_scores) if uniqueness_scores else 100
        avg_freshness = sum(freshness_scores) / len(freshness_scores) if freshness_scores else 100
        
        # Overall score
        overall_score = (avg_completeness + avg_uniqueness + avg_freshness) / 3
        
        # Determine grade
        if overall_score >= 99:
            grade = 'EXCELLENT'
        elif overall_score >= 95:
            grade = 'GOOD'
        elif overall_score >= 90:
            grade = 'POOR'
        else:
            grade = 'CRITICAL'
        
        # Store scores
        insert_sql = """
            INSERT INTO dq.quality_scores 
            (score_date, score_hour, completeness_score, uniqueness_score, 
             timeliness_score, overall_score, grade, total_checks, passed_checks, failed_checks)
            VALUES (CURRENT_DATE, EXTRACT(HOUR FROM CURRENT_TIMESTAMP), %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        total_checks = len(results)
        passed_checks = sum(1 for r in results if r[2] == 'PASS')
        failed_checks = total_checks - passed_checks
        
        cursor.execute(insert_sql, (
            avg_completeness, avg_uniqueness, avg_freshness,
            overall_score, grade, total_checks, passed_checks, failed_checks
        ))
        
        self.conn.commit()
        
        return {
            'overall_score': overall_score,
            'grade': grade,
            'completeness': avg_completeness,
            'uniqueness': avg_uniqueness,
            'freshness': avg_freshness
        }
    
    def run_full_pipeline(self):
        """Run the complete data quality pipeline."""
        print(f"\n{'='*60}")
        print(f"DATA QUALITY PIPELINE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Execution ID: {self.execution_id}")
        print(f"{'='*60}\n")
        
        # Create execution summary
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO dq.execution_summary 
            (execution_id, started_at, status, triggered_by, trigger_type)
            VALUES (%s, %s, 'RUNNING', %s, 'MANUAL')
        """, (self.execution_id, datetime.now(), 'data_quality_pipeline'))
        self.conn.commit()
        
        try:
            # Run all checks
            print("Running staging table checks...")
            staging_results = self.run_all_staging_checks()
            
            print("\nRunning gold table checks...")
            gold_results = self.run_all_gold_checks()
            
            print("\nRunning business rule checks...")
            business_results = self.run_business_rule_checks()
            
            # Calculate quality scores
            print("\nCalculating quality scores...")
            scores = self.calculate_quality_scores()
            
            # Update execution summary
            total_checks = sum(len(v) for v in staging_results.values()) + \
                          sum(len(v) for v in gold_results.values()) + \
                          len(business_results)
            
            passed_checks = sum(1 for v in staging_results.values() 
                              for r in v.values() 
                              if isinstance(r, dict) and r.get('status') == 'PASS')
            passed_checks += sum(1 for v in gold_results.values() 
                               for r in v.values() 
                               if isinstance(r, dict) and r.get('status') == 'PASS')
            passed_checks += sum(1 for v in business_results.values() 
                               if isinstance(v, dict) and v.get('status') == 'PASS')
            
            cursor.execute("""
                UPDATE dq.execution_summary 
                SET completed_at = %s, status = 'COMPLETED',
                    total_rules = %s, passed_rules = %s, failed_rules = %s,
                    overall_score = %s
                WHERE execution_id = %s
            """, (datetime.now(), total_checks, passed_checks, 
                  total_checks - passed_checks, scores['overall_score'],
                  self.execution_id))
            self.conn.commit()
            
            # Print summary
            print(f"\n{'='*60}")
            print("PIPELINE COMPLETE")
            print(f"{'='*60}")
            print(f"Overall Quality Score: {scores['overall_score']:.2f}%")
            print(f"Grade: {scores['grade']}")
            print(f"Completeness: {scores['completeness']:.2f}%")
            print(f"Uniqueness: {scores['uniqueness']:.2f}%")
            print(f"Freshness: {scores['freshness']:.2f}%")
            print(f"Total Checks: {total_checks}")
            print(f"Passed: {passed_checks}")
            print(f"Failed: {total_checks - passed_checks}")
            print(f"{'='*60}\n")
            
            return scores
            
        except Exception as e:
            # Update execution summary with error
            cursor.execute("""
                UPDATE dq.execution_summary 
                SET completed_at = %s, status = 'FAILED'
                WHERE execution_id = %s
            """, (datetime.now(), self.execution_id))
            self.conn.commit()
            raise e


# =====================================================
# MAIN ENTRY POINT
# =====================================================
if __name__ == "__main__":
    pipeline = DataQualityPipeline()
    
    try:
        results = pipeline.run_full_pipeline()
        
        # Exit with appropriate code
        if results['overall_score'] >= 95:
            sys.exit(0)  # Success
        else:
            sys.exit(1)  # Quality below threshold
            
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        sys.exit(2)  # Pipeline error
        
    finally:
        pipeline.close()
