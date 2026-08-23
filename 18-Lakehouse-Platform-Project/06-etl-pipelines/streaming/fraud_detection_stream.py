"""
Real-time Fraud Detection Streaming Pipeline
Purpose: Detect fraudulent transactions in real-time using Kafka Streams
Tool:    Apache Flink / Kafka Streams
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col, lit
import json
import logging

logger = logging.getLogger(__name__)

def create_fraud_detection_stream():
    """Create real-time fraud detection stream processing pipeline"""
    
    # Initialize Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    
    # Create Table environment
    t_env = StreamTableEnvironment.create(env)
    
    # Define Kafka source for card transactions
    t_env.execute_sql("""
        CREATE TABLE card_transactions (
            txn_id STRING,
            card_number STRING,
            amount DOUBLE,
            merchant_name STRING,
            merchant_category STRING,
            txn_timestamp TIMESTAMP(3),
            WATERMARK FOR txn_timestamp AS txn_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'card_transactions',
            'properties.bootstrap.servers' = 'kafka-1:9092',
            'properties.group.id' = 'fraud-detection',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)
    
    # Define Kafka sink for fraud alerts
    t_env.execute_sql("""
        CREATE TABLE fraud_alerts (
            txn_id STRING,
            card_number STRING,
            amount DOUBLE,
            merchant_name STRING,
            fraud_score DOUBLE,
            alert_level STRING,
            alert_timestamp TIMESTAMP(3),
            PRIMARY KEY (txn_id) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'fraud-alerts',
            'properties.bootstrap.servers' = 'kafka-1:9092',
            'format' = 'json',
            'sink.partitioner' = 'round-robin'
        )
    """)
    
    # Fraud detection rules using SQL
    fraud_detection_query = """
        INSERT INTO fraud_alerts
        SELECT 
            txn_id,
            card_number,
            amount,
            merchant_name,
            -- Calculate fraud score based on rules
            (
                -- High amount rule (30 points)
                CASE WHEN amount > 50000000 THEN 30 ELSE 0 END +
                -- Velocity rule (40 points)
                CASE WHEN (
                    SELECT COUNT(*) 
                    FROM card_transactions ct2 
                    WHERE ct2.card_number = ct.card_number 
                      AND ct2.txn_timestamp BETWEEN 
                          ct.txn_timestamp - INTERVAL '1' HOUR AND 
                          ct.txn_timestamp
                ) > 5 THEN 40 ELSE 0 END +
                -- Unusual time rule (20 points)
                CASE WHEN HOUR(ct.txn_timestamp) BETWEEN 0 AND 5 THEN 20 ELSE 0 END +
                -- Weekend rule (10 points)
                CASE WHEN DAYOFWEEK(ct.txn_timestamp) IN (1, 7) THEN 10 ELSE 0 END
            ) AS fraud_score,
            
            -- Determine alert level
            CASE 
                WHEN (
                    CASE WHEN amount > 50000000 THEN 30 ELSE 0 END +
                    CASE WHEN (
                        SELECT COUNT(*) 
                        FROM card_transactions ct2 
                        WHERE ct2.card_number = ct.card_number 
                          AND ct2.txn_timestamp BETWEEN 
                              ct.txn_timestamp - INTERVAL '1' HOUR AND 
                              ct.txn_timestamp
                    ) > 5 THEN 40 ELSE 0 END +
                    CASE WHEN HOUR(ct.txn_timestamp) BETWEEN 0 AND 5 THEN 20 ELSE 0 END +
                    CASE WHEN DAYOFWEEK(ct.txn_timestamp) IN (1, 7) THEN 10 ELSE 0 END
                ) >= 80 THEN 'CRITICAL'
                WHEN (
                    CASE WHEN amount > 50000000 THEN 30 ELSE 0 END +
                    CASE WHEN (
                        SELECT COUNT(*) 
                        FROM card_transactions ct2 
                        WHERE ct2.card_number = ct.card_number 
                          AND ct2.txn_timestamp BETWEEN 
                              ct.txn_timestamp - INTERVAL '1' HOUR AND 
                              ct.txn_timestamp
                    ) > 5 THEN 40 ELSE 0 END +
                    CASE WHEN HOUR(ct.txn_timestamp) BETWEEN 0 AND 5 THEN 20 ELSE 0 END +
                    CASE WHEN DAYOFWEEK(ct.txn_timestamp) IN (1, 7) THEN 10 ELSE 0 END
                ) >= 50 THEN 'HIGH'
                WHEN (
                    CASE WHEN amount > 50000000 THEN 30 ELSE 0 END +
                    CASE WHEN (
                        SELECT COUNT(*) 
                        FROM card_transactions ct2 
                        WHERE ct2.card_number = ct.card_number 
                          AND ct2.txn_timestamp BETWEEN 
                              ct.txn_timestamp - INTERVAL '1' HOUR AND 
                              ct.txn_timestamp
                    ) > 5 THEN 40 ELSE 0 END +
                    CASE WHEN HOUR(ct.txn_timestamp) BETWEEN 0 AND 5 THEN 20 ELSE 0 END +
                    CASE WHEN DAYOFWEEK(ct.txn_timestamp) IN (1, 7) THEN 10 ELSE 0 END
                ) >= 30 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS alert_level,
            
            CURRENT_TIMESTAMP AS alert_timestamp
            
        FROM card_transactions ct
        WHERE (
            -- Only process transactions that trigger at least one rule
            amount > 50000000 OR
            HOUR(ct.txn_timestamp) BETWEEN 0 AND 5 OR
            DAYOFWEEK(ct.txn_timestamp) IN (1, 7) OR
            EXISTS (
                SELECT 1 
                FROM card_transactions ct2 
                WHERE ct2.card_number = ct.card_number 
                  AND ct2.txn_timestamp BETWEEN 
                      ct.txn_timestamp - INTERVAL '1' HOUR AND 
                      ct.txn_timestamp
                HAVING COUNT(*) > 5
            )
        )
    """
    
    # Execute fraud detection
    t_env.execute_sql(fraud_detection_query)
    
    return env

def create_real_time_dashboard_stream():
    """Create real-time dashboard aggregation stream"""
    
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)
    
    # Define Kafka source
    t_env.execute_sql("""
        CREATE TABLE transactions_stream (
            txn_id STRING,
            account_id STRING,
            amount DOUBLE,
            txn_type STRING,
            channel STRING,
            txn_timestamp TIMESTAMP(3),
            WATERMARK FOR txn_timestamp AS txn_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'transactions',
            'properties.bootstrap.servers' = 'kafka-1:9092',
            'properties.group.id' = 'realtime-dashboard',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)
    
    # Define sink for real-time aggregations
    t_env.execute_sql("""
        CREATE TABLE realtime_metrics (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            channel STRING,
            txn_type STRING,
            transaction_count BIGINT,
            total_amount DOUBLE,
            avg_amount DOUBLE,
            unique_accounts BIGINT,
            PRIMARY KEY (window_start, channel, txn_type) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'realtime-metrics',
            'properties.bootstrap.servers' = 'kafka-1:9092',
            'format' = 'json'
        )
    """)
    
    # Real-time aggregation query (1-minute windows)
    aggregation_query = """
        INSERT INTO realtime_metrics
        SELECT 
            TUMBLE_START(txn_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(txn_timestamp, INTERVAL '1' MINUTE) AS window_end,
            channel,
            txn_type,
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount,
            AVG(amount) AS avg_amount,
            COUNT(DISTINCT account_id) AS unique_accounts
        FROM transactions_stream
        GROUP BY 
            TUMBLE(txn_timestamp, INTERVAL '1' MINUTE),
            channel,
            txn_type
    """
    
    t_env.execute_sql(aggregation_query)
    
    return env

if __name__ == "__main__":
    # Run fraud detection pipeline
    env = create_fraud_detection_stream()
    env.execute("Real-time Fraud Detection Pipeline")
