"""
Real-time Metrics Streaming Pipeline
Purpose: Aggregate transaction metrics in real-time for dashboards
Tool:    Apache Flink
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
import logging

logger = logging.getLogger(__name__)

def create_transaction_metrics_stream():
    """Create real-time transaction metrics aggregation"""
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    
    t_env = StreamTableEnvironment.create(env)
    
    # Source: Card transactions from Kafka
    t_env.execute_sql("""
        CREATE TABLE card_transactions_source (
            txn_id STRING,
            card_number STRING,
            customer_id STRING,
            amount DOUBLE,
            merchant_name STRING,
            merchant_category STRING,
            channel STRING,
            txn_timestamp TIMESTAMP(3),
            WATERMARK FOR txn_timestamp AS txn_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'card_transactions',
            'properties.bootstrap.servers' = 'kafka-1:9092',
            'properties.group.id' = 'metrics-aggregation',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)
    
    # Sink: Real-time metrics to Kafka
    t_env.execute_sql("""
        CREATE TABLE realtime_card_metrics (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            merchant_category STRING,
            transaction_count BIGINT,
            total_amount DOUBLE,
            avg_amount DOUBLE,
            max_amount DOUBLE,
            unique_cards BIGINT,
            high_value_count BIGINT,
            PRIMARY KEY (window_start, merchant_category) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'realtime-card-metrics',
            'properties.bootstrap.servers' = 'kafka-1:9092',
            'format' = 'json'
        )
    """)
    
    # Aggregation query (5-minute tumbling windows)
    t_env.execute_sql("""
        INSERT INTO realtime_card_metrics
        SELECT 
            TUMBLE_START(txn_timestamp, INTERVAL '5' MINUTE) AS window_start,
            TUMBLE_END(txn_timestamp, INTERVAL '5' MINUTE) AS window_end,
            merchant_category,
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount,
            AVG(amount) AS avg_amount,
            MAX(amount) AS max_amount,
            COUNT(DISTINCT card_number) AS unique_cards,
            SUM(CASE WHEN amount > 10000000 THEN 1 ELSE 0 END) AS high_value_count
        FROM card_transactions_source
        GROUP BY 
            TUMBLE(txn_timestamp, INTERVAL '5' MINUTE),
            merchant_category
    """)
    
    return env

def create_channel_metrics_stream():
    """Create real-time channel performance metrics"""
    
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)
    
    # Source: All transactions
    t_env.execute_sql("""
        CREATE TABLE transactions_source (
            txn_id STRING,
            account_id STRING,
            amount DOUBLE,
            channel STRING,
            status STRING,
            txn_timestamp TIMESTAMP(3),
            WATERMARK FOR txn_timestamp AS txn_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'transactions',
            'properties.bootstrap.servers' = 'kafka-1:9092',
            'properties.group.id' = 'channel-metrics',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)
    
    # Sink: Channel metrics
    t_env.execute_sql("""
        CREATE TABLE channel_metrics (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            channel STRING,
            total_transactions BIGINT,
            successful_transactions BIGINT,
            failed_transactions BIGINT,
            success_rate DOUBLE,
            total_amount DOUBLE,
            avg_amount DOUBLE,
            PRIMARY KEY (window_start, channel) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'channel-metrics',
            'properties.bootstrap.servers' = 'kafka-1:9092',
            'format' = 'json'
        )
    """)
    
    # Channel performance aggregation
    t_env.execute_sql("""
        INSERT INTO channel_metrics
        SELECT 
            TUMBLE_START(txn_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(txn_timestamp, INTERVAL '1' MINUTE) AS window_end,
            channel,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_transactions,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_transactions,
            ROUND(
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
            ) AS success_rate,
            SUM(amount) AS total_amount,
            AVG(amount) AS avg_amount
        FROM transactions_source
        GROUP BY 
            TUMBLE(txn_timestamp, INTERVAL '1' MINUTE),
            channel
    """)
    
    return env

if __name__ == "__main__":
    # Run all streaming pipelines
    env1 = create_transaction_metrics_stream()
    env2 = create_channel_metrics_stream()
    
    env1.execute("Real-time Card Metrics Pipeline")
    env2.execute("Real-time Channel Metrics Pipeline")
