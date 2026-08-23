"""
CDC Kafka Ingestion DAG
Purpose: Consume CDC events from Kafka and load to Bronze layer
Schedule: Real-time (triggered by Kafka messages)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import json
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    dag_id='cdc_kafka_ingestion',
    default_args=default_args,
    description='Consume CDC events from Kafka to Bronze layer',
    schedule_interval='*/5 * * * *',  # Every 5 minutes (micro-batch)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['cdc', 'kafka', 'real-time', 'banking'],
) as dag:

    def consume_core_banking_cdc(**context):
        """Consume Core Banking CDC events from Kafka"""
        from kafka import KafkaConsumer
        import pandas as pd
        from datetime import datetime
        
        # Kafka configuration
        consumer = KafkaConsumer(
            'accounts',
            'customers',
            'transactions',
            bootstrap_servers=['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092'],
            group_id='airflow-cdc-core-banking',
            auto_offset_reset='latest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Consume messages (micro-batch)
        messages = []
        for message in consumer:
            messages.append({
                'topic': message.topic,
                'key': message.key.decode('utf-8') if message.key else None,
                'value': message.value,
                'timestamp': datetime.fromtimestamp(message.timestamp / 1000),
                'partition': message.partition,
                'offset': message.offset
            })
            
            # Process in batches of 1000
            if len(messages) >= 1000:
                break
        
        consumer.close()
        
        if not messages:
            logger.info("No CDC messages found")
            return {'messages_processed': 0}
        
        # Convert to DataFrame
        df = pd.DataFrame(messages)
        
        # Process by topic
        for topic in df['topic'].unique():
            topic_df = df[df['topic'] == topic]
            
            # Write to Bronze layer
            output_path = f"s3a://banking-lake/bronze/{topic}/cdc_date={context['ds']}"
            topic_df.to_parquet(output_path, index=False)
            
            logger.info(f"Processed {len(topic_df)} messages for topic {topic}")
        
        return {
            'messages_processed': len(messages),
            'topics': df['topic'].unique().tolist()
        }

    def consume_cards_cdc(**context):
        """Consume Credit Cards CDC events from Kafka"""
        from kafka import KafkaConsumer
        import pandas as pd
        from datetime import datetime
        
        consumer = KafkaConsumer(
            'cards',
            'card_transactions',
            bootstrap_servers=['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092'],
            group_id='airflow-cdc-cards',
            auto_offset_reset='latest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        messages = []
        for message in consumer:
            messages.append({
                'topic': message.topic,
                'key': message.key.decode('utf-8') if message.key else None,
                'value': message.value,
                'timestamp': datetime.fromtimestamp(message.timestamp / 1000),
                'partition': message.partition,
                'offset': message.offset
            })
            
            if len(messages) >= 1000:
                break
        
        consumer.close()
        
        if not messages:
            logger.info("No CDC messages found for cards")
            return {'messages_processed': 0}
        
        df = pd.DataFrame(messages)
        
        for topic in df['topic'].unique():
            topic_df = df[df['topic'] == topic]
            output_path = f"s3a://banking-lake/bronze/{topic}/cdc_date={context['ds']}"
            topic_df.to_parquet(output_path, index=False)
        
        return {'messages_processed': len(messages)}

    def consume_loans_cdc(**context):
        """Consume Loans CDC events from Kafka"""
        from kafka import KafkaConsumer
        import pandas as pd
        from datetime import datetime
        
        consumer = KafkaConsumer(
            'loan_accounts',
            'loan_payments',
            bootstrap_servers=['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092'],
            group_id='airflow-cdc-loans',
            auto_offset_reset='latest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        messages = []
        for message in consumer:
            messages.append({
                'topic': message.topic,
                'key': message.key.decode('utf-8') if message.key else None,
                'value': message.value,
                'timestamp': datetime.fromtimestamp(message.timestamp / 1000),
                'partition': message.partition,
                'offset': message.offset
            })
            
            if len(messages) >= 1000:
                break
        
        consumer.close()
        
        if not messages:
            logger.info("No CDC messages found for loans")
            return {'messages_processed': 0}
        
        df = pd.DataFrame(messages)
        
        for topic in df['topic'].unique():
            topic_df = df[df['topic'] == topic]
            output_path = f"s3a://banking-lake/bronze/{topic}/cdc_date={context['ds']}"
            topic_df.to_parquet(output_path, index=False)
        
        return {'messages_processed': len(messages)}

    def validate_cdc_data(**context):
        """Validate CDC data quality"""
        import pandas as pd
        
        validation_results = {}
        
        # Check each CDC topic
        topics = ['accounts', 'customers', 'transactions', 'cards', 
                  'card_transactions', 'loan_accounts', 'loan_payments']
        
        for topic in topics:
            try:
                path = f"s3a://banking-lake/bronze/{topic}/cdc_date={context['ds']}"
                df = pd.read_parquet(path)
                
                validation_results[topic] = {
                    'row_count': len(df),
                    'status': 'SUCCESS' if len(df) > 0 else 'EMPTY',
                    'columns': list(df.columns)
                }
            except Exception as e:
                validation_results[topic] = {
                    'row_count': 0,
                    'status': 'NO_DATA',
                    'error': str(e)
                }
        
        return validation_results

    # Task definitions
    with TaskGroup('consume_cdc_events') as cdc_group:
        consume_core = PythonOperator(
            task_id='consume_core_banking_cdc',
            python_callable=consume_core_banking_cdc,
        )
        
        consume_cards = PythonOperator(
            task_id='consume_cards_cdc',
            python_callable=consume_cards_cdc,
        )
        
        consume_loans = PythonOperator(
            task_id='consume_loans_cdc',
            python_callable=consume_loans_cdc,
        )
        
        [consume_core, consume_cards, consume_loans]

    validate = PythonOperator(
        task_id='validate_cdc_data',
        python_callable=validate_cdc_data,
    )

    cdc_group >> validate
