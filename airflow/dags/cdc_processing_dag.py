import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from utils.excel_filter_processor import ExcelFilterProcessor
from utils.clickhouse_utils import ClickHouseManager
from utils.data_integrity_manager import DataIntegrityManager
from utils.kafka_sensor import KafkaMessageSensor, KafkaHealthChecker
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG Configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'cdc_processing_pipeline',
    default_args=default_args,
    description='CDC Processing Pipeline with Kafka and Dynamic Filtering',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=['cdc', 'kafka', 'real-time'],
)


def check_kafka_connectivity(**context) -> None:
    try:
        logger.info("Checking Kafka connectivity and topic health")

        health_checker = KafkaHealthChecker()

        topic_name = 'edw_cdc.product_changes_log'
        topic_exists = health_checker.check_topic_exists(topic_name)

        if not topic_exists:
            logger.error(f"CDC topic {topic_name} does not exist")
            raise Exception(f"Required topic {topic_name} not found")
        topic_info = health_checker.get_topic_info(topic_name)
        if 'error' in topic_info:
            raise Exception(f"Error getting topic info: {topic_info['error']}")

        logger.info(f"Topic info: {topic_info}")

        context['task_instance'].xcom_push(key='topic_info', value=topic_info)

    except Exception as e:
        logger.error(f"Error in Kafka connectivity check: {str(e)}")
        raise


def process_cdc_messages(**context) -> None:
    try:
        logger.info("Starting CDC message processing")
        topic_info = context['task_instance'].xcom_pull(key='topic_info')

        if not topic_info or topic_info.get('total_messages', 0) == 0:
            logger.info("No messages to process")
            return

        excel_processor = ExcelFilterProcessor()
        ch_manager = ClickHouseManager()
        integrity_manager = DataIntegrityManager(ch_manager)

        consumer = KafkaConsumer(
            'edw_cdc.product_changes_log',
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092').split(','),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            group_id='cdc_processor_group',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=int(os.getenv('BATCH_SIZE', 1000)),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )

        try:
            batch_data = []
            processed_count = 0
            batch_size = int(os.getenv('BATCH_SIZE', 1000))
            processing_timeout = 300
            start_time = datetime.now()

            for message in consumer:
                if (datetime.now() - start_time).seconds > processing_timeout:
                    logger.info("Processing timeout reached, finishing current batch")
                    break

                if message.value is None:
                    continue

                cdc_record = message.value
                logger.debug(f"Processing CDC record: {cdc_record}")

                try:
                    processed_records = parse_cdc_record(cdc_record, excel_processor, integrity_manager)

                    if processed_records:
                        batch_data.extend(processed_records)
                        processed_count += 1
                    if len(batch_data) >= batch_size:
                        success = process_batch(ch_manager, integrity_manager, batch_data)
                        if success:
                            consumer.commit()
                            logger.info(f"Processed and committed batch of {len(batch_data)} records")
                        batch_data = []

                except Exception as e:
                    logger.error(f"Error processing CDC record: {str(e)}")
                    continue

        except KafkaError as e:
            logger.error(f"Kafka error during message consumption: {str(e)}")
            raise

        finally:
            if batch_data:
                success = process_batch(ch_manager, integrity_manager, batch_data)
                if success:
                    consumer.commit()
                    logger.info(f"Processed final batch of {len(batch_data)} records")

            consumer.close()

        logger.info(f"CDC processing completed. Processed {processed_count} messages")

    except Exception as e:
        logger.error(f"Error in CDC processing: {str(e)}")
        raise


def parse_cdc_record(cdc_record: Dict[str, Any], excel_processor: ExcelFilterProcessor,
                     integrity_manager: DataIntegrityManager) -> List[Dict[str, Any]]:
    try:
        operation = cdc_record.get('op', 'c')

        if operation == 'd':
            before_data = cdc_record.get('before', {})
            if before_data and before_data.get('product_base_id'):
                return [create_delete_record(before_data)]

        elif operation in ['c', 'u', 'r']:
            after_data = cdc_record.get('after', {})
            if not after_data:
                return []

            if not integrity_manager.is_valid_record(after_data):
                logger.warning(f"Invalid record format: {after_data}")
                return []

            if integrity_manager.is_duplicate_record(after_data):
                logger.debug(f"Duplicate record detected, skipping: {after_data.get('product_base_id')}")
                return []

            matching_deals = excel_processor.apply_filters_to_record(after_data)

            if matching_deals:
                logger.info(f"Record {after_data.get('product_base_id')} matches deals: {matching_deals}")

                processed_records = []
                for deal_name in matching_deals:
                    processed_record = create_change_record(after_data, deal_name, operation)
                    processed_records.append(processed_record)

                return processed_records
            else:
                logger.debug(f"Record {after_data.get('product_base_id')} does not match any deal filters")
                return []

        return []

    except Exception as e:
        logger.error(f"Error parsing CDC record: {str(e)}")
        return []


def create_change_record(record: Dict[str, Any], deal_name: str, operation: str) -> Dict[str, Any]:
    return {
        'product_base_id': record.get('product_base_id'),
        'product_name': record.get('product_name'),
        'product_description': record.get('product_description'),
        'category': record.get('category'),
        'brand': record.get('brand'),
        'product_rating': record.get('product_rating'),
        'product_image': record.get('product_image'),
        'shop_name': record.get('shop_name'),
        'shop_link': record.get('shop_link'),
        'revenue': record.get('revenue'),
        'month': record.get('month'),
        'price': record.get('price'),
        'quantity': record.get('quantity'),
        'deal_name': deal_name,
        'operation_type': operation,
        'load_date': datetime.now(),
        'record_source': 'cdc_kafka',
        'change_timestamp': record.get('change_timestamp', datetime.now()),
        'product_hash_key': hash(str(record.get('product_base_id'))),
        'deal_hash_key': hash(str(deal_name)),
        'product_deal_hash_key': hash(f"{record.get('product_base_id')}||{deal_name}"),
        'product_hash_diff': hash(
            f"{record.get('product_name', '')}||{record.get('price', 0)}||{record.get('revenue', 0)}")
    }


def create_delete_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'product_base_id': record.get('product_base_id'),
        'operation_type': 'd',
        'load_date': datetime.now(),
        'record_source': 'cdc_kafka_delete',
        'change_timestamp': record.get('change_timestamp', datetime.now())
    }


def process_batch(ch_manager: ClickHouseManager, integrity_manager: DataIntegrityManager,
                  batch_data: List[Dict[str, Any]]) -> bool:
    try:
        logger.info(f"Processing batch of {len(batch_data)} records")

        if not batch_data:
            return True
        inserts_updates = [r for r in batch_data if r.get('operation_type') in ['c', 'u', 'r']]
        deletes = [r for r in batch_data if r.get('operation_type') == 'd']
        if inserts_updates:
            deal_groups = {}
            for record in inserts_updates:
                deal_name = record.get('deal_name', 'unknown')
                if deal_name not in deal_groups:
                    deal_groups[deal_name] = []
                deal_groups[deal_name].append(record)

            for deal_name, records in deal_groups.items():
                logger.info(f"Processing {len(records)} records for deal {deal_name}")
                validated_records = integrity_manager.validate_batch(records)
                if validated_records:
                    ch_manager.parallel_insert_to_staging(validated_records, deal_name)
                    integrity_manager.track_processed_records(validated_records)

        if deletes:
            logger.info(f"Processing {len(deletes)} delete records")
            integrity_manager.process_delete_records(deletes)

        return True

    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
        return False


def trigger_dbt_incremental(**context) -> None:
    try:
        logger.info("Starting dbt incremental processing for CDC data")
        dbt_commands = [
            "dbt run --models staging",
            "dbt run --models vault.hubs",
            "dbt run --models vault.links",
            "dbt run --models vault.satellites"
        ]

        for cmd in dbt_commands:
            logger.info(f"Executing: {cmd}")
            result = os.system(f"cd /opt/airflow/dbt && {cmd}")
            if result != 0:
                logger.warning(f"dbt command completed with warnings: {cmd}")

        logger.info("dbt incremental processing completed successfully")

    except Exception as e:
        logger.error(f"Error in dbt incremental processing: {str(e)}")
        raise


kafka_health_check_task = PythonOperator(
    task_id='check_kafka_connectivity',
    python_callable=check_kafka_connectivity,
    dag=dag,
)

kafka_sensor = KafkaMessageSensor(
    task_id='check_kafka_messages',
    kafka_topic='edw_cdc.product_changes_log',
    poke_interval=30,
    timeout=600,
    dag=dag,
)

process_cdc_task = PythonOperator(
    task_id='process_cdc_messages',
    python_callable=process_cdc_messages,
    dag=dag,
)

dbt_incremental_task = PythonOperator(
    task_id='run_dbt_incremental',
    python_callable=trigger_dbt_incremental,
    dag=dag,
)
kafka_health_check_task >> kafka_sensor >> process_cdc_task >> dbt_incremental_task
