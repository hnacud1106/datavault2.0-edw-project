import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from utils.excel_filter_processor import ExcelFilterProcessor
from utils.clickhouse_utils import ClickHouseManager
from utils.deal_detector import DealDetector
from dotenv import load_dotenv
import subprocess

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'initial_load_pipeline',
    default_args=default_args,
    description='Initial Load Pipeline with Dynamic Deal Detection',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['initial-load', 'data-vault', 'clickhouse'],
)


def detect_new_deals(**context) -> str:
    try:
        logger.info("Starting new deal detection process")

        ch_manager = ClickHouseManager()
        excel_processor = ExcelFilterProcessor()
        deal_detector = DealDetector(ch_manager, excel_processor)
        existing_deals = deal_detector.get_existing_deals()
        logger.info(f"Found {len(existing_deals)} existing deals: {existing_deals}")
        excel_deals = deal_detector.get_excel_deal_sheets()
        logger.info(f"Found {len(excel_deals)} deal sheets in Excel: {excel_deals}")
        new_deals = deal_detector.find_new_deals(existing_deals, excel_deals)

        if new_deals:
            logger.info(f"Detected {len(new_deals)} new deals: {new_deals}")
            context['task_instance'].xcom_push(key='new_deals', value=new_deals)
            return 'process_new_deals'
        else:
            logger.info("Không phát sinh deal mới")
            return 'no_new_deals'

    except Exception as e:
        logger.error(f"Error in deal detection: {str(e)}")
        raise


def process_initial_load_for_new_deals(**context) -> None:
    try:
        new_deals = context['task_instance'].xcom_pull(key='new_deals')

        if not new_deals:
            logger.info("No new deals to process")
            return

        logger.info(f"Processing initial load for deals: {new_deals}")

        ch_manager = ClickHouseManager()
        excel_processor = ExcelFilterProcessor()

        for deal_name in new_deals:
            logger.info(f"Processing initial load for deal: {deal_name}")

            filter_conditions = excel_processor.get_deal_filters(deal_name)
            logger.info(f"Filter conditions for {deal_name}: {filter_conditions}")

            filtered_data = ch_manager.extract_filtered_data(
                deal_name=deal_name,
                filter_conditions=filter_conditions
            )

            logger.info(f"Extracted {len(filtered_data)} records for deal {deal_name}")
            if filtered_data:
                ch_manager.parallel_insert_to_staging(
                    data=filtered_data,
                    deal_name=deal_name
                )
                logger.info(f"Successfully loaded data for deal {deal_name}")
    except Exception as e:
        logger.error(f"Error in initial load processing: {str(e)}")
        raise


def run_dbt_models(**context) -> None:
    try:
        logger.info("Starting dbt transformation process")
        new_deals = context['task_instance'].xcom_pull(key='new_deals')
        if new_deals:
            deal_var = ','.join(new_deals)
            logger.info(f"Running dbt models for deals: {deal_var}")

            os.environ['DBT_DEALS_FILTER'] = deal_var

        dbt_commands = [
            "dbt debug",
            "dbt run --select staging",
            "dbt run --select vault.hubs",
            "dbt run --select vault.links",
            "dbt run --select vault.satellites",
            "dbt test"
        ]
        dbt_dir = "/opt/airflow/dbt"
        for cmd in dbt_commands:
            logger.info(f"Executing: {cmd}")

            result = subprocess.run(
                cmd.split(),
                cwd=dbt_dir,
                capture_output=True,
                text=True,
                env=os.environ.copy()
            )

            if result.stdout:
                logger.info(f"STDOUT:\n{result.stdout}")
            if result.stderr:
                logger.info(f"STDERR:\n{result.stderr}")

            if result.returncode != 0:
                logger.error(f"dbt command failed: {cmd}")
                logger.error(f"Return code: {result.returncode}")
                logger.error(f"Error output: {result.stderr}")
                if cmd == "dbt debug":
                    logger.warning("dbt debug failed, but continuing...")
                else:
                    raise subprocess.CalledProcessError(result.returncode, cmd, result.stderr)
            else:
                logger.info(f"dbt command succeeded: {cmd}")

        logger.info("dbt transformation completed successfully")

    except Exception as e:
        logger.error(f"Error in dbt processing: {str(e)}")
        raise


detect_deals_task = BranchPythonOperator(
    task_id='detect_new_deals',
    python_callable=detect_new_deals,
    dag=dag,
)

process_load_task = PythonOperator(
    task_id='process_new_deals',
    python_callable=process_initial_load_for_new_deals,
    dag=dag,
)

no_action_task = BashOperator(
    task_id='no_new_deals',
    bash_command='echo "No new deals detected - pipeline completed"',
    dag=dag,
)

dbt_transform_task = PythonOperator(
    task_id='run_dbt_transformation',
    python_callable=run_dbt_models,
    dag=dag,
)

detect_deals_task >> [process_load_task, no_action_task]
process_load_task >> dbt_transform_task
