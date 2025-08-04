import clickhouse_connect
import pandas as pd
import logging
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
import os
from contextlib import contextmanager
from queue import Queue
import time
from dotenv import load_dotenv
import hashlib
logger = logging.getLogger(__name__)
load_dotenv()

class ClickHouseManager:
    def __init__(self):
        self.source_config = {
            'host': os.getenv('CLICKHOUSE_SOURCE_HOST'),
            'port': int(os.getenv('CLICKHOUSE_SOURCE_PORT')),
            'username': os.getenv('CLICKHOUSE_SOURCE_USER'),
            'password': os.getenv('CLICKHOUSE_SOURCE_PASSWORD'),
            'database': os.getenv('CLICKHOUSE_SOURCE_DATABASE')
        }

        self.edw_config = {
            'host': os.getenv('CLICKHOUSE_EDW_HOST'),
            'port': int(os.getenv('CLICKHOUSE_EDW_PORT')),
            'username': os.getenv('CLICKHOUSE_EDW_USER'),
            'password': os.getenv('CLICKHOUSE_EDW_PASSWORD'),
            'database': os.getenv('CLICKHOUSE_EDW_DATABASE')
        }

        self.max_workers = int(os.getenv('MAX_WORKERS', 4))
        self.batch_size = int(os.getenv('BATCH_SIZE', 10000))

        self._source_clients = Queue(maxsize=self.max_workers)
        self._edw_clients = Queue(maxsize=self.max_workers)
        self._init_connection_pools()

    def _init_connection_pools(self) -> None:
        try:
            for _ in range(self.max_workers):
                client = clickhouse_connect.get_client(**self.source_config)
                self._source_clients.put(client)

            for _ in range(self.max_workers):
                client = clickhouse_connect.get_client(**self.edw_config)
                self._edw_clients.put(client)

            logger.info(f"Initialized connection pools with {self.max_workers} connections each")

        except Exception as e:
            logger.error(f"Error initializing connection pools: {str(e)}")
            raise

    @contextmanager
    def get_source_client(self):
        client = self._source_clients.get()
        try:
            yield client
        finally:
            self._source_clients.put(client)

    @contextmanager
    def get_edw_client(self):
        client = self._edw_clients.get()
        try:
            yield client
        finally:
            self._edw_clients.put(client)

    def extract_filtered_data(self, deal_name: str, filter_conditions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        try:
            logger.info(f"Extracting data for deal: {deal_name}")
            where_clause = self._build_where_clause(filter_conditions)
            query = f"""
            SELECT 
                product_base_id,
                product_name,
                product_description,
                category,
                brand,
                product_rating,
                product_image,
                shop_name,
                shop_link,
                revenue,
                month,
                price,
                quantity,
                '{deal_name}' as deal_name,
                now() as load_date,
                'initial_load' as record_source
            FROM staging.products
            WHERE {where_clause}
            """

            with self.get_source_client() as client:
                result = client.query(query)
                data = result.result_rows

                columns = [desc for desc in result.column_names]
                logger.info(f'Cloumns in result: {columns}')
                records = [dict(zip(columns, row)) for row in data]

            logger.info(f"Extracted {len(records)} records for deal {deal_name}")
            logger.info(f"Extracted records: {records[:2]}")
            return records

        except Exception as e:
            logger.error(f"Error extracting data for deal {deal_name}: {str(e)}")
            raise

    def _build_where_clause(self, filter_conditions: List[Dict[str, Any]]) -> str:
        if not filter_conditions:
            return "1=1"

        conditions = []
        for condition in filter_conditions:
            column = condition['column']
            operator = condition['operator']
            value = condition['value']

            if operator == 'EQUALS':
                conditions.append(f"{column} = '{value}'")
            elif operator == 'CONTAINS':
                conditions.append(f"positionCaseInsensitive({column}, '{value}') > 0")
            elif operator == 'STARTS_WITH':
                conditions.append(f"startsWith({column}, '{value}')")
            elif operator == 'ENDS_WITH':
                conditions.append(f"endsWith({column}, '{value}')")
            elif operator == 'GREATER_THAN':
                conditions.append(f"{column} > {value}")
            elif operator == 'LESS_THAN':
                conditions.append(f"{column} < {value}")
            elif operator == 'BETWEEN':
                min_val, max_val = str(value).split(',')
                conditions.append(f"{column} BETWEEN {min_val} AND {max_val}")
            elif operator == 'IN':
                values = [f"'{v.strip()}'" for v in str(value).split(',')]
                conditions.append(f"{column} IN ({','.join(values)})")

        return ' AND '.join(conditions)

    def parallel_insert_to_staging(self, data: List[Dict[str, Any]], deal_name: str) -> None:
        try:
            if not data:
                logger.info("No data to insert")
                return

            logger.info(f"Starting parallel insert of {len(data)} records for deal {deal_name}")
            batches = [data[i:i + self.batch_size] for i in range(0, len(data), self.batch_size)]
            logger.info(f"Split data into {len(batches)} batches")
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                for i, batch in enumerate(batches):
                    future = executor.submit(self._insert_batch, batch, deal_name, i)
                    futures.append(future)
                for future in futures:
                    future.result()

            logger.info(f"Successfully inserted all data for deal {deal_name}")

        except Exception as e:
            logger.error(f"Error in parallel insert: {str(e)}")
            raise

    def _insert_batch(self, batch: List[Dict[str, Any]], deal_name: str, batch_id: int) -> None:
        try:
            logger.info(f"Processing batch {batch_id} with {len(batch)} records")

            with self.get_edw_client() as client:
                staging_table = 'edw.stg_products'
                df = pd.DataFrame(batch)
                df['product_hash_key'] = df['product_base_id'].apply(
                    lambda x: int(hashlib.md5(str(x).encode()).hexdigest()[:15], 16)
                )
                df['deal_hash_key'] = df['deal_name'].apply(
                    lambda x: int(hashlib.md5(str(x).encode()).hexdigest()[:15], 16)
                )
                df['product_deal_hash_key'] = df.apply(
                    lambda row: int(
                        hashlib.md5(f"{row['product_base_id']}||{row['deal_name']}".encode()).hexdigest()[:15], 16),
                    axis=1
                )
                df['product_hash_diff'] = df.apply(
                    lambda row: int(
                        hashlib.md5(f"{row['product_name']}||{row['price']}||{row['revenue']}".encode()).hexdigest()[
                        :15], 16),
                    axis=1
                )
                logger.info(f"Prepared DataFrame for batch {batch_id} with shape {df}")
                client.insert_df(
                    table=staging_table,
                    df=df,
                    settings={
                        'async_insert': 1,
                        'wait_for_async_insert': 1
                    }
                )

            logger.info(f"Successfully inserted batch {batch_id}")

        except Exception as e:
            logger.error(f"Error inserting batch {batch_id}: {str(e)}")
            raise

    def get_existing_deals(self) -> List[str]:
        try:
            query = "SELECT DISTINCT deal_business_key FROM edw.hub_deal WHERE deal_business_key IS NOT NULL"

            with self.get_edw_client() as client:
                result = client.query(query)
                deals = [row[0] for row in result.result_rows]

            return deals

        except Exception as e:
            logger.info("hub_deal table might not exist yet, returning empty list")
            return []

    def execute_query(self, query: str, use_edw: bool = True) -> Any:
        try:
            context_manager = self.get_edw_client() if use_edw else self.get_source_client()

            with context_manager as client:
                result = client.query(query)
                return result

        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise