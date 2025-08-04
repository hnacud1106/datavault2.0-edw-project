import clickhouse_connect
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import logging
import time
from typing import List, Dict, Any
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ParallelClickHouseInserter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.max_workers = config.get('max_workers', 4)
        self.batch_size = config.get('batch_size', 10000)

    def parallel_insert(self, data: List[Dict[str, Any]], table_name: str) -> None:
        try:
            logger.info(f"Starting parallel insert of {len(data)} records to {table_name}")
            start_time = time.time()
            df = pd.DataFrame(data)
            batches = np.array_split(df, self.max_workers)
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []

                for i, batch_df in enumerate(batches):
                    if not batch_df.empty:
                        future = executor.submit(
                            self._insert_batch_df,
                            batch_df,
                            table_name,
                            i
                        )
                        futures.append(future)
                for future in futures:
                    future.result()

            elapsed_time = time.time() - start_time
            records_per_second = len(data) / elapsed_time

            logger.info(f"Parallel insert completed in {elapsed_time:.2f}s")
            logger.info(f"Insertion rate: {records_per_second:.0f} records/second")

        except Exception as e:
            logger.error(f"Error in parallel insert: {str(e)}")
            raise

    def _insert_batch_df(self, batch_df: pd.DataFrame, table_name: str, batch_id: int) -> None:
        try:
            logger.info(f"Processing batch {batch_id} with {len(batch_df)} records")
            client = clickhouse_connect.get_client(**self.config)
            client.insert_df(
                table=table_name,
                df=batch_df,
                settings={
                    'async_insert': 1,
                    'wait_for_async_insert': 1,
                    'async_insert_max_data_size': '10MB',
                    'async_insert_busy_timeout_ms': 1000
                }
            )

            logger.info(f"Batch {batch_id} inserted successfully")

        except Exception as e:
            logger.error(f"Error inserting batch {batch_id}: {str(e)}")
            raise
        finally:
            client.close()


if __name__ == "__main__":
    config = {
        'host': 'localhost',
        'port': 8124,
        'username': 'default',
        'password': 'edw_password_123',
        'database': 'edw',
        'max_workers': 4,
        'batch_size': 10000
    }
    inserter = ParallelClickHouseInserter(config)
    sample_data = [
        {
            'product_base_id': i,
            'product_name': f'Product {i}',
            'deal_name': 'Test Deal',
            'load_date': '2025-01-01 00:00:00'
        }
        for i in range(50000)
    ]

    inserter.parallel_insert(sample_data, 'edw.stg_products')
