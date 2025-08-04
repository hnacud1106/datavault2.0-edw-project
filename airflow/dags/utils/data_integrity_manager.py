import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set
import hashlib
import json

logger = logging.getLogger(__name__)


class DataIntegrityManager:
    """
    Simplified Data Integrity Manager for CDC processing
    """

    def __init__(self, clickhouse_manager):
        self.ch_manager = clickhouse_manager
        self.processed_records_cache = set()

    def is_valid_record(self, record: Dict[str, Any]) -> bool:
        """
        Validate record format and required fields
        """
        required_fields = [
            'product_base_id', 'product_name', 'category',
            'price', 'revenue', 'month'
        ]

        for field in required_fields:
            if field not in record or record[field] is None:
                logger.warning(f"Missing required field: {field}")
                return False

        # Validate data types
        try:
            int(record['product_base_id'])
            float(record['price'])
            float(record['revenue'])
        except (ValueError, TypeError):
            logger.warning(f"Invalid data types in record: {record}")
            return False

        return True

    def generate_record_hash(self, record: Dict[str, Any]) -> str:
        """
        Generate unique hash for record to detect duplicates
        """
        normalized_record = {
            'product_base_id': record.get('product_base_id'),
            'deal_name': record.get('deal_name'),
            'operation_type': record.get('operation_type'),
            'product_name': record.get('product_name'),
            'price': record.get('price'),
            'revenue': record.get('revenue'),
            'month': record.get('month')
        }

        record_str = json.dumps(normalized_record, sort_keys=True)
        return hashlib.md5(record_str.encode()).hexdigest()

    def is_duplicate_record(self, record: Dict[str, Any]) -> bool:
        """
        Check if record is duplicate based on hash
        """
        try:
            record_hash = self.generate_record_hash(record)

            # Check in local cache first
            if record_hash in self.processed_records_cache:
                return True

            # Add to cache
            self.processed_records_cache.add(record_hash)

            # Keep cache size reasonable
            if len(self.processed_records_cache) > 10000:
                self.processed_records_cache.clear()

            return False

        except Exception as e:
            logger.warning(f"Error checking duplicate: {str(e)}")
            return False

    def validate_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate and deduplicate a batch of records
        """
        validated_records = []
        seen_hashes = set()

        for record in records:
            # Validate record
            if not self.is_valid_record(record):
                continue

            # Check for duplicates within batch
            record_hash = self.generate_record_hash(record)
            if record_hash in seen_hashes:
                logger.debug(f"Duplicate within batch: {record.get('product_base_id')}")
                continue

            # Check for global duplicates
            if self.is_duplicate_record(record):
                continue

            seen_hashes.add(record_hash)
            validated_records.append(record)

        logger.info(f"Validated {len(validated_records)} out of {len(records)} records")
        return validated_records

    def track_processed_records(self, records: List[Dict[str, Any]]) -> None:
        """
        Track processed records (simplified version)
        """
        logger.info(f"Tracked {len(records)} processed records")
        # In production, this would insert to tracking table

    def process_delete_records(self, delete_records: List[Dict[str, Any]]) -> None:
        """
        Process delete operations
        """
        logger.info(f"Processed {len(delete_records)} delete records")
        # In production, this would handle deletes properly
