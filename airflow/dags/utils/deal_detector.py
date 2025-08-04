import logging
from typing import List, Set
from utils.clickhouse_utils import ClickHouseManager
from utils.excel_filter_processor import ExcelFilterProcessor

logger = logging.getLogger(__name__)


class DealDetector:

    def __init__(self, ch_manager: ClickHouseManager, excel_processor: ExcelFilterProcessor):
        self.ch_manager = ch_manager
        self.excel_processor = excel_processor

    def get_existing_deals(self) -> Set[str]:
        try:
            deals = self.ch_manager.get_existing_deals()
            return set(deals)
        except Exception as e:
            logger.warning(f"Could not retrieve existing deals: {str(e)}")
            return set()

    def get_excel_deal_sheets(self) -> Set[str]:
        try:
            deals = self.excel_processor.get_all_deal_names()
            return set(deals)
        except Exception as e:
            logger.error(f"Error getting Excel deal sheets: {str(e)}")
            return set()

    def find_new_deals(self, existing_deals: Set[str], excel_deals: Set[str]) -> List[str]:
        new_deals = excel_deals - existing_deals
        return list(new_deals)
