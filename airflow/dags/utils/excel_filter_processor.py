import pandas as pd
import logging
from typing import Dict, List, Any, Optional
import os
from dotenv import load_dotenv
logger = logging.getLogger(__name__)

load_dotenv()
class ExcelFilterProcessor:

    def __init__(self):
        self.excel_path = os.getenv('EXCEL_FILTER_PATH')
        self.filters_cache = {}
        self._load_filters()

    def _load_filters(self) -> None:
        try:
            if not os.path.exists(self.excel_path):
                logger.warning(f"Excel filter file not found: {self.excel_path}")
                return

            xl_file = pd.ExcelFile(self.excel_path)

            for sheet_name in xl_file.sheet_names:
                try:
                    df = pd.read_excel(self.excel_path, sheet_name=sheet_name)
                    filter_conditions = self._parse_filter_conditions(df)

                    if filter_conditions:
                        self.filters_cache[sheet_name] = filter_conditions
                        logger.info(f"Loaded filters for deal: {sheet_name}")

                except Exception as e:
                    logger.error(f"Error loading sheet {sheet_name}: {str(e)}")

            logger.info(f"Loaded filters for {len(self.filters_cache)} deals")

        except Exception as e:
            logger.error(f"Error loading Excel filters: {str(e)}")

    def _parse_filter_conditions(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        conditions = []

        try:
            for _, row in df.iterrows():
                if pd.notna(row.get('column_name')) and pd.notna(row.get('operator')):
                    condition = {
                        'column': str(row['column_name']).strip(),
                        'operator': str(row['operator']).strip().upper(),
                        'value': row.get('value'),
                        'logical_op': str(row.get('logical_operator', 'AND')).strip().upper()
                    }
                    conditions.append(condition)

        except Exception as e:
            logger.error(f"Error parsing filter conditions: {str(e)}")

        return conditions

    def get_deal_filters(self, deal_name: str) -> List[Dict[str, Any]]:
        return self.filters_cache.get(deal_name, [])

    def get_all_deal_names(self) -> List[str]:
        return list(self.filters_cache.keys())

    def apply_filters_to_record(self, record: Dict[str, Any]) -> List[str]:
        matching_deals = []

        for deal_name, conditions in self.filters_cache.items():
            if self._record_matches_conditions(record, conditions):
                matching_deals.append(deal_name)

        return matching_deals

    def _record_matches_conditions(self, record: Dict[str, Any], conditions: List[Dict[str, Any]]) -> bool:
        if not conditions:
            return False

        try:
            results = []

            for condition in conditions:
                column = condition['column']
                operator = condition['operator']
                filter_value = condition['value']

                if column not in record:
                    results.append(False)
                    continue

                record_value = record[column]
                match_result = self._evaluate_condition(record_value, operator, filter_value)
                results.append(match_result)

            # Apply logical operators (simplified: all AND for now)
            return all(results)

        except Exception as e:
            logger.error(f"Error evaluating conditions: {str(e)}")
            return False

    def _evaluate_condition(self, record_value: Any, operator: str, filter_value: Any) -> bool:
        try:
            if operator == 'EQUALS':
                return str(record_value).lower() == str(filter_value).lower()
            elif operator == 'CONTAINS':
                return str(filter_value).lower() in str(record_value).lower()
            elif operator == 'STARTS_WITH':
                return str(record_value).lower().startswith(str(filter_value).lower())
            elif operator == 'ENDS_WITH':
                return str(record_value).lower().endswith(str(filter_value).lower())
            elif operator == 'GREATER_THAN':
                return float(record_value) > float(filter_value)
            elif operator == 'LESS_THAN':
                return float(record_value) < float(filter_value)
            elif operator == 'BETWEEN':
                min_val, max_val = str(filter_value).split(',')
                return float(min_val) <= float(record_value) <= float(max_val)
            elif operator == 'IN':
                values = [v.strip().lower() for v in str(filter_value).split(',')]
                return str(record_value).lower() in values
            else:
                logger.warning(f"Unknown operator: {operator}")
                return False

        except Exception as e:
            logger.error(f"Error evaluating condition {operator}: {str(e)}")
            return False

    def build_sql_where_clause(self, deal_name: str) -> str:
        conditions = self.get_deal_filters(deal_name)

        if not conditions:
            return "1=1"

        where_parts = []

        for condition in conditions:
            column = condition['column']
            operator = condition['operator']
            value = condition['value']

            if operator == 'EQUALS':
                where_parts.append(f"{column} = '{value}'")
            elif operator == 'CONTAINS':
                where_parts.append(f"positionCaseInsensitive({column}, '{value}') > 0")
            elif operator == 'STARTS_WITH':
                where_parts.append(f"startsWith({column}, '{value}')")
            elif operator == 'ENDS_WITH':
                where_parts.append(f"endsWith({column}, '{value}')")
            elif operator == 'GREATER_THAN':
                where_parts.append(f"{column} > {value}")
            elif operator == 'LESS_THAN':
                where_parts.append(f"{column} < {value}")
            elif operator == 'BETWEEN':
                min_val, max_val = str(value).split(',')
                where_parts.append(f"{column} BETWEEN {min_val} AND {max_val}")
            elif operator == 'IN':
                values = [f"'{v.strip()}'" for v in str(value).split(',')]
                where_parts.append(f"{column} IN ({','.join(values)})")

        return ' AND '.join(where_parts)

    def reload_filters(self) -> None:
        logger.info("Reloading Excel filters")
        self.filters_cache.clear()
        self._load_filters()
