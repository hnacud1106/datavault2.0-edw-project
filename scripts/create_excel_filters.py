import pandas as pd
import os


def create_excel_filters():
    os.makedirs('configs', exist_ok=True)
    with pd.ExcelWriter('configs/deal_filters.xlsx', engine='openpyxl') as writer:
        hapas_data = {
            'column_name': ['brand', 'category', 'price'],
            'operator': ['CONTAINS', 'EQUALS', 'GREATER_THAN'],
            'value': ['Hapas', 'Skincare', '100000'],
            'logical_operator': ['AND', 'AND', 'AND']
        }
        pd.DataFrame(hapas_data).to_excel(writer, sheet_name='Hapas', index=False)
        obagi_data = {
            'column_name': ['brand', 'product_name', 'category'],
            'operator': ['CONTAINS', 'CONTAINS', 'IN'],
            'value': ['Obagi', 'serum', 'Skincare,Beauty'],
            'logical_operator': ['AND', 'OR', 'AND']
        }
        pd.DataFrame(obagi_data).to_excel(writer, sheet_name='Obagi', index=False)
        loreal_data = {
            'column_name': ['brand', 'category', 'price'],
            'operator': ['CONTAINS', 'EQUALS', 'BETWEEN'],
            'value': ['Loreal', 'Skincare', '50000,500000'],
            'logical_operator': ['AND', 'AND', 'AND']
        }
        pd.DataFrame(loreal_data).to_excel(writer, sheet_name='Loreal', index=False)

    print("✅ Excel filter file created successfully at configs/deal_filters.xlsx")


if __name__ == "__main__":
    create_excel_filters()
