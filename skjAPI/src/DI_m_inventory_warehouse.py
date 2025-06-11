"""
  '1、数据整理/库存数据'
"""

import pandas as pd
from skjAPI.utils.DataInsert import DataInsert, logger


class DI_m_inventory_warehouse(DataInsert):
    def __init__(self, english_table_name, table_name, project_name):
        super().__init__( english_table_name, table_name, project_name)


    def clean_row_before_insert(self, row):
        """
        数据清洗预处理
        :param row: df的行数据row
        :return: 返回清洗好的row
        """
        try:
            # 1. ***处理指定列
            row.iloc[2] = str(row.iloc[2]).replace(',', '').replace('，', '')
            row.iloc[3] = str(row.iloc[3]).replace(',', '').replace('，', '')


            # 2. 通用处理逻辑示例
            for i in range(len(row)):
                # 处理所有字段的空值
                if pd.isna(row.iloc[i]):

                    row.iloc[i] = None  # ***或者改为0
                # 统一去除前后空格
                elif isinstance(row.iloc[i], str):
                    row.iloc[i] = row.iloc[i].strip()
        except  Exception as e:
            logger.error(e)

        return row

if __name__ == '__main__':
    di = DI_m_inventory_warehouse('m_inventory_warehouse','库存数据','数据整理')
    di.truncate_table()
    di.insert_data_from_excel('E:\YYZ')