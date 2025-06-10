"""
把获取到的excel文件清洗到MySQL中
"""
import os
import pandas as pd
import redis
from skjAPI.utils.log_util import SKJLogger
from skjAPI.utils.get_table_data import download_table_excel,delete_excel_files_three_days_ago
from skjAPI.shukuajing_config.skj_config import log_win_path,log_linux_path,api_url
from skjAPI.utils.token_str import get_api_keys_by_token
from skjAPI.utils.create_table_sql import translate_chinese_to_pinyin
import pymysql
from openpyxl import Workbook
from openpyxl.styles import Font

wb = Workbook()
ws = wb.active
# 显式设置默认字体样式（示例）
ws.font = Font(name="Calibri", size=11)
# 定义日志路径
logger = SKJLogger('数跨境toMySQL',log_dir= log_win_path).get_logger()

# 定义数据库配置
mysql_config = {'host': '127.0.0.1',
                'user': 'u123',
                'password': 'u123',
                'database': 'test'
                }


class DataInsert:
    """
    下载数跨境api的数据
    """
    def __init__(self, english_table_name, table_name, project_name):
        self.english_table_name = english_table_name
        self.table_name = table_name
        self.project_name = project_name
        self.token_str = get_api_keys_by_token()#self.receive_token()  # 从 redis 获取 token


    def receive_token(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
        token_str = str(r.get('token')).split("'")[1]
        return token_str

    def clean_row_before_insert(self, row):
        """
        数据清洗预处理
        :param row: df的行数据row
        :return: 返回清洗好的row
        """
        try:
            # 1. ***处理指定列
            if len(row) > 6:
                # 案例，这里是处理第7列的数据
                row.iloc[6] = str(row.iloc[6]).replace(',', '').replace('，', '')

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

    def import_excel_sheets_to_mysql(self, folder_path: str, mysql_config: dict, table_name: str):
        """
        遍历指定路径下的所有Excel文件，将每个sheet的数据写入MySQL数据库中指定的表。

        :param folder_path: 要遍历的文件夹路径
        :param mysql_config: MySQL连接配置字典，格式：
                             {
                                 'host': 'localhost',
                                 'user': 'root',
                                 'password': 'password',
                                 'database': 'your_database'
                             }
        :param table_name: 要写入的目标数据库表名
        """
        # 建立数据库连接
        conn = pymysql.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )
        cursor = conn.cursor()

        file_path = folder_path
        print(f"正在处理文件：{file_path}")

        try:
            # 使用 pandas 打开 Excel 文件
            with pd.ExcelFile(file_path) as xls:
                for sheet_name in xls.sheet_names:
                    print(f"读取sheet页：{sheet_name}，来自文件：{file_path.split('/')[-1]}")
                    df = pd.read_excel(xls, sheet_name=sheet_name)

                    if df.empty:
                        print(f"{sheet_name} 是空表，跳过导入")
                        continue

                    # 将DataFrame数据写入MySQL（逐行插入）
                    for _, row in df.iterrows():
                        key_list = [translate_chinese_to_pinyin(i) for i in list(row.index)]
                        key = ', '.join(key_list)
                        values = ', '.join(['%s'] * len(row))
                        insert_sql = f"INSERT INTO {table_name} ({key}) VALUES ({values})"
                        # 通用清洗逻辑插入
                        cursor.execute(insert_sql, tuple(self.clean_row_before_insert(row)))

                    conn.commit()
        except Exception as e:
            print(f"处理文件 {file_path} 出错：{e}")
            conn.rollback()



        cursor.close()
        conn.close()
        print("所有Excel文件导入完成！")
    def insert_data_from_excel(self, data_path:  str):
        """
        1）清空下载路径的时长超过三天的文件
        2）下载文件到该路径
        3）插入数据到对应数据库
        :param data_path: 数跨境文件的下载路径
        :return:
        """
        try:
            # if not os.path.exists(data_path):
            #     os.makedirs(data_path)
            # 删除三天前的文件
            delete_excel_files_three_days_ago(data_path)
            #  下载数据
            full_path = download_table_excel(self.table_name,self.project_name,self.token_str, api_url,data_path,10,30)
            print(full_path)
            # 导入excel文件数据到MySQL数据库
            self.import_excel_sheets_to_mysql(str(full_path), mysql_config, self.english_table_name)
        except Exception as e:
            logger.error(e)


    def truncate_table(self):
        """
        清空表
        :return:
        """
        try:
            conn = pymysql.connect(
                host=mysql_config['host'],
                user=mysql_config['user'],
                password=mysql_config['password'],
                database=mysql_config['database']
            )
            cursor = conn.cursor()
            cursor.execute(f"TRUNCATE TABLE {self.english_table_name}")
            conn.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error(e)
            return False

    def delete_table_from_date(self, begin_date: str, end_date: str):
        """
        删除指定时间段内的数据
        :param begin_date: 开始时间
        :param end_date: 结束时间
        :return:
        """
        try:
            conn = pymysql.connect(
                host=mysql_config['host'],
                user=mysql_config['user'],
                password=mysql_config['password'],
                database=mysql_config['database']
            )
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {self.english_table_name} WHERE date >= '{begin_date}' AND date <= '{end_date}'")
            conn.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error(e)
            return False





if __name__ == '__main__':
    # 创建DataInsert对象
    data_insert = DataInsert(english_table_name='m_inventory_warehouse', table_name='库存数据', project_name='数据整理')
    data_insert.truncate_table()
    data_insert.insert_data_from_excel('E:\YYZ')


