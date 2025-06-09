"""
把获取到的excel文件清洗到MySQL中
"""
import os
import pandas as pd
import redis
from shukuajingAPI.utils.log_util import SKJLogger
from shukuajingAPI.utils.get_table_data import download_table_excel,delete_excel_files_three_days_ago
from shukuajingAPI.shukuajing_config.skj_config import log_win_path,log_linux_path
import pymysql

# 定义日志路径
logger = SKJLogger('数跨境',log_dir= log_win_path).get_logger()




class DataInsert:
    """
    下载数跨境api的数据
    """
    def __init__(self, english_table_name, table_name, project_name):
        self.english_table_name = english_table_name
        self.table_name = table_name
        self.project_name = project_name
        self.token_str = self.receive_token()  # 从 redis 获取 token


    def receive_token(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
        token_str = str(r.get('token')).split("'")[1]
        return token_str

    def import_excel_sheets_to_mysql(folder_path: str, mysql_config: dict, table_name: str):
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
            host='127.0.0.1',
            user='u123',
            password='u123',
            database='model'
        )
        cursor = conn.cursor()

        # 支持的Excel扩展名
        excel_extensions = ('.xlsx', '.xls')

        # 遍历文件夹
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith(excel_extensions):
                    file_path = os.path.join(root, file)
                    print(f"正在处理文件：{file_path}")

                    try:
                        # 使用 pandas 打开 Excel 文件
                        with pd.ExcelFile(file_path) as xls:
                            for sheet_name in xls.sheet_names:
                                print(f"读取sheet页：{sheet_name}，来自文件：{file}")
                                df = pd.read_excel(xls, sheet_name=sheet_name)

                                if df.empty:
                                    print(f"{sheet_name} 是空表，跳过导入")
                                    continue

                                # 将DataFrame数据写入MySQL（逐行插入）
                                for _, row in df.iterrows():
                                    keys = ', '.join(row.index)
                                    values = ', '.join(['%s'] * len(row))
                                    insert_sql = f"INSERT INTO {table_name} ({keys}) VALUES ({values})"
                                    cursor.execute(insert_sql, tuple(row))

                                conn.commit()
                    except Exception as e:
                        print(f"处理文件 {file_path} 出错：{e}")
                        conn.rollback()

        cursor.close()
        conn.close()
        print("所有Excel文件导入完成！")
    def insert_data_from_excel(self, data_path:  str):
        """

        :param data_path:
        :return:
        """
        # 删除三天前的文件
        delete_excel_files_three_days_ago(data_path)
        #  下载数据
        file_name = download_table_excel(self.token_str, self.english_table_name, self.table_name, self.project_name, data_path)




if __name__ == '__main__':
    # 创建DataInsert对象
    data_insert = DataInsert(english_table_name='', table_name='', project_name='')


