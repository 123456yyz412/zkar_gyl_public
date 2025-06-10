import re
from skjAPI.utils.token_str import get_api_keys_by_token
from skjAPI.utils.get_table_data import get_table_data_details
from skjAPI.shukuajing_config.skj_config import api_url
import pinyin
import pandas as pd


def translate_chinese_to_pinyin(text):
    """
    辅助函数，将字符串中的中文部分转换为拼音。
    参数:
    text (str): 输入的字符串。
    返回值:
    str: 转换后的字符串，中文部分为拼音，其他部分保持不变。
    """
    result = ""
    pattern = re.compile(r'([\u4e00-\u9fa5]+)')
    parts = re.split(pattern, text)
    for part in parts:
        if pattern.match(part):
            result += pinyin.get(part, format="strip", delimiter="_")
        else:
            result += part.split("（")[0].split("）")[0].split("(")[0].split(")")[0].replace("-","_").lower()
    return result
def generate_create_table_sql(table_name, columns):
    """
    该函数根据传入的表名和列名列表生成对应的 MySQL 建表语句。
    参数:
    table_name (str): 要创建的表的名称。
    columns (list): 包含列名的列表，其中英文、数字或包含括号、短横线等符号的部分保持不变，中文部分会被转换为拼音作为字段名，同时原字段作为字段注释。

    返回值:
    str: 生成的 MySQL 建表语句。
    """


    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n"
    for col in columns:
        translated_col = translate_chinese_to_pinyin(col)
        create_table_sql += f"    `{translated_col}` VARCHAR(255) COMMENT '{col}',\n"
    create_table_sql = create_table_sql.rstrip(",\n") + f"\n) ENGINE=InnoDB COMMENT = '{table_name}';"
    return create_table_sql

def Create_table_sql(table_name, project_name, token_str, api_url):
    """
    通过API的POST请求获取表数据
    :param table_name: 表的名字，唯一近似名字
    :param token_str: 临时令牌（十秒钟只允许获取一次 token，且 token 将于一小时内有效，请妥善保存，失效后请重新获取）get_api_keys_by_token获取
    :param api_url: API的URL地址
    :param pageIndex: 页码，默认为1
    :param table_body: 请求体，默认为None
    :return: 建表语句
    """
    columns = get_table_data_details(table_name, project_name, token_str, api_url, 1).get("fields")

    sql_str = generate_create_table_sql(table_name, columns)
    return sql_str

def create_table_sql_from_excel(table_name, url):
    """
    通过Excel表格获取表数据
    :param table_name: 表的名字，唯一近似名字
    :param url: Excel表格的URL地址
    :return: 建表语句
    """
    # 读取Excel文件（注意文件路径）
    df = pd.read_excel(url, header=None)  # header=None表示不将首行作为列名

    # 获取首行数据并转为列表
    first_row = df.iloc[0].tolist()
    print(first_row)


    return generate_create_table_sql(table_name, first_row)


if __name__ == '__main__':
    token_str = get_api_keys_by_token()
    # print(token_str)
    # print(Create_table_sql("销售数量", "销售管理", token_str, api_url))
    # print(create_table_sql_from_excel("inventory_warehouse", "E:\YYZ\库存数据_20250609.xlsx"))
    print(create_table_sql_from_excel("m_sales_quantity", r"E:\YYZ\2、销售数量_20250610.xlsx"))