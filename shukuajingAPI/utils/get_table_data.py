import time
import os
from datetime import datetime
import requests
from shukuajingAPI.shukuajing_config.skj_config import api_url
from shukuajingAPI.utils.token_str import get_api_keys_by_token
from shukuajingAPI.utils.log_util import logger


def get_all_projects(token_str:str,api_url):
    """
    通过API的POST请求获取所有项目
    :param api_url: API的URL地址
    :param headers: 请求头，默认为None
    :param token_str: 临时令牌（十秒钟只允许获取一次 token，且 token 将于一小时内有效，请妥善保存，失效后请重新获取）
    :return: 所有项目的列表
    """
    api_str ="/decision/api/v1/folders"
    try:
        response = requests.post(api_url + api_str, headers={'Authorization':token_str})
        response.raise_for_status()  # 如果响应状态码不是200，抛出异常
        data = response.json()
        #print("所有项目名称:"+str(data.get('data','')))
        return data.get('data','')

    except requests.exceptions.RequestException as e:
        # print(f"Error fetching API projects: {e}")
        logger.error(f"Error fetching API projects: {e}")
        return None

def get_id_by_name(name:str,token_str:str,api_url):
    """
    通过项目名称请求获取该项目的id
    :param api_url: API的URL地址
    :param headers: 请求头，默认为None
    :param name: 项目名称
    :param token_str: 临时令牌（十秒钟只允许获取一次 token，且 token 将于一小时内有效，请妥善保存，失效后请重新获取）
    :return: 所有项目的列表
    """
    list_all = get_all_projects(token_str,api_url)
    for dict in list_all:
        if name in dict.get('name'):
            #print("项目id: "+str(dict['id'])+'  项目名称：'+str(dict['name']))
            return dict['id']
    return None

def get_table_names(project_name:str,token_str:str, api_url):
    """
    通过API的POST请求获取所有表名
    :param project_name: 项目名称
    :param token_str: 临时令牌（十秒钟只允许获取一次 token，且 token 将于一小时内有效，请妥善保存，失效后请重新获取）
    :param api_url: API的URL地址
    :param headers: 请求头，默认为None
    :return: 所有表的列表
    """
    folderId = str(get_id_by_name(project_name,token_str, api_url))
    api_str = f"/decision/api/v1/{folderId}/tables"
    try:
        response = requests.post(api_url + api_str, headers={'Authorization':token_str})
        response.raise_for_status()
        data = response.json()
        #print("所有表名:"+str(data.get('data','')))
        return data.get('data','')
    except requests.exceptions.RequestException as e:
        # print(f"Error fetching API tables: {e}")
        logger.error(f"Error fetching API tables: {e}")
        return None

def get_tableNameIDDIct_by_name(table_name:str,project_name:str,token_str:str, api_url):
    """
    通过表名称请求获取该表的id
    :param table_name: 表名称
    :param project_name: 项目名称
    :param token_str: 临时令牌（十秒钟只允许获取一次 token，且 token 将于一小时内有效，请妥善保存，失效后请重新获取）
    :param api_url: API的URL地址
    :return: 表的id
    """
    list_all = get_table_names(project_name,token_str, api_url)
    for dict in list_all:
        if table_name in dict.get('name'):
            return [dict['name'],dict['id']]
    return None

def get_table_data(table_name:str,project_name:str,token_str:str, api_url):
    """
    通过API的POST请求获取表数据
    :param table_name: 表的名字，唯一近似名字
    :param token_str: 临时令牌（十秒钟只允许获取一次 token，且 token 将于一小时内有效，请妥善保存，失效后请重新获取）get_api_keys_by_token获取
    :param api_url: API的URL地址
    :param pageIndex: 页码，默认为1
    :param table_body: 请求体，默认为None
    :return: 表的数据
    """
    tableNameID_list = get_tableNameIDDIct_by_name(table_name,project_name,token_str, api_url)
    tableId = str(tableNameID_list[1])
    tableName = str(tableNameID_list[0])
    api_str = f"/decision/api/v2/export"
    body = {
        "tableId": tableId,
        "format": "excel"
    }

    try:
        response = requests.post(api_url + api_str, headers={'Authorization':token_str}, json=body)
        response.raise_for_status()
        data = response.json()
        #print(tableName+"字段名:"+str(data.get('data','').get('fields','')))
        #print('*****:'+str(data))
        # print(data)
        logger.info(data)
        return data
    except requests.exceptions.RequestException as e:
        # print(f"Error fetching API table datas: {e}")
        logger.error(f"Error fetching API table datas: {e}")
        return None

def get_table_data_gip_30s(table_name:str,project_name:str,token_str:str, api_url):
    """
    通过表名称请求获取该表的数据
    :param table_name: 表名称
    :param project_name: 项目名称
    :param token_str: 临时令牌（十秒钟只允许获取一次 token，且 token 将于一小时内有效，请妥善保存，失效后请重新获取）
    :param api_url: API的URL地址
    :return: 表的数据
    """
    table_data = get_table_data(table_name,project_name,token_str, api_url)
    if  table_data != None:
        return table_data
    else:
        time.sleep(30)
        return get_table_data_gip_30s(table_name,project_name,token_str, api_url)


def download_table_excel(
        table_name: str,
        project_name: str,
        token_str: str,
        api_url: str,
        save_path: str = None,
        max_retries: int = 3,
        retry_interval: int = 10
) -> str:
    """
    下载指定表数据为Excel文件

    :param table_name: 目标表名称
    :param project_name: 所属项目名称
    :param token_str: API认证令牌
    :param api_url: API基础地址
    :param save_path: 文件保存路径（默认当前目录）
    :param max_retries: 最大重试次数
    :param retry_interval: 重试间隔秒数
    :return: 文件绝对路径（成功时）/ None（失败时）
    """

    # 获取表元数据
    table_meta = get_tableNameIDDIct_by_name(table_name, project_name, token_str, api_url)
    if not table_meta:
        # print(f"[ERROR] 未找到表 {table_name}")
        logger.error(f"[ERROR] 未找到表 {table_name}")
        return None

    # 生成规范文件名
    file_name = f"{table_meta[0]}_{datetime.now().strftime('%Y%m%d')}.xlsx"
    # 判断文件是否存在于该目录下面，存在则不下载，不存在则下载
    if check_excel_file_exists(file_name, save_path):
        logger.info(f"[INFO] 文件 {file_name} 已存在，跳过下载")
        return file_name

    save_path = save_path or os.getcwd()
    full_path = os.path.abspath(os.path.join(save_path, file_name))

    # 创建保存目录
    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    for attempt in range(1, max_retries + 1):
        try:
            # 获取导出数据
            export_data = get_table_data(table_name, project_name, token_str, api_url)

            # 处理异步导出状态
            if export_data.get('errorCode') == '61311022':
                # print(f"[INFO] 导出进行中，第 {attempt} 次重试...")
                logger.info(f"[INFO] 导出进行中，第 {attempt} 次重试...")
                time.sleep(retry_interval)
                continue

            # 获取下载链接（根据实际API响应结构调整）
            download_url = export_data.get('data', '')
            if not download_url:
                # print("[ERROR] 响应中未包含下载链接")
                logger.error("[ERROR] 响应中未包含下载链接")
                return None

            # 下载文件流
            response = requests.get(download_url, stream=True)
            response.raise_for_status()

            # 写入文件
            with open(full_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # print(f"[SUCCESS] 文件已保存至：{full_path}")
            logger.info(f"[SUCCESS] 文件已保存至：{full_path}")
            return file_name

        except requests.exceptions.RequestException as e:
            # print(f"[ERROR] 请求失败：{str(e)}")
            logger.error(f"[ERROR] 请求失败：{str(e)}")
        except Exception as e:
            # print(f"[ERROR] 文件保存异常：{str(e)}")
            logger.error(f"[ERROR] 文件保存异常：{str(e)}")

        # 失败时等待重试
        if attempt < max_retries:
            # print(f"等待 {retry_interval} 秒后重试...")
            logger.info(f"等待 {retry_interval} 秒后重试...")
            time.sleep(retry_interval)

    return None

def delete_excel_files_three_days_ago(directory_path):
    """
    遍历指定路径下的所有Excel文件，并删除修改时间为三天前的文件。

    :param directory_path: 要遍历的目录路径
    """
    # 获取当前时间戳
    current_time = time.time()
    # 计算三天前的时间戳
    three_days_ago = current_time - 3 * 24 * 60 * 60

    # 遍历目录下的所有文件和子目录
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            # 检查文件是否为Excel文件（扩展名）
            if file.endswith(".xls") or file.endswith(".xlsx"):
                file_path = os.path.join(root, file)
                # 获取文件的最后修改时间
                file_mtime = os.path.getmtime(file_path)

                # 如果文件的修改时间早于三天前，则删除该文件
                if file_mtime < three_days_ago:
                    os.remove(file_path)


def check_excel_file_exists(file_name, directory_path):
    """
    判断指定路径下是否存在特定名称的Excel文件。

    :param file_name: 要查找的Excel文件名（包括扩展名，如.xlsx或.xls）
    :param directory_path: 要搜索的目录路径
    :return: 如果存在返回 True，否则返回 False
    """
    # 构造完整文件路径
    target_path = os.path.join(directory_path, file_name)

    # 判断该路径是否存在且是一个文件
    return os.path.isfile(target_path)





if __name__ == '__main__':
    token_str = get_api_keys_by_token()
    # # 获取所有项目
    # print(get_all_projects(token_str,api_url))
    # # 获取所有表名
    # print(get_table_names('数据库资料',token_str, api_url))
    # # 获取表数据
    # print(get_table_data('库存数据','数据整理',token_str, api_url))
    # print(download_table_excel('库存数据','数据整理',token_str, api_url,'E:\YYZ',10,30))
    print(download_table_excel('FBA库存状况报告','数据整理',token_str, api_url,'E:\YYZ',10,30))
    # print(download_table_excel('销售数量','销售管理',token_str, api_url,'E:\YYZ',10,30))
    # print(download_table_excel('销售管理-销售数量','数据库资料',token_str, api_url,'E:\YYZ',10,30))
    # print(download_table_excel('一级订单SKU','数据库资料',token_str, api_url,'E:\YYZ',10,30))


