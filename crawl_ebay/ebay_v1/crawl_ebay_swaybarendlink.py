import random
import time
from contextlib import contextmanager
from bs4 import BeautifulSoup
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

# 创建全局引擎（单例模式）- 兼容2.0语法
_engine = create_engine(
    f"mysql+mysqlconnector://u123:u123@127.0.0.1:3306/test",
    pool_size=5,
    max_overflow=10,
    pool_recycle=3600,
    future=True  # 关键参数：启用2.0兼容模式
)


# 创建线程安全的session工厂（需同步修改）
_session_factory = scoped_session(
    sessionmaker(bind=_engine, future=True)  # 添加future参数
)

@contextmanager
def get_mysql_session():
    """上下文管理器自动处理会话生命周期"""
    session = _session_factory()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def item_specifics(html):
    """
    获取ebay listing的详情页面的数据字典，方便定位数据
    :param html: eBay item specifics详情页面的标签html
    :return: 详情页面的字典
    """
    soup = BeautifulSoup(html, 'html.parser')
    result = {}

    # 找到所有的<dl>标签，其data-testid为ux-labels-values
    dl_tags = soup.find_all('dl', attrs={'data-testid': 'ux-labels-values'})
    for dl in dl_tags:
        dt = dl.find('dt', class_='ux-labels-values__labels')
        dd = dl.find('dd', class_='ux-labels-values__values')
        if dt and dd:
            key = dt.get_text(strip=True)
            value = dd.get_text(strip=True)
            result[key] = value
    return result

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_page_section(url):
    """
    获取指定URL页面中<div class="ux-layout-section-module-evo__container">标签的HTML内容

    参数：
        url (str): 要抓取的网页URL

    返回：
        str: 目标div的HTML字符串，如果未找到返回None
    """
    # 步骤1：定义预选User-Agent池（示例含3个主流浏览器最新版本）
    PRESET_AGENTS = [
        # 新增1: Edge 124 Android Mobile
        'Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36 EdgA/124.0.0.0',

        # 新增2: Chrome 124 Android TV
        'Mozilla/5.0 (Linux; Android 14; ADT-3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',

        # 新增3: Vivaldi 6.6 Windows 10
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Vivaldi/6.6'
    ]

    headers = {
        'User-Agent': random.choice(PRESET_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.ebay.com/',
        'DNT': '1'
    }



    try:
        # 先获取 cookies
        with requests.Session() as s:
            # 模拟首次访问获取基础 cookies
            s.get('https://www.ebay.com/', headers=headers, timeout=15)

            # 携带 cookies 访问目标页面
            response = s.get(url, headers=headers, timeout=20)

            if "Pardon Our Interruption" in response.text:
                raise Exception("触发反爬验证页面，需人工干预")
        response.raise_for_status()  # 检查HTTP状态码

        soup = BeautifulSoup(response.text, 'html.parser')
        container = soup.find('div', class_='ux-layout-section-module-evo__container')

        return str(container) if container else None

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return None

def get_item_links():
    try:
        with get_mysql_session() as session:
            # 查询所有item_id
            query_item_links =""" SELECT a.vc_item_link
  FROM (SELECT t.vc_item_link
  FROM crawl_ebay_swaybarendlink_listing1 t 
 WHERE (t.d_num1 LIKE '%sold%') OR (t.d_num2 LIKE '%sold%')) a 
 LEFT JOIN crawl_ebay_swaybarendlink_listing2 b 
	  ON a.vc_item_link = b.vc_item_link
 WHERE b.vc_item_link is null
 """
            df = pd.read_sql(query_item_links, session.connection())
        return df['vc_item_link'].tolist()
    except Exception as e:
        print(f"Error: {e}")

def truncate_crawl_ebay_swaybarendlink_listing2():
    try:
        with get_mysql_session() as session:
            # 查询所有item_id
            query_item_links ="""TRUNCATE TABLE crawl_ebay_swaybarendlink_listing2"""
            result = session.execute(text(query_item_links))
    except Exception as e:
        print(f"Error: {e}")
def insert_item_specifics_to_mysql(flag:bool):
    # 新增失败记录文件路径
    FAILED_LINKS_FILE = r'D:\迅雷下载\failed_links_swaybarendlink.txt'

    try:
        # 清空表
        if flag:
            truncate_crawl_ebay_swaybarendlink_listing2()
            # 新增：创建/清空失败记录文件
            with open(FAILED_LINKS_FILE, 'w', encoding='utf-8') as f:
                # 清空文件内
                f.write('')
        # 获取所有链接
        item_links_list = get_item_links()
        for item_link in item_links_list:
            # 品牌
            _brand = ''
            # 制造商零部件编号
            _manufacturer_part_number = ''
            # OE/OEM number moog号标准
            _oe_number = ''
            # 包含的部件
            _items_included = ''
            # 在车轮的位置
            _placement_on_vehicle = ''
            # 互换零件编号
            _interchange_part_num = ''

            # 获取页面内容
            html_str = get_page_section(item_link)
            if html_str:
                # 解析页面内容
                item_specifics_dict = item_specifics(html_str)
                print(item_specifics_dict)
                for key, value in item_specifics_dict.items():
                    if 'Brand' in key:
                        _brand = value
                    elif 'Manufacturer Part Number' in key:
                        if len(value) > 100:
                            _manufacturer_part_number = value[:100]
                        else:
                            _manufacturer_part_number = value
                    elif 'OE/OEM' in key:
                        if len(value) > 100:
                            _oe_number = value[:100]
                        else:
                            _oe_number = value
                    elif 'Items Included' in key:
                        if len(value) > 100:
                            _items_included = value[:100]
                        else:
                            _items_included = value
                    elif 'Vehicle' in key:
                        _placement_on_vehicle = value
                    elif 'Interchange' in key:
                        _interchange_part_num = value.replace("'","''")
                # 插入到数据库
                with get_mysql_session() as session:
                    # 查询item_id
                    stmt = text("""
                        INSERT INTO crawl_ebay_swaybarendlink_listing2
                        (vc_item_link, vc_brand, vc_manufacturer_part_number, 
                         vc_oe_number, vc_items_included, 
                         vc_placement_on_vehicle, vc_interchange_part_num)
                        VALUES
                        (:item_link, :brand, :manufacturer_part_number, 
                         :oe_number, :items_included, 
                         :placement_on_vehicle, :interchange_part_num)
                    """)
                    result = session.execute(stmt, {
                        "item_link": item_link,
                        "brand": _brand,
                        "manufacturer_part_number": _manufacturer_part_number,  # 原代码此处变量名拼写错误（双下划线__）
                        "oe_number": _oe_number,
                        "items_included": _items_included,
                        "placement_on_vehicle": _placement_on_vehicle,
                        "interchange_part_num": _interchange_part_num  # 已处理过单引号
                    })
            else:
                with open(FAILED_LINKS_FILE, 'a', encoding='utf-8') as f:
                    # 追加链接到文件
                    f.write(item_link + '\n')
            time.sleep(random.randint(3, 5))
    except Exception as e:
        print(f"Error: {e}")
        with open(FAILED_LINKS_FILE, 'a', encoding='utf-8') as f:
            # 追加链接到文件
            f.write(item_link + '\n')
        return None


if __name__ == '__main__':

    insert_item_specifics_to_mysql(False)
