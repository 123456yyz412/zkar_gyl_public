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
        # Chrome 120 Windows
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',

        # Firefox 121 Windows
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',

        # Edge 120 Windows
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'
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
            query_item_links ="""SELECT a.vc_item_link
  FROM (SELECT t.vc_item_link
  FROM crawl_ebay_wheelhub_listing1 t 
 WHERE (t.d_num1 LIKE '%sold%') OR (t.d_num2 LIKE '%sold%')) a 
 LEFT JOIN crawl_ebay_wheelhub_listing2 b 
	  ON a.vc_item_link = b.vc_item_link
 WHERE b.vc_item_link is null
 """
            df = pd.read_sql(query_item_links, session.connection())
        return df['vc_item_link'].tolist()
    except Exception as e:
        print(f"Error: {e}")

def truncate_crawl_ebay_wheelhub_listing2():
    try:
        with get_mysql_session() as session:
            # 查询所有item_id
            query_item_links ="""TRUNCATE TABLE crawl_ebay_wheelhub_listing2"""
            result = session.execute(text(query_item_links))
    except Exception as e:
        print(f"Error: {e}")
def insert_item_specifics_to_mysql(flag:bool):
    # 新增失败记录文件路径
    FAILED_LINKS_FILE = r'D:\Desktop\yyz\销售模型和目标划分\failed_links_wheel_hub.txt'

    try:
        if flag:
            # 清空表
            truncate_crawl_ebay_wheelhub_listing2()
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
            # 轮毂螺柱数量
            _wheelhub_stub_quantity = ''
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
                        _manufacturer_part_number = value
                    elif 'OE/OEM' in key:
                        if len(value) > 100:
                            _oe_number = value[:100]
                        else:
                            _oe_number = value
                    elif 'Wheel Stud Quantity' in key:
                        _wheelhub_stub_quantity = value
                    elif 'Vehicle' in key:
                        _placement_on_vehicle = value
                    elif 'Interchange' in key:
                        _interchange_part_num = value.replace("'","''")
                # 插入到数据库
                with get_mysql_session() as session:
                    # 查询item_id
                    stmt = text("""
                        INSERT INTO crawl_ebay_wheelhub_listing2
                        (vc_item_link, vc_brand, vc_manufacturer_part_number, 
                         vc_oe_number, d_wheelhub_stub_quantity, 
                         vc_placement_on_vehicle, vc_interchange_part_num)
                        VALUES
                        (:item_link, :brand, :manufacturer_part_number, 
                         :oe_number, :wheelhub_stub_quantity, 
                         :placement_on_vehicle, :interchange_part_num)
                    """)
                    result = session.execute(stmt, {
                        "item_link": item_link,
                        "brand": _brand,
                        "manufacturer_part_number": _manufacturer_part_number,  # 原代码此处变量名拼写错误（双下划线__）
                        "oe_number": _oe_number,
                        "wheelhub_stub_quantity": _wheelhub_stub_quantity,
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
    #html = """<div class="ux-layout-section-module-evo__container"><div data-testid="ux-layout-section-module-evo" class="ux-layout-section-module-evo"><div class="section-title"><div class="section-title__title-container"><h2 class="section-title__title" id="s0-35-26-41-18-1-93[1]-2-3-7[0]-7[0]-4[0]-11[2]-1-5-title"><span data-testid="ux-textual-display" class="ux-layout-section-module-evo__title"><span class="ux-textspans">Item specifics</span></span></h2></div></div><div data-testid="ux-layout-section-evo" class="ux-layout-section-evo ux-layout-section--features"><div data-testid="ux-layout-section-evo__item" class="ux-layout-section-evo__item ux-layout-section-evo__item--table-view"><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--condition"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Condition</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-expandable-textual-display-block-inline"><span data-testid="text"><span class="ux-textspans">New: A brand-new, unused, unopened, undamaged item in its original packaging (where packaging is ... </span></span><span class="ux-expandable-textual-display-block-inline__control" data-testid="viewMore"><button class="ux-action fake-link fake-link--action" data-testid="ux-action" data-clientpresentationmetadata="{&quot;presentationType&quot;:&quot;EXPAND_INLINE&quot;}" data-vi-tracking="{&quot;eventFamily&quot;:&quot;ITM&quot;,&quot;eventAction&quot;:&quot;ACTN&quot;,&quot;actionKind&quot;:&quot;CLICK&quot;,&quot;operationId&quot;:&quot;4429486&quot;,&quot;flushImmediately&quot;:false,&quot;eventProperty&quot;:{&quot;moduledtl&quot;:&quot;mi:3560|li:106744&quot;,&quot;sid&quot;:&quot;p4429486.m3560.l106744&quot;}}" data-click="{&quot;eventFamily&quot;:&quot;ITM&quot;,&quot;eventAction&quot;:&quot;ACTN&quot;,&quot;actionKind&quot;:&quot;CLICK&quot;,&quot;operationId&quot;:&quot;4429486&quot;,&quot;flushImmediately&quot;:false,&quot;eventProperty&quot;:{&quot;moduledtl&quot;:&quot;mi:3560|li:106744&quot;,&quot;sid&quot;:&quot;p4429486.m3560.l106744&quot;}}"><span class="ux-textspans">Read more<span class="clipped">about the condition</span></span></button></span></span><span aria-hidden="true" class="ux-expandable-textual-display-block-inline hide" data-testid="ux-expandable-textual-display-block-inline"><span data-testid="text"><span class="ux-textspans">New: A brand-new, unused, unopened, undamaged item in its original packaging (where packaging is applicable). Packaging should be the same as what is found in a retail store, unless the item was packaged by the manufacturer in non-retail packaging, such as an unprinted box or plastic bag. See the seller's listing for full details. </span><a href="https://pages.ebay.com/ru/en-us/help/sell/contextual/condition_8.html" target="_blank" class="ux-action" data-testid="ux-action" data-clientpresentationmetadata="{&quot;presentationType&quot;:&quot;OPEN_WINDOW&quot;}"><span class="ux-textspans">See all condition definitions<span class="clipped">opens in a new window or tab</span></span></a></span></span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment12"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 12</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2006-2011 Cadillac Dts</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment8"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 8</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2002-2007 Buick Rendezvous</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment13"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 13</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1997-2002 Cadillac Eldorado</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment7"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 7</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1997-2004 Buick Regal</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment10"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 10</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2005 Buick Terraza</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment6"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 6</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1997-2005 Buick Park Avenue</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment11"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 11</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1997-2005 Cadillac Deville</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment5"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 5</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2006-2011 Buick Lucerne</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment16"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 16</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2000-2007 Chevrolet Monte Carlo</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment4"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 4</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2000-2005 Buick Lesabre</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment17"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 17</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2005 Chevrolet Uplander</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment3"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 3</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2005-2009 Buick Lacrosse</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment14"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 14</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1997-2004 Cadillac Seville</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment2"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 2</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1997-2005 Buick Century</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment1"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 1</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2005-2010 Buick Allure</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment15"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 15</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2000-2013 Chevrolet Impala</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment18"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 18</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1997-2005 Chevrolet Venture</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment19"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 19</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1997-2003 Oldsmobile Aurora</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--placementOnVehicle"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Placement on Vehicle</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Front, Left, Right</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment9"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 9</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1997-1999 Buick Riviera</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--wheelStudQuantity"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Wheel Stud Quantity</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">5</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--manufacturerWarranty"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Manufacturer Warranty</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">3 Years</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--finish"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Finish</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Polished, Rust Protected</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment23"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 23</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2000-2005 Pontiac Bonneville</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--brakePilotDiameter"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Brake Pilot Diameter</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">2.78 in</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment24"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 24</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1997-2008 Pontiac Grand Prix</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--otherPartNumber"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Other Part Number</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">80168860297;WB13187;80168860297;BR930149</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment21"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 21</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1997-2004 Oldsmobile Silhouette</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment22"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 22</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2001-2005 Pontiac Aztek</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment27"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 27</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 2005 Saturn Relay</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment25"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 25</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1999-2005 Pontiac Montana</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment26"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 26</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1997-1999 Pontiac Trans Sport</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--manufacturerPartNumber"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Manufacturer Part Number</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">TK513121, 513187</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--material"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Material</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Steel</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--brand"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Brand</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">MOOG</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--type"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Type</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Wheel Bearing</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--interchangePartNumber"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Interchange Part Number</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">513121 513179 513187 513199, Cadillac DeVille, Eldorado, Seville, Chevrolet Impala, Monte Carlo, Oldsmobile Aurora, Intrigue,, Pontiac Aztek, Bonneville, Grand Prix, 2.4l 3.6l 3.9l 4.6l 3.0l, Impala, cxl cx ls lt super, LTZ Police SS 3.5L CXL, driver passenger side, wheel hub assembly, wheel bearing hub assembly, wheel hub, front wheel bearing hub assembly, wheel hubs assembly, wheel assembly, chevy buick pontiac chevrolet</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--oe/oemPartNumber"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">OE/OEM Part Number</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">513121, 513187</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--flangeDiameter"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Flange Diameter</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">5.73 in</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--anti-lockBrakingSystem"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Anti-lock Braking System</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Yes</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--flangeOffset"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Flange Offset</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">1.65 in</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values--fitment20"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Fitment 20</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">Fits 1998-2002 Oldsmobile Intrigue</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values__column-last-row ux-labels-values--hubPilotDiameter"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">Hub Pilot Diameter</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">3.57 in</span></div></div></dd></dl></div></div><div class="ux-layout-section-evo__row"><div class="ux-layout-section-evo__col"><dl data-testid="ux-labels-values" class="ux-labels-values ux-labels-values--inline col-6 ux-labels-values__column-last-row ux-labels-values--upc"><dt class="ux-labels-values__labels"><div class="ux-labels-values__labels-content"><div><span class="ux-textspans">UPC</span></div></div></dt><dd class="ux-labels-values__values"><div class="ux-labels-values__values-content"><div><span class="ux-textspans">0614046703322</span></div></div></dd></dl></div><div class="ux-layout-section-evo__col"></div></div></div></div></div></div>"""
    # url = "https://www.ebay.com/itm/175353100286?_skw=Wheel+Hub+bearing&itmmeta=01JS5M8G2CP6TRMS6T25RP6HBW&hash=item28d3dbf7fe:g:RMQAAOSwEAlm68kD&itmprp=enc%3AAQAKAAAA0FkggFvd1GGDu0w3yXCmi1ef3lcv3WCXD2KBztioUctX1hyqWwM76qIsCF7M1wLa9c%2BVNZqclIni62ZAv25%2FqLnszl4XDX%2BvAktPub7%2BedSkcs1BTwNsyi1THu8y%2FsKFdjXux9TE4qil5SHpf0Y76bvq86D%2B4FbYUB3PLL1G5WLzRWsnkcmYbkA7lRs0tYjpjR8DCBNBtM%2FO0EuVGFzfulosL5g%2B%2BQoHuwAMjynPZLbkPQG7kbm94w%2F7FokLgmzW56TOp1MJ%2Fh%2F6mk7770chRYY%3D%7Ctkp%3ABk9SR7yBorTJZQ"
    #
    # html_str = get_page_section(url)
    # for key, value in item_specifics(html_str).items():
    #     print(f"{key}: {value}")
    insert_item_specifics_to_mysql(False)
