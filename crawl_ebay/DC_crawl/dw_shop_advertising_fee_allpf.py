import smtplib
from email.mime.text import MIMEText
import pymysql
from pymysql import Error
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import requests
import time
import pendulum
from pendulum import timezone
from selenium.webdriver.support.select import Select
from shukuajingAPI.config.gettablecnf import mysql_user,mysql_secret,szqz_user,szqz_secret,s_email,s_email_keys,r_email_yyz
from tenacity import retry, stop_after_attempt, wait_fixed



# 当前时间
shanghai_tz = timezone('Asia/Shanghai')
now = pendulum.now(tz=shanghai_tz)
today = now.strftime('%Y-%m-%d')
today = str(today)
# 昨天时间
yesterday = now.subtract(days=1).strftime('%Y-%m-%d')
yesterday = str(yesterday)
print(yesterday+ '--' + today)

chrome_driver_path = '/usr/bin/chromedriver'
db_config = {
        "host": "127.0.0.1",
        "user": mysql_user,
        "password": mysql_secret,
        "database": "dw"
    }

def get_login_html(login_url):
    """
    获取登录页面的HTML数据
    :param login_url: 访问网址
    :return:
    """
    login_url = login_url
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        # 发送 GET 请求
        response = requests.get(login_url, headers=headers)
        # 检查响应状态码
        response.raise_for_status()
        # 返回 HTML 数据
        return response.text
    except requests.RequestException as e:
        print(f"请求发生错误: {e}")
        return None


def chrome_driver_start(driver_path):
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # 无头模式
    chrome_options.add_argument('--no-sandbox')  # Linux服务器需要
    chrome_options.add_argument('--disable-dev-shm-usage')  # 限制资源使用
    chrome_options.add_argument('--disable-gpu')  # 禁用GPU加速

    service = Service(executable_path=driver_path)  # 更规范的写法
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def login_with_selenium(login_url, username, password, driver):
    """
    使用Selenium模拟用户登录，并返回登录后的页面数据
    :param login_url: 登录页面的URL
    :param username: 用户名
    :param password: 密码
    :param driver: chrome driver实例
    :return: 登录后的页面数据
    """
    try:
        # 打开登录页面，将此 URL 替换为实际的登录页面 URL
        login_url = login_url
        driver.get(login_url)

        # 等待页面加载
        time.sleep(2)

        # 找到账号和密码输入框，并输入相应的值

        username_input = driver.find_element(By.ID, 'login')
        password_input = driver.find_element(By.ID, 'password')
        username_input.send_keys(username)
        password_input.send_keys(password)

        # 找到登录按钮并点击
        login_button = driver.find_element(By.NAME, 'submit')
        login_button.click()

        # 等待登录完成
        time.sleep(5)

        # 获取登录后的页面 HTML 信息
        html_content = driver.page_source
        # print(html_content)

        # print(driver.get_cookies())

        # 新增获取当前 URL 逻辑
        current_url = driver.current_url  # <--- 关键代码
        # print("登录后网址:", current_url)

        return current_url

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def platform_ebay_click(driver: webdriver.Chrome,date_flag: bool):
    ebay_url = 'https://erp.datacaciques.com/finance/report/account#/platform_ebay/all'
    driver.get(ebay_url)

    try:
        # 优化1：改用可交互性验证
        date_selector = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "div.u-date-selector-contents"))
        )
        print("日期选择器已找到")
        # 优化2：显式滚动到可视区域
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", date_selector)
        print("日期选择器已滚动到可视区域")
        # 优化3：添加点击后等待日期面板弹出
        # 获取当前日期
        date_str = yesterday + ' / ' + today
        print(date_str)

        # 定位到 txt 类的 span 元素
        txt_span = driver.find_element(By.CSS_SELECTOR, 'span.current span.txt')

        # 执行 JavaScript 来修改 span 元素的文本内容
        # driver.execute_script(f"arguments[0].textContent = '{date_str}';", txt_span)
        # print("日期选择器已点击")
        if date_flag == 0:
            yesterday_button = driver.find_element(By.CSS_SELECTOR, '.u-date-selector-contents .yesterday')
            yesterday_button.click()
            print("昨天按钮已点击")
        else:
            today_button = driver.find_element(By.CSS_SELECTOR, '.u-date-selector-contents .today')
            today_button.click()
            print("今天按钮已点击")

        # 定位到最外层的 span 元素
        outer_span = driver.find_element(By.CSS_SELECTOR, 'span.current')

        # 点击最外层的 span 元素
        outer_span.click()

        return driver.page_source

    except Exception as e:
        print(f"操作失败: {str(e)}")
        # driver.save_screenshot('error_screenshot.png')
        return None


def click_daily_analysis(driver):
    """
    点击逐日分析按钮
    :param driver:
    :return:
    """
    ebay_url = 'https://erp.datacaciques.com/finance/report/account#/platform_ebay/all'
    driver.get(ebay_url)
    try:
        # 使用显式等待确保元素可点击
        daily_tab = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable(
                (By.XPATH, '//li[@tab-type="day"]/a[contains(text(),"逐日分析")]')
            )
        )

        # 滚动到可视区域（视情况添加）
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", daily_tab)

        # 点击前确保元素可见
        time.sleep(0.5)
        daily_tab.click()
        print("成功点击逐日分析选项卡")

        # 等待表格内容加载（根据实际页面特征调整）
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "#u-list-table-day tbody tr"))
        )

    except Exception as e:
        print(f"点击失败: {str(e)}")
        # driver.save_screenshot('daily_tab_error.png')

def select_300_rows(driver):
    try:
        # 显式等待下拉框元素可点击
        select_element = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "select[gaevent='click|store/listingShowRows']"))
        )

        # 创建Select对象
        select = Select(select_element)

        # 通过value值选择300行
        select.select_by_value("300")  # 对应option的value属性

        time.sleep(3)
        print("已选择300行显示")

    except Exception as e:
        print(f"选择失败: {str(e)}")
        # driver.save_screenshot('select_300_error.png')

def get_all_data(driver):
    try:
        # 获取表格所有行
        rows = driver.find_elements(By.CSS_SELECTOR, "#u-list-table-day tbody tr")

        data_list= []

        for row in rows:
            # 提取每列数据（根据实际表格结构调整选择器）
            cells = row.find_elements(By.TAG_NAME, 'td')
            row_data = [
                cells[2].text.strip(),
                cells[1].text.replace(',', ''),
                cells[16].text.replace('￥', '').replace(',', ''),
                cells[17].text.replace('￥', '').replace(',', ''),
                cells[18].text.replace('￥', '').replace(',', '')
            ]
            data_list.append(row_data)

        return data_list
    except Exception as e:
        print(f"获取数据失败: {str(e)}")
        return None


def save_to_database_pymysql(data_list, db_config,date_flag:bool):
    """
    使用pymysql将数据存入MySQL
    :param data_list: get_all_data返回的数据列表
    :param db_config: 数据库连接配置字典
    :return: 插入记录数
    """
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        print("数据库连接成功")
    except Error as e:
        print(f"连接失败: {e}")
        return None



    # SQL模板（使用INSERT IGNORE避免重复）
    if date_flag == False:
        print(yesterday)
        delete_sql = f"""DELETE FROM shop_advertising_fee t WHERE t.d_date = STR_TO_DATE('{yesterday}','%Y-%m-%d')"""
    else:
        delete_sql = f"""DELETE FROM shop_advertising_fee t WHERE t.d_date = STR_TO_DATE('{today}','%Y-%m-%d')"""
    insert_sql = """INSERT INTO shop_advertising_fee 
        (d_date, shop_count, plp_ad_rate, po_ad_rate, plg_ad_rate)
        VALUES (%s, %s, %s, %s, %s)"""

    if not conn:
        return 0
    try:
        with conn.cursor() as cursor:
            cursor.execute(delete_sql)
            conn.commit()
    except Exception as e:
        print(f"Delete Error: {e}")
        conn.rollback()
        return None

    try:
        with conn.cursor() as cursor:
            for data in data_list:
                # 清洗逻辑
                cursor.execute(insert_sql, data)
                print(data)
                pass
        conn.commit()
    except Exception as e:
        print(f"Insert Error: {e}")
        # log_error(e, 'insert_by_data_list')
        conn.rollback()
        # print(len(data_list))
        return len(data_list)
    finally:
        conn.close()

def web_auto_get_data_to_mysql():
    try:
        # 启动driver,打开浏览器
        driver = chrome_driver_start(chrome_driver_path)

        login_url = 'https://www.datacaciques.com/login'
        # 示例调用Selenium登录函数
        login_response = login_with_selenium(login_url, szqz_user, szqz_secret, driver)

        # 拉取昨日数据
        platform_ebay_click(driver, False)

        click_daily_analysis(driver)
        select_300_rows(driver)
        data_list1 = get_all_data(driver)
        del data_list1[0]
        save_to_database_pymysql(data_list1, db_config, False)
        time.sleep(3)

        # 拉取今日数据
        platform_ebay_click(driver, True)
        click_daily_analysis(driver)
        select_300_rows(driver)
        data_list2 = get_all_data(driver)
        del data_list2[0]
        save_to_database_pymysql(data_list2, db_config, True)
        time.sleep(5)
        driver.quit()
    except Exception as e:
        print(f"Error: {e}")
        send_email(e)
        driver.quit()
    finally:
        print("程序结束")
        driver.quit()


if __name__ == '__main__':

    web_auto_get_data_to_mysql()
    # data_list1 = [['2025-03-17', 'H12A', '0.00', '0.00', '40.17'],['2025-03-17', 'H12B', '0.00', '0.00', '40.1789']]
    # save_to_database_pymysql(data_list1, db_config, False)
    # data_list2 = [['2025-03-18', 'H12A', '0.00', '0.00', '40.17'],['2025-03-18', 'H12B', '10.00', '0.00', '40.17']]
    # save_to_database_pymysql(data_list2, db_config, True)