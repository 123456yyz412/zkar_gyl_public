import http.client
import json
from urllib.parse import quote
from ForestAPI.forest_api_config.forest_config import token_user_name,token_password

def user_api_token(company_name:str, password:str):
    """
    获取公司的api token
    :param company_name: 公司名称
    :param password: 用户名
    :return: {"success":true,
    "status":"0",
    "message":"查询成功",
    "code":200,
    "result":"70db89a",
    "errorMsg":null,
    "overageMes":null,
    "timestamp":1748316892425}
    """
    username = quote(company_name)  # URL 编码

    conn = http.client.HTTPSConnection("sys.do2do.com")
    payload = ''
    headers = {}

    conn.request("GET",
                 f"/api/interface/haiyun/baseData/getApiTokenByName?username={username}&password={password}",
                 payload, headers)

    res = conn.getresponse()
    data = res.read()
    # print(data.decode("utf-8"))
    return json.loads(data.decode("utf-8"))

def apply_query_info(api_token:str,dif_url_str:str,flag=0):
    """
    获取查询信息
    :param api_token: 公司的api token
    :return: 查询信息json数据
    """
    conn = http.client.HTTPSConnection("sys.do2do.com")
    payload = ''
    headers = {
        'api-token': api_token
    }
    if flag == 0:
        conn.request("GET", f"/api/interface/haiyun/baseData/{dif_url_str}", payload, headers)
    else:
        conn.request("GET", f"/api/order/kdOrderBaseApi/{dif_url_str}", payload, headers)
    res = conn.getresponse()
    data = res.read()
    # 查看完整数据
    # print(data.decode("utf-8"))
    # 查看数据是否成功获取和状态码，成功获取code为200
    # print('message:',  json.loads(data.decode("utf-8"))['message'],  '  code:', json.loads(data.decode("utf-8"))['code'])
    return json.loads(data.decode("utf-8"))

def country_base_info(api_token:str):
    """
    获取各个国家的名称和对应信息
    :param api_token: 公司的api token
    :return: 国家信息的json数据
    """
    dif_url_str = 'queryInterfaceCountryList'

    return apply_query_info(api_token,dif_url_str)['result']

def fba_code_base_info(api_token:str):
    """
    查询FBA CODE基础数据
    :param api_token: 公司的api token
    :return:查询FBA CODE基础json数据
    """
    dif_url_str = 'queryInterfaceFbaList'
    return apply_query_info(api_token,dif_url_str)['result']

def st_store_base_info(api_token:str):
    """
    查询海外仓基础数据
    :param api_token: 公司的api token
    :return: 查询海外仓基础数据json数据
    """
    dif_url_str = 'queryInterfaceFbaThirdPartyList'
    return apply_query_info(api_token,dif_url_str)['result']

def mixed_list_basee_info(api_token:str):
    """
    查询混装编码基础数据
    :param api_token: 公司的api token
    :return: 查询混装编码基础数据json数据
    """
    dif_url_str = 'queryInterfaceMixedList'
    return apply_query_info(api_token,dif_url_str)['result']

#查询海云订单渠道基础数据
def st_order_channel_base_info(api_token:str):
    """
    查询海云订单渠道基础数据
    :param api_token: 公司的api token
    :return: 查询海云订单渠道基础数据json数据
    """
    dif_url_str = 'queryInterfaceOrderChannelList?isHaiyun='
    return apply_query_info(api_token,dif_url_str)['result']

# 查询海运订单报关基础数据
def st_order_declare_base_info(api_token:str):
    """
    查询海运订单报关基础数据
    :param api_token: 公司的api token
    :return: 查询海云订单申报基础数据json数据
    """
    dif_url_str = 'queryInterfaceOrderDeclareTypeList'
    return apply_query_info(api_token,dif_url_str)['result']

# 查询海运订单交货仓库基础数据
def st_order_delivery_warehouse(api_token:str):
    """
    查询海运订单交货仓库基础数据
    :param api_token:公司的api token
    :return:海运订单交货仓库json数据
    """
    dif_url_str = 'queryInterfaceOrderDeliveryPointList'
    return apply_query_info(api_token,dif_url_str)['result']

# 查询海运订单文件类型基础数据
def st_order_file_type_base_info(api_token:str):
    """
    查询海运订单文件类型基础数据
    :param api_token: 公司的api token
    :return: 海运订单文件类型基础数据json数据
    """
    dif_url_str = 'queryInterfaceOrderFileTypeList'
    return apply_query_info(api_token,dif_url_str)['result']

#查询单位基础数据
def unit_base_info(api_token:str):
    """
    查询单位基础数据
    :param api_token: 公司的api token
    :return: 单位基础数据json数据
    """
    dif_url_str = 'queryInterfaceUnitList'
    return apply_query_info(api_token,dif_url_str)['result']

# 获取用户私人or商业地址基础数据
def user_address_base_info(api_token:str):
    """
    获取用户私人or商业地址基础数据
    :param api_token: 公司的api token
    :return: 用户私人or商业地址基础数据json数据
    """
    dif_url_str = 'queryAddressList?isBussiness=0'
    return apply_query_info(api_token,dif_url_str,1)['result']

# 查询快递订单渠道名称基础数据
def kd_order_channel_base_info(api_token:str):
    """
    查询快递订单渠道名称基础数据
    :param api_token: 公司的api token
    :return: 快递订单渠道名称基础数据json数据
    """
    dif_url_str = 'queryKdOrderChannelList'
    return apply_query_info(api_token,dif_url_str,1)['result']

# 查询快递订单交货仓库基础数据
def kd_order_delivery_warehouse_base_info(api_token:str):
    """
    查询快递订单交货仓库基础数据
    :param api_token: 公司的api token
    :return: 快递订单交货仓库基础数据json数据
    """
    dif_url_str = 'queryKdOrderDeliveryPoints'
    return apply_query_info(api_token,dif_url_str,1)['result']



if __name__ == '__main__':
    token_str = user_api_token(token_user_name,token_password)['result']
    print(kd_order_delivery_warehouse_base_info(token_str))






