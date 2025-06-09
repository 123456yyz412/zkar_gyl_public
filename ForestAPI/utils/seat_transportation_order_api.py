"""
海运订单接口api
"""
import http.client
import json
from ForestAPI.forest_api_config.forest_config import token_user_name,token_password
from ForestAPI.utils.base_api import user_api_token


#创建海运订单2，创建成功返回订单号
def create_sea_transportation_order(api_token: str):
    """
    创建海运订单2，创建成功返回订单号
    :param api_token: 公司的api token
    :return: 创建成功的订单号
    """
    conn = http.client.HTTPSConnection("sys.do2do.com")
    payload = json.dumps({
        "containerType": "string",
        "orderNo": "string",
        "cusId": "string",
        "customer": "string",
        "shipmentCountry": "string",
        "channelName": "string",
        "channelId": "string",
        "lockChayan": "string",
        "packageType": "string",
        "hasTax": "string",
        "withinOrderRemark": "string",
        "sanLock": "string",
        "shipmentType": "string",
        "shipmentCode": "string",
        "state": "string",
        "city": "string",
        "shipmentZip": "string",
        "shipmentAddress": "string",
        "contact": "string",
        "tel": "string",
        "shipmentCompany": "string",
        "deliveryPointName": "string",
        "deliveryPoint": "string",
        "shipperName": "string",
        "shipperAddress": "string",
        "shipperContact": "string",
        "shipperTel": "string",
        "shipperEmail": "string",
        "insurance": "string",
        "insuranceFee": 0,
        "insured": "string",
        "insuranceCur": "string",
        "insuranceRemark": "string",
        "deliveryRemark": "string",
        "declareTypeName": "string",
        "declareType": "string",
        "blNumber": 0,
        "isRucang": "string",
        "expetTime": "string",
        "isBattery": "0",
        "isSui": "0",
        "isZheng": "0",
        "isDischarge": "string",
        "isYuyue": "string",
        "yuyueRemark": "string",
        "fileList": [
            {
                "id": "string",
                "name": "string",
                "url": "string",
                "size": 0,
                "type": "string",
                "ext": "string"
            }
        ],
        "goodsList": [
            {
                "shipmentId": "string",
                "referenceId": "string",
                "name": "string",
                "ename": "string",
                "hscode": "string",
                "taxRate": "string",
                "material": "string",
                "purpose": "string",
                "addtionalCode": "string",
                "unitCost": 0,
                "unitValue": 0,
                "qty": 0,
                "ctn": 0,
                "nkg": 0,
                "kg": 0,
                "cmb": 0,
                "maker": "string",
                "makerAdd": "string",
                "saleLink": "string",
                "mixed": "string",
                "remark": "string",
                "asin": "string",
                "fnSku": "string",
                "amazonIdNum": "string"
            }
        ],
        "whType": "string",
        "froRemark": "string",
        "deliveryType": "string",
        "hyDeliveryType": "string"
    })
    headers = {
        'api-token': api_token,
        'Content-Type': 'application/json'
    }
    conn.request("POST", "/api/interface/haiyun/order/createHyOrder2", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))

# 获取forestApi文档链接
def get_forest_api_doc(api_token: str):
    """
    获取forestApi文档链接
    :param api_token: 公司的api token
    :return:
    """
    conn = http.client.HTTPSConnection("sys.do2do.com")
    payload = ''
    headers = {
        'api-token': api_token
    }
    conn.request("GET", "/api/interface/haiyun/order/getApiUrl", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))

# 查询订单物流轨迹
def query_order_logistics_track(api_token: str,order_no:  str):
    """
    查询订单物流轨迹
    :param api_token: 公司的api token
    :param order_no: 物流单号
    :return:
    """
    conn = http.client.HTTPSConnection("sys.do2do.com")
    payload = ''
    headers = {
        'api-token': api_token
    }
    conn.request("GET", f"/api/interface/haiyun/order/getOrderStatus?orderNo={order_no}", payload, headers)
    res = conn.getresponse()
    data = res.read()
    if json.loads(data.decode("utf-8"))['code'] == 200:
        return json.loads(data.decode("utf-8"))['result']
    else:
        return None

# 查询订单详情
def query_order_detail(api_token: str,order_no: list):
    """
    查询订单详情
    :param api_token: 公司的api token
    :param order_no: 物流单号的列表
    :return: 订单的详情信息列表
    """
    conn = http.client.HTTPSConnection("sys.do2do.com")
    payload = json.dumps({
        "orderNos":
            order_no

    })
    headers = {
        'api-token': api_token,
        'Content-Type': 'application/json'
    }
    conn.request("POST", "/api/interface/haiyun/order/getHyOrderDetail", payload, headers)
    res = conn.getresponse()
    data = res.read()
    if json.loads(data.decode("utf-8"))['code'] == 200:
        return json.loads(data.decode("utf-8"))['result']
    else:
        return None

# 查询客户材积数据
def query_customer_cubic_data(api_token: str,order_no: list):
    """
    查询客户材积数据
    :param api_token: 公司的api token
    :param order_no: 物流单号的列表
    :return: 客户的材积数据
    """
    conn = http.client.HTTPSConnection("sys.do2do.com")
    payload = json.dumps({
        "orderNos":
            order_no
    })
    headers = {
        'api-token': api_token,
        'Content-Type': 'application/json'
    }
    conn.request("POST", "/api/interface/haiyun/order/getHyOrderVolumeDetail", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))
    if json.loads(data.decode("utf-8"))['code'] == 200:
        return json.loads(data.decode("utf-8"))['result']
    else:
        return None

if __name__ == '__main__':
    token_str = user_api_token(token_user_name, token_password)['result']
    # get_forest_api_doc(token_str)
    print(query_order_detail(token_str, ["FSHY2505052340","FSHY2505032770"]))
    print(query_customer_cubic_data(token_str, ["FSHY2505052340","FSHY2505032770"]))
