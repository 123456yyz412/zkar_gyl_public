"""
获取token
"""
import redis
import requests
from skjAPI.shukuajing_config.skj_config import api_url,accessKeyId,accessKeySecret,headers,service_ipv4,service_port_redis


def get_api_keys_by_token():
    """
    通过API的GET请求获取临时令牌
    :param api_url: API的URL地址
    :return: 临时令牌
    """

    api_id_key_str = f"/decision/api/v1/corp/oauth/token?accessKeyId={accessKeyId}&accessKeySecret={accessKeySecret}"
    try:
        response = requests.get(api_url + api_id_key_str, headers=headers)
        response.raise_for_status()  # 如果响应状态码不是200，抛出异常
        data = response.json()
        # 获取 data 字典中的 'data' 键对应的值，并确保其存在，默认为空字符串
        token_str = str(data.get('data', ''))  # 使用 get 方法避免 KeyError 异常

        return token_str
    except requests.exceptions.RequestException as e:
        print(f"Error fetching API token: {e}")
        return None


def get_token_from_service_redis():
    """
    从服务器的redis中获取
    注意，service_port_redis需要去服务器打开安全组限制,其次，redis默认本地访问，需要去配置 redis.conf文件修改权限
    bind 0.0.0.0
    protected-mode no
    port 6379
    daemonize yes
    修改后重启redis
    :return:
    """
    r = redis.Redis(host=service_ipv4, port=service_port_redis, db=0)
    token_str = str(r.get('token')).split("'")[1]
    return token_str



if __name__ == '__main__':
    print(get_token_from_service_redis())
