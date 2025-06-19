from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
import pandas as pd
from collections import OrderedDict

data = '''
#施璐
a001 = [164000,180400]
#董倩
a002 = [142000,156200]
#黄美璇
a003 = [132000,145200]
#刘林萍
a004 = [65000,71500]
#钱正宇
a005 = [65000,71500]
#张竹青
a006 = [57000,62700]
#李信
a007 = [55000,60500]
#陈佳燕
a008 = [50000,55000]
#彭霞
a009 = [45000,49500]
#蒙昌平
a010 = [45000,49500]
#邹慧
a011 = [43000,47300]
#黄美婷
a012 = [36401,40041.1]
#黄雪明
a013 = [36000,39600]
#谢小蓉
a014 = [36000,39600]
'''
# 数值字典: {'a001': [179000, 268500], 'a002': [154962, 232443], 'a003': [145000, 217500], 'a004': [72000, 108000], 'a005': [66000, 99000], 'a006': [62000, 93000], 'a007': [61000, 91500], 'a008': [50000, 75000], 'a009': [50000, 75000], 'a010': [50000, 75000], 'a011': [50000, 75000], 'a012': [50000, 75000], 'a013': [36000, 54000], 'a014': [18000, 27000]}
targets_dic = {}
# 姓名字典: {'a001': '施璐', 'a002': '董倩', 'a003': '黄美璇', 'a004': '刘林萍', 'a005': '钱正宇', 'a006': '张竹青', 'a007': '李信', 'a008': '黄美婷', 'a009': '蒙昌平', 'a010': '彭霞', 'a011': '邹慧', 'a012': '陈佳燕', 'a013': '黄雪明', 'a014': '朱伟豪'}
names_dic = {}


def parse_data(data):
    current_name = ""

    for line in data.strip().split('\n'):
        if line.startswith('#'):
            current_name = line[1:]
        else:
            var_name, values = line.split(' = ')
            var = var_name.strip()
            targets_dic[var] = eval(values)
            names_dic[var] = current_name


# 创建全局引擎（单例模式）- 兼容2.0语法
_engine = create_engine(
    f"mysql+mysqlconnector://u123:u123@127.0.0.1:3306/model",
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


def sales_targets_principle():
    try:
        with get_mysql_session() as session:
            # 查询所有item_id
            query_item_links = """SELECT t.sku
      ,t.deng_ji
      ,t.dan_jia
      ,t.zai_ku_ku_cun
      ,t.sku_xiao_shou_mu_biao
      ,CONCAT(IF(t.a001 is not null,'a001,',''),IF(t.a002 is not null,'a002,',''),IF(t.a003 is not null,'a003,',''),IF(t.a004 is not null,'a004,',''),IF(t.a005 is not null,'a005,',''),IF(t.a006 is not null,'a006,',''),IF(t.a007 is not null,'a007,',''),IF(t.a008 is not null,'a008,',''),IF(t.a009 is not null,'a009,',''),IF(t.a010 is not null,'a010,',''),IF(t.a011 is not null,'a011,',''),IF(t.a012 is not null,'a012,',''),IF(t.a013 is not null,'a013,',''),IF(t.a014 is not null,'a014,','')) as principal
  FROM model.dim_sku_sales_target_principal t 
ORDER BY t.deng_ji asc,t.sku_xiao_shou_mu_biao desc

 """
            df = pd.read_sql(query_item_links, session.connection())
        return df
    except Exception as e:
        print(f"Error: {e}")


def select_undistributed_sku():
    """

    :param emo_id:
    :return:
    """
    try:
        with get_mysql_session() as session:
            # 查询所有item_id
            stmt = text(""" SELECT sku,deng_ji,dan_jia,zai_ku_ku_cun,sku_xiao_shou_mu_biao,last_sales_num,a001,a002,a003,a004,a005,a006,a007,a008,a009,a010,a011,a012,a013,a014,a015
  FROM model.temp_sku_sales_target_principal02 t
 WHERE ((t.a001  = 0) OR (t.a002  = 0) OR (t.a003  = 0) OR (t.a004  = 0) OR (t.a005  = 0) OR (t.a006  = 0) OR (t.a007  = 0) OR (t.a008  = 0) OR (t.a009  = 0) OR (t.a010  = 0) OR (t.a011  = 0) OR (t.a012  = 0) OR (t.a013  = 0) OR (t.a014  = 0))
   and t.zai_ku_ku_cun <> 0
	 ORDER BY t.deng_ji ASC,t.sku_xiao_shou_mu_biao desc;""")
            # df = pd.read_sql(stmt, session.connection(),params={':_emo_id':emo_id})
            df = pd.read_sql(stmt, session.connection())
        return df
    except Exception as e:
        print(f"Error: {e}")


def write_target_sales_to_msyql(data_dic: dict, insert_talble: str):
    """
    插入数据到数据库销售目标表里面
    :param data_dic:
    :return:
    """
    try:
        isnert_data_dic = {'sku': None, 'deng_ji': None, 'dan_jia': None, 'zai_ku_ku_cun': None,
                           'sku_xiao_shou_mu_biao': None, 'last_sales_num': None, 'a001': None,
                           'a002': None, 'a003': None, 'a004': None, 'a005': None, 'a006': None, 'a007': None,
                           'a008': None, 'a009': None, 'a010': None,
                           'a011': None, 'a012': None, 'a013': None, 'a014': None}
        for key, value in data_dic.items():
            isnert_data_dic[key] = value
        with get_mysql_session() as session:
            stmt = text(f"""insert into {insert_talble}
            (sku,deng_ji,dan_jia,zai_ku_ku_cun,sku_xiao_shou_mu_biao,last_sales_num
            ,a001,a002,a003,a004,a005,a006,a007,a008,a009,a010,a011,a012,a013,a014)
            values (
            :_sku,:_deng_ji,:_dan_jia,:_zai_ku_ku_cun,:_sku_xiao_shou_mu_biao,:_last_sales_num
            ,:_a001,:_a002,:_a003,:_a004,:_a005,:_a006,:_a007,:_a008,:_a009,:_a010,:_a011,:_a012,:_a013,:_a014
            )
            """)
            session.execute(stmt, {'_sku': isnert_data_dic['sku'],
                                   '_deng_ji': isnert_data_dic['deng_ji'],
                                   '_dan_jia': isnert_data_dic['dan_jia'],
                                   '_zai_ku_ku_cun': isnert_data_dic['zai_ku_ku_cun'],
                                   '_sku_xiao_shou_mu_biao': isnert_data_dic['sku_xiao_shou_mu_biao'],
                                   '_last_sales_num': isnert_data_dic['last_sales_num'],
                                   '_a001': isnert_data_dic['a001'],
                                   '_a002': isnert_data_dic['a002'],
                                   '_a003': isnert_data_dic['a003'],
                                   '_a004': isnert_data_dic['a004'],
                                   '_a005': isnert_data_dic['a005'],
                                   '_a006': isnert_data_dic['a006'],
                                   '_a007': isnert_data_dic['a007'],
                                   '_a008': isnert_data_dic['a008'],
                                   '_a009': isnert_data_dic['a009'],
                                   '_a010': isnert_data_dic['a010'],
                                   '_a011': isnert_data_dic['a011'],
                                   '_a012': isnert_data_dic['a012'],
                                   '_a013': isnert_data_dic['a013'],
                                   '_a014': isnert_data_dic['a014']})
    except Exception as e:
        print(f"Error: {e}")


def truncate_table(table_name: str):
    """
    清空SKU销售目标表
    :return:
    """
    try:
        with get_mysql_session() as session:
            # 查询所有item_id
            stmt = text(f"""truncate table model.{table_name}""")
            result = session.execute(stmt)
    except Exception as e:
        print(f"Error: {e}")


def get_employ_target(index: int):
    """
    根据index获取目标id
    :param index:0是自己定的目标，既下线，索引1是目标上线，暂定分配到个人的1.5倍
    :return:
    """
    pass


def subtract_targets_dic_values(emoployee_id: str, number: int, dan_jia: float):
    """

    :param emoployee_id: 员工id，需要
    :param number:
    :return:
    """
    try:
        # 如果目标销量上线大于0，则减少目标销量
        if targets_dic[emoployee_id][0] > 0:
            targets_dic[emoployee_id] = [targets_dic[emoployee_id][0] - number * dan_jia,
                                         targets_dic[emoployee_id][1] - number * dan_jia]
            if targets_dic[emoployee_id][0] < 0:
                targets_dic[emoployee_id][0] = 0
    except Exception as e:
        print(f"Error: {e}")




def gradient_allocation_avg(x, y):
    """
    尽量平均分配，优先保证元素值从大到小，允许重复值
    :param x: 分配对象数量
    :param y: 可分配产品数量
    :return: 长度为x的列表，元素之和为y，元素值从大到小排列
    """
    if x <= 0 or y < 0:
        raise ValueError("分配对象数量必须大于0，可分配产品数量不能为负数")

    base = y // x        # 每个元素的基础值
    remainder = y % x    # 剩余数量，需分配到前几个元素

    result = [base] * x  # 初始化列表

    # 将余数分配到前几个元素
    for i in range(remainder):
        result[i] += 1

    return result

def gradient_allocation_decrease(x, y):
    """

    :param x: 分配对象数量
    :param y: 可分配商品数量
    :return: 列表
    """
    if x <= 0 or y < 0:
        raise ValueError("分配对象数量必须大于0，可分配产品数量不能为负数")

    base = y // x
    remainder = y % x

    result = [base] * x

    # 分配余数，使前几个元素+1，保持大致递减趋势
    for i in range(remainder):
        result[i] += 1

    # 降序排列以保证“梯度递减”的视觉效果
    result.sort(reverse=True)
    return result


def gradient_distribution12345():
    """
    对滞销冗余的可分配库存进行梯度分配
    :return:
    """
    truncate_table('temp_sku_sales_target_principal03')
    available_df = residual_inventory_unsalable12345()
    for index, row in available_df.iterrows():
        # 梯度算法着重于人员的顺序
        # 保持顺序,按照总体目标值进行降序排序，如果都减到0一下，都是相同0值，按照最大目标上限进行排序
        not_achieved_targets_dic = dict(
            sorted(targets_dic.items(),
                   key=lambda item: (-item[1][0], -item[1][1]))
        )
        write_temp_table_dic = {'sku': row['sku'], 'deng_ji': row['deng_ji'], 'dan_jia': row['dan_jia'],
                                'zai_ku_ku_cun': row['zai_ku_ku_cun'],
                                'sku_xiao_shou_mu_biao': row['sku_xiao_shou_mu_biao'],
                                'last_sales_num': row['residual_inventory']}
        principle_list = row['principal'].split(',')


        # 判断基础目标是否完成分配，如果这个sku5个人负责，其中2个未达成梯度分配，sku要分配5个，则分配列表[3,2]+[0,0,0],结果[3,2,0,0,0]
        x1 = 0
        x2_list = []
        for key, value in not_achieved_targets_dic.items():
            if key in principle_list:
                if float(value[0]) > 0:
                    x1 += 1
                else:
                    x2_list.append(0)
        if  x1 > 0:
            value_ordered_list = gradient_allocation_decrease(x1, int(row['residual_inventory'])) + x2_list
        else:
            value_ordered_list = gradient_allocation_decrease(len(principle_list), int(row['residual_inventory']))



        # 调整顺序，因为principle_list是从a001到a014次序
        for key in not_achieved_targets_dic.keys():
            if key in principle_list:
                write_temp_table_dic[key] = value_ordered_list[0]

                targets_dic[key][0] -= value_ordered_list[0] * row['dan_jia']
                # if targets_dic[key][0] <= 0:
                #     targets_dic[key][0] = 0
                targets_dic[key][1] -= value_ordered_list[0] * row['dan_jia']
                # if targets_dic[key][1] <= 0:
                #     targets_dic[key][1] = 0
                #     del write_temp_table_dic[key]

                value_ordered_list.pop(0)

        write_target_sales_to_msyql(write_temp_table_dic, 'temp_sku_sales_target_principal03')


def gradient_distribution6():
    """
    对滞销冗余的可分配库存进行梯度分配
    :return:
    """
    truncate_table('temp_sku_sales_target_principal01')
    available_df = residual_inventory_unsalable6()
    for index, row in available_df.iterrows():
        # 梯度算法着重于人员的顺序
        # 保持顺序,按照总体目标值进行降序排序，如果都减到0一下，都是相同0值，按照最大目标上限进行排序
        not_achieved_targets_dic = dict(
            sorted(targets_dic.items(),
                   key=lambda item: (-item[1][0], -item[1][1]))
        )
        write_temp_table_dic = {'sku': row['sku'], 'deng_ji': row['deng_ji'], 'dan_jia': row['dan_jia'],
                                'zai_ku_ku_cun': row['zai_ku_ku_cun'],
                                'sku_xiao_shou_mu_biao': row['sku_xiao_shou_mu_biao'],
                                'last_sales_num': row['residual_inventory']}
        principle_list = row['principal'].split(',')

        value_ordered_list = gradient_allocation_avg(len(principle_list), row['residual_inventory'])

        # 调整顺序，因为principle_list是从a001到a014次序
        for key in not_achieved_targets_dic.keys():
            if key in principle_list:
                write_temp_table_dic[key] = value_ordered_list[0]

                targets_dic[key][0] -= value_ordered_list[0] * row['dan_jia']
                # if targets_dic[key][0] <= 0:
                #     targets_dic[key][0] = 0
                targets_dic[key][1] -= value_ordered_list[0] * row['dan_jia']
                # if targets_dic[key][1] <= 0:
                #     targets_dic[key][1] = 0
                #     del write_temp_table_dic[key]

                value_ordered_list.pop(0)

        write_target_sales_to_msyql(write_temp_table_dic, 'temp_sku_sales_target_principal01')

def gradient_distribution_last_target():
    """
    对滞销冗余的可分配库存进行梯度分配
    :return:
    """
    truncate_table('temp_sku_sales_target_principal04')
    available_df = residual_inventory_unsalable_last()
    for index, row in available_df.iterrows():
        # 梯度算法着重于人员的顺序
        # 目标未完成的字典
        ac_principle_dic = {}
        for key,value in targets_dic.items():
            if value[0] > 0:
                ac_principle_dic[key] = value[0]
        write_temp_table_dic = {'sku': row['sku'], 'deng_ji': row['deng_ji'], 'dan_jia': row['dan_jia'],
                                'zai_ku_ku_cun': row['zai_ku_ku_cun'],
                                'sku_xiao_shou_mu_biao': row['sku_xiao_shou_mu_biao'],
                                'last_sales_num': row['res_num']}
        principle_list = row['principal'].split(',')

        x = 0
        new_dic = {}
        for key, value in ac_principle_dic.items():
            if key in principle_list:
               x += 1
               new_dic[key] = value
        sorted_dict = dict(sorted(new_dic.items(), key=lambda item: item[1], reverse=True))

        if x>0:
            value_ordered_list = gradient_allocation_decrease(x, int(row['res_num']))
            for k,v in sorted_dict.items():
                write_temp_table_dic[k] = value_ordered_list[0]
                targets_dic[k][0] -= value_ordered_list[0] * row['dan_jia']
                targets_dic[k][1] -= value_ordered_list[0] * row['dan_jia']
                value_ordered_list.pop(0)


            write_target_sales_to_msyql(write_temp_table_dic, 'temp_sku_sales_target_principal04')

def gradient_distribution_last_target_force():
    """
    对滞销冗余的可分配库存进行梯度分配
    :return:
    """
    truncate_table('temp_sku_sales_target_principal04')
    available_df = residual_inventory_unsalable_last_force()
    for index, row in available_df.iterrows():
        # 梯度算法着重于人员的顺序
        # 目标未完成的字典
        ac_principle_dic = {}
        for key,value in targets_dic.items():
            ac_principle_dic[key] = value[0]
        write_temp_table_dic = {'sku': row['sku'], 'deng_ji': row['deng_ji'], 'dan_jia': row['dan_jia'],
                                'zai_ku_ku_cun': row['zai_ku_ku_cun'],
                                'sku_xiao_shou_mu_biao': row['sku_xiao_shou_mu_biao'],
                                'last_sales_num': row['res_num']}
        principle_list = row['principal'].split(',')

        x = 0
        new_dic = {}
        for key, value in ac_principle_dic.items():
            if key in principle_list:
               x += 1
               new_dic[key] = value
        sorted_dict = dict(sorted(new_dic.items(), key=lambda item: item[1], reverse=True))

        if x>0:
            value_ordered_list = gradient_allocation_decrease(x, int(row['res_num']))
            for k,v in sorted_dict.items():
                write_temp_table_dic[k] = value_ordered_list[0]
                targets_dic[k][0] -= value_ordered_list[0] * row['dan_jia']
                targets_dic[k][1] -= value_ordered_list[0] * row['dan_jia']
                value_ordered_list.pop(0)


            write_target_sales_to_msyql(write_temp_table_dic, 'temp_sku_sales_target_principal04')
def residual_inventory_unsalable12345():
    """
    计算可用库存：当前可以分配的库存数据 = 在库库存 - 已分配销售目标 - 4个月的销售数量
    滞销冗余库存分配 留足4个月的库存。如果fore_cast值是0则用sku_xiao_shou_mu_biao判断每个月的销售数据。优先级：等级>滞销数量
    :return:
    """
    try:
        with get_mysql_session() as session:
            stmt = text("""SELECT t.sku
      ,t.vc_type
			,t.deng_ji
			,t.dan_jia
			,t.zai_ku_ku_cun
			,t.forecast_values
			,t.sku_xiao_shou_mu_biao
			,t.total_target
			,IF((t.zai_ku_ku_cun - t.total_target - if(t.forecast_values = 0,t.sku_xiao_shou_mu_biao * 4, t.forecast_values * 4)) > t.sku_xiao_shou_mu_biao,t.sku_xiao_shou_mu_biao,(t.zai_ku_ku_cun - t.total_target - if(t.forecast_values = 0,t.sku_xiao_shou_mu_biao * 4, t.forecast_values * 4))) as residual_inventory 
			,TRIM(TRAILING ',' FROM t.principal) as principal
  FROM (
SELECT a.sku
      ,c.vc_type
      ,a.deng_ji
			,a.dan_jia
			,a.zai_ku_ku_cun
			,c.forecast_values
			,c.sku_xiao_shou_mu_biao
			,IF(a.a001 IS NULL,0,a.a001 )+
       IF(a.a002 IS NULL,0,a.a002 ) +
       IF(a.a003 IS NULL,0,a.a003 ) +
       IF(a.a004 IS NULL,0,a.a004 ) +
       IF(a.a005 IS NULL,0,a.a005 ) +
       IF(a.a006 IS NULL,0,a.a006 ) +
       IF(a.a007 IS NULL,0,a.a007 ) +
       IF(a.a008 IS NULL,0,a.a008 ) +
       IF(a.a009 IS NULL,0,a.a009 ) +
       IF(a.a010 IS NULL,0,a.a010 ) +
       IF(a.a011 IS NULL,0,a.a011 ) +
       IF(a.a012 IS NULL,0,a.a012 ) +
       IF(a.a013 IS NULL,0,a.a013 ) +
       IF(a.a014 IS NULL,0,a.a014 ) as total_target 
			 ,CONCAT(IF(c.a001 is not null,'a001,',''),IF(c.a002 is not null,'a002,',''),IF(c.a003 is not null,'a003,',''),IF(c.a004 is not null,'a004,',''),IF(c.a005 is not null,'a005,',''),IF(c.a006 is not null,'a006,',''),IF(c.a007 is not null,'a007,',''),IF(c.a008 is not null,'a008,',''),IF(c.a009 is not null,'a009,',''),IF(c.a010 is not null,'a010,',''),IF(c.a011 is not null,'a011,',''),IF(c.a012 is not null,'a012,',''),IF(c.a013 is not null,'a013,',''),IF(c.a014 is not null,'a014,','')) as principal
  FROM model.temp_sku_sales_target_principal02 a 
	JOIN model.dim_sku_sales_target_principal c 
	  on a.sku = c.sku
ORDER BY a.deng_ji) t 
WHERE (t.zai_ku_ku_cun - t.total_target - if(t.forecast_values = 0,t.sku_xiao_shou_mu_biao * 4, t.forecast_values * 4)) > 0
  AND t.deng_ji <> 6
ORDER BY t.deng_ji ASC,(t.zai_ku_ku_cun - t.total_target - if(t.forecast_values = 0,t.sku_xiao_shou_mu_biao * 4, t.forecast_values * 4)) DESC
            """)
            df = pd.read_sql(stmt, session.connection())
            return df
    except Exception as e:
        print(e)

def residual_inventory_unsalable6():
    """
    计算可用库存：当前可以分配的库存数据 = 在库库存 - 已分配销售目标 - 4个月的销售数量
    滞销冗余库存分配 留足4个月的库存。如果fore_cast值是0则用sku_xiao_shou_mu_biao判断每个月的销售数据。优先级：等级>滞销数量
    :return:
    """
    try:
        with get_mysql_session() as session:
            stmt = text("""SELECT t.sku
      ,t.vc_type
			,t.deng_ji
			,t.dan_jia
			,t.zai_ku_ku_cun
			,t.forecast_values
			,t.sku_xiao_shou_mu_biao
			,t.total_target
			,IF((t.zai_ku_ku_cun - t.total_target - if(t.forecast_values = 0,t.sku_xiao_shou_mu_biao * 4, t.forecast_values * 4)) > t.sku_xiao_shou_mu_biao,t.sku_xiao_shou_mu_biao,(t.zai_ku_ku_cun - t.total_target - if(t.forecast_values = 0,t.sku_xiao_shou_mu_biao * 4, t.forecast_values * 4))) as residual_inventory 
			,TRIM(TRAILING ',' FROM t.principal) as principal
  FROM (
SELECT a.sku
      ,c.vc_type
      ,a.deng_ji
      ,a.dan_jia
      ,a.zai_ku_ku_cun
      ,c.forecast_values
      ,c.sku_xiao_shou_mu_biao
      ,IF(a.a001 IS NULL,0,a.a001 )+
       IF(a.a002 IS NULL,0,a.a002 ) +
       IF(a.a003 IS NULL,0,a.a003 ) +
       IF(a.a004 IS NULL,0,a.a004 ) +
       IF(a.a005 IS NULL,0,a.a005 ) +
       IF(a.a006 IS NULL,0,a.a006 ) +
       IF(a.a007 IS NULL,0,a.a007 ) +
       IF(a.a008 IS NULL,0,a.a008 ) +
       IF(a.a009 IS NULL,0,a.a009 ) +
       IF(a.a010 IS NULL,0,a.a010 ) +
       IF(a.a011 IS NULL,0,a.a011 ) +
       IF(a.a012 IS NULL,0,a.a012 ) +
       IF(a.a013 IS NULL,0,a.a013 ) +
       IF(a.a014 IS NULL,0,a.a014 ) as total_target 
	  ,CONCAT(IF(c.a001 is not null,'a001,',''),IF(c.a002 is not null,'a002,',''),IF(c.a003 is not null,'a003,',''),IF(c.a004 is not null,'a004,',''),IF(c.a005 is not null,'a005,',''),IF(c.a006 is not null,'a006,',''),IF(c.a007 is not null,'a007,',''),IF(c.a008 is not null,'a008,',''),IF(c.a009 is not null,'a009,',''),IF(c.a010 is not null,'a010,',''),IF(c.a011 is not null,'a011,',''),IF(c.a012 is not null,'a012,',''),IF(c.a013 is not null,'a013,',''),IF(c.a014 is not null,'a014,','')) as principal
  FROM model.temp_sku_sales_target_principal02 a 
	JOIN model.dim_sku_sales_target_principal c 
	  on a.sku = c.sku
ORDER BY a.deng_ji) t 
WHERE (t.zai_ku_ku_cun - t.total_target - if(t.forecast_values = 0,t.sku_xiao_shou_mu_biao * 4, t.forecast_values * 4)) > 0
  AND t.deng_ji = 6
ORDER BY t.deng_ji ASC,(t.zai_ku_ku_cun - t.total_target - if(t.forecast_values = 0,t.sku_xiao_shou_mu_biao * 4, t.forecast_values * 4)) DESC
            """)
            df = pd.read_sql(stmt, session.connection())
            return df
    except Exception as e:
        print(e)

def residual_inventory_unsalable_last():
    """
    计算可用库存：当前可以分配的库存数据 = 在库库存 - 已分配销售目标 - 4个月的销售数量
    滞销冗余库存分配 留足4个月的库存。如果fore_cast值是0则用sku_xiao_shou_mu_biao判断每个月的销售数据。优先级：等级>滞销数量
    :return:
    """
    try:
        with get_mysql_session() as session:
            stmt = text("""SELECT t.sku
      ,t.vc_type
      ,t.deng_ji
			,t.dan_jia
			,t.zai_ku_ku_cun
			,t.forecast_values
			,t.sku_xiao_shou_mu_biao 
			,t.total_target
			,ROUND(t.zai_ku_ku_cun/5,0) -t.total_target as res_num
			,CONCAT(IF(t.a001 is not null,'a001,',''),IF(t.a002 is not null,'a002,',''),IF(t.a003 is not null,'a003,',''),IF(t.a004 is not null,'a004,',''),IF(t.a005 is not null,'a005,',''),IF(t.a006 is not null,'a006,',''),IF(t.a007 is not null,'a007,',''),IF(t.a008 is not null,'a008,',''),IF(t.a009 is not null,'a009,',''),IF(t.a010 is not null,'a010,',''),IF(t.a011 is not null,'a011,',''),IF(t.a012 is not null,'a012,',''),IF(t.a013 is not null,'a013,',''),IF(t.a014 is not null,'a014,','')) as principal
FROM(
SELECT a.sku
      ,d.vc_type
      ,a.deng_ji
			,a.dan_jia
			,a.zai_ku_ku_cun
			,d.forecast_values
			,d.sku_xiao_shou_mu_biao
			,(IF(a.a001 IS NULL,0,a.a001 ) + IF(b.a001 IS NULL,0,b.a001 ) + IF(c.a001 IS NULL,0,c.a001 ))+
       (IF(a.a002 IS NULL,0,a.a002 ) + IF(b.a002 IS NULL,0,b.a002 ) + IF(c.a002 IS NULL,0,c.a002 ))+
       (IF(a.a003 IS NULL,0,a.a003 ) + IF(b.a003 IS NULL,0,b.a003 ) + IF(c.a003 IS NULL,0,c.a003 ))+
       (IF(a.a004 IS NULL,0,a.a004 ) + IF(b.a004 IS NULL,0,b.a004 ) + IF(c.a004 IS NULL,0,c.a004 ))+
       (IF(a.a005 IS NULL,0,a.a005 ) + IF(b.a005 IS NULL,0,b.a005 ) + IF(c.a005 IS NULL,0,c.a005 ))+
       (IF(a.a006 IS NULL,0,a.a006 ) + IF(b.a006 IS NULL,0,b.a006 ) + IF(c.a006 IS NULL,0,c.a006 ))+
       (IF(a.a007 IS NULL,0,a.a007 ) + IF(b.a007 IS NULL,0,b.a007 ) + IF(c.a007 IS NULL,0,c.a007 ))+
       (IF(a.a008 IS NULL,0,a.a008 ) + IF(b.a008 IS NULL,0,b.a008 ) + IF(c.a008 IS NULL,0,c.a008 ))+
       (IF(a.a009 IS NULL,0,a.a009 ) + IF(b.a009 IS NULL,0,b.a009 ) + IF(c.a009 IS NULL,0,c.a009 ))+
       (IF(a.a010 IS NULL,0,a.a010 ) + IF(b.a010 IS NULL,0,b.a010 ) + IF(c.a010 IS NULL,0,c.a010 ))+
       (IF(a.a011 IS NULL,0,a.a011 ) + IF(b.a011 IS NULL,0,b.a011 ) + IF(c.a011 IS NULL,0,c.a011 ))+
       (IF(a.a012 IS NULL,0,a.a012 ) + IF(b.a012 IS NULL,0,b.a012 ) + IF(c.a012 IS NULL,0,c.a012 ))+
       (IF(a.a013 IS NULL,0,a.a013 ) + IF(b.a013 IS NULL,0,b.a013 ) + IF(c.a013 IS NULL,0,c.a013 ))+
       (IF(a.a014 IS NULL,0,a.a014 ) + IF(b.a014 IS NULL,0,b.a014 ) + IF(c.a014 IS NULL,0,c.a014 ))	as total_target
			,IF(a.a001 IS NULL,NULL,(IF(a.a001 IS NULL,0,a.a001 ) + IF(b.a001 IS NULL,0,b.a001 ) + IF(c.a001 IS NULL,0,c.a001 ))) as a001
      ,IF(a.a002 IS NULL,NULL,(IF(a.a002 IS NULL,0,a.a002 ) + IF(b.a002 IS NULL,0,b.a002 ) + IF(c.a002 IS NULL,0,c.a002 ))) as a002
      ,IF(a.a003 IS NULL,NULL,(IF(a.a003 IS NULL,0,a.a003 ) + IF(b.a003 IS NULL,0,b.a003 ) + IF(c.a003 IS NULL,0,c.a003 ))) as a003
      ,IF(a.a004 IS NULL,NULL,(IF(a.a004 IS NULL,0,a.a004 ) + IF(b.a004 IS NULL,0,b.a004 ) + IF(c.a004 IS NULL,0,c.a004 ))) as a004
      ,IF(a.a005 IS NULL,NULL,(IF(a.a005 IS NULL,0,a.a005 ) + IF(b.a005 IS NULL,0,b.a005 ) + IF(c.a005 IS NULL,0,c.a005 ))) as a005
      ,IF(a.a006 IS NULL,NULL,(IF(a.a006 IS NULL,0,a.a006 ) + IF(b.a006 IS NULL,0,b.a006 ) + IF(c.a006 IS NULL,0,c.a006 ))) as a006
      ,IF(a.a007 IS NULL,NULL,(IF(a.a007 IS NULL,0,a.a007 ) + IF(b.a007 IS NULL,0,b.a007 ) + IF(c.a007 IS NULL,0,c.a007 ))) as a007
      ,IF(a.a008 IS NULL,NULL,(IF(a.a008 IS NULL,0,a.a008 ) + IF(b.a008 IS NULL,0,b.a008 ) + IF(c.a008 IS NULL,0,c.a008 ))) as a008
      ,IF(a.a009 IS NULL,NULL,(IF(a.a009 IS NULL,0,a.a009 ) + IF(b.a009 IS NULL,0,b.a009 ) + IF(c.a009 IS NULL,0,c.a009 ))) as a009
      ,IF(a.a010 IS NULL,NULL,(IF(a.a010 IS NULL,0,a.a010 ) + IF(b.a010 IS NULL,0,b.a010 ) + IF(c.a010 IS NULL,0,c.a010 ))) as a010
      ,IF(a.a011 IS NULL,NULL,(IF(a.a011 IS NULL,0,a.a011 ) + IF(b.a011 IS NULL,0,b.a011 ) + IF(c.a011 IS NULL,0,c.a011 ))) as a011
      ,IF(a.a012 IS NULL,NULL,(IF(a.a012 IS NULL,0,a.a012 ) + IF(b.a012 IS NULL,0,b.a012 ) + IF(c.a012 IS NULL,0,c.a012 ))) as a012
      ,IF(a.a013 IS NULL,NULL,(IF(a.a013 IS NULL,0,a.a013 ) + IF(b.a013 IS NULL,0,b.a013 ) + IF(c.a013 IS NULL,0,c.a013 ))) as a013
      ,IF(a.a014 IS NULL,NULL,(IF(a.a014 IS NULL,0,a.a014 ) + IF(b.a014 IS NULL,0,b.a014 ) + IF(c.a014 IS NULL,0,c.a014 ))) as a014
  FROM model.temp_sku_sales_target_principal02 a 
LEFT JOIN model.temp_sku_sales_target_principal01 b 
    ON a.sku = b.sku
LEFT JOIN model.temp_sku_sales_target_principal03 c
    ON a.sku = c.sku
LEFT JOIN model.dim_sku_sales_target_principal d 
    ON a.sku = d.sku 
ORDER BY a.deng_ji) t 
where (t.zai_ku_ku_cun - t.total_target - 4*forecast_values) > 0
  AND ROUND(t.zai_ku_ku_cun/5,0) -t.total_target >0
ORDER BY t.deng_ji ASC, (ROUND(t.zai_ku_ku_cun/5,0) -t.total_target) DESC
            """)
            df = pd.read_sql(stmt, session.connection())
            return df
    except Exception as e:
        print(e)

def residual_inventory_unsalable_last_force():
    """
    计算可用库存：当前可以分配的库存数据 = 在库库存 - 已分配销售目标 - 4个月的销售数量
    滞销冗余库存分配 留足4个月的库存。如果fore_cast值是0则用sku_xiao_shou_mu_biao判断每个月的销售数据。优先级：等级>滞销数量
    :return:
    """
    try:
        with get_mysql_session() as session:
            stmt = text("""SELECT t.sku
      ,t.vc_type
      ,t.deng_ji
			,t.dan_jia
			,t.zai_ku_ku_cun
			,t.forecast_values
			,t.sku_xiao_shou_mu_biao 
			,t.total_target
			,ROUND(t.zai_ku_ku_cun/5,0) -t.total_target as res_num
			,CONCAT(IF(t.a001 is not null,'a001,',''),IF(t.a002 is not null,'a002,',''),IF(t.a003 is not null,'a003,',''),IF(t.a004 is not null,'a004,',''),IF(t.a005 is not null,'a005,',''),IF(t.a006 is not null,'a006,',''),IF(t.a007 is not null,'a007,',''),IF(t.a008 is not null,'a008,',''),IF(t.a009 is not null,'a009,',''),IF(t.a010 is not null,'a010,',''),IF(t.a011 is not null,'a011,',''),IF(t.a012 is not null,'a012,',''),IF(t.a013 is not null,'a013,',''),IF(t.a014 is not null,'a014,','')) as principal
FROM(
SELECT a.sku
      ,d.vc_type
      ,a.deng_ji
			,a.dan_jia
			,a.zai_ku_ku_cun
			,d.forecast_values
			,d.sku_xiao_shou_mu_biao
			,(IF(a.a001 IS NULL,0,a.a001 ) + IF(b.a001 IS NULL,0,b.a001 ) + IF(c.a001 IS NULL,0,c.a001 ))+
       (IF(a.a002 IS NULL,0,a.a002 ) + IF(b.a002 IS NULL,0,b.a002 ) + IF(c.a002 IS NULL,0,c.a002 ))+
       (IF(a.a003 IS NULL,0,a.a003 ) + IF(b.a003 IS NULL,0,b.a003 ) + IF(c.a003 IS NULL,0,c.a003 ))+
       (IF(a.a004 IS NULL,0,a.a004 ) + IF(b.a004 IS NULL,0,b.a004 ) + IF(c.a004 IS NULL,0,c.a004 ))+
       (IF(a.a005 IS NULL,0,a.a005 ) + IF(b.a005 IS NULL,0,b.a005 ) + IF(c.a005 IS NULL,0,c.a005 ))+
       (IF(a.a006 IS NULL,0,a.a006 ) + IF(b.a006 IS NULL,0,b.a006 ) + IF(c.a006 IS NULL,0,c.a006 ))+
       (IF(a.a007 IS NULL,0,a.a007 ) + IF(b.a007 IS NULL,0,b.a007 ) + IF(c.a007 IS NULL,0,c.a007 ))+
       (IF(a.a008 IS NULL,0,a.a008 ) + IF(b.a008 IS NULL,0,b.a008 ) + IF(c.a008 IS NULL,0,c.a008 ))+
       (IF(a.a009 IS NULL,0,a.a009 ) + IF(b.a009 IS NULL,0,b.a009 ) + IF(c.a009 IS NULL,0,c.a009 ))+
       (IF(a.a010 IS NULL,0,a.a010 ) + IF(b.a010 IS NULL,0,b.a010 ) + IF(c.a010 IS NULL,0,c.a010 ))+
       (IF(a.a011 IS NULL,0,a.a011 ) + IF(b.a011 IS NULL,0,b.a011 ) + IF(c.a011 IS NULL,0,c.a011 ))+
       (IF(a.a012 IS NULL,0,a.a012 ) + IF(b.a012 IS NULL,0,b.a012 ) + IF(c.a012 IS NULL,0,c.a012 ))+
       (IF(a.a013 IS NULL,0,a.a013 ) + IF(b.a013 IS NULL,0,b.a013 ) + IF(c.a013 IS NULL,0,c.a013 ))+
       (IF(a.a014 IS NULL,0,a.a014 ) + IF(b.a014 IS NULL,0,b.a014 ) + IF(c.a014 IS NULL,0,c.a014 ))	as total_target
			,IF(a.a001 IS NULL,NULL,(IF(a.a001 IS NULL,0,a.a001 ) + IF(b.a001 IS NULL,0,b.a001 ) + IF(c.a001 IS NULL,0,c.a001 ))) as a001
      ,IF(a.a002 IS NULL,NULL,(IF(a.a002 IS NULL,0,a.a002 ) + IF(b.a002 IS NULL,0,b.a002 ) + IF(c.a002 IS NULL,0,c.a002 ))) as a002
      ,IF(a.a003 IS NULL,NULL,(IF(a.a003 IS NULL,0,a.a003 ) + IF(b.a003 IS NULL,0,b.a003 ) + IF(c.a003 IS NULL,0,c.a003 ))) as a003
      ,IF(a.a004 IS NULL,NULL,(IF(a.a004 IS NULL,0,a.a004 ) + IF(b.a004 IS NULL,0,b.a004 ) + IF(c.a004 IS NULL,0,c.a004 ))) as a004
      ,IF(a.a005 IS NULL,NULL,(IF(a.a005 IS NULL,0,a.a005 ) + IF(b.a005 IS NULL,0,b.a005 ) + IF(c.a005 IS NULL,0,c.a005 ))) as a005
      ,IF(a.a006 IS NULL,NULL,(IF(a.a006 IS NULL,0,a.a006 ) + IF(b.a006 IS NULL,0,b.a006 ) + IF(c.a006 IS NULL,0,c.a006 ))) as a006
      ,IF(a.a007 IS NULL,NULL,(IF(a.a007 IS NULL,0,a.a007 ) + IF(b.a007 IS NULL,0,b.a007 ) + IF(c.a007 IS NULL,0,c.a007 ))) as a007
      ,IF(a.a008 IS NULL,NULL,(IF(a.a008 IS NULL,0,a.a008 ) + IF(b.a008 IS NULL,0,b.a008 ) + IF(c.a008 IS NULL,0,c.a008 ))) as a008
      ,IF(a.a009 IS NULL,NULL,(IF(a.a009 IS NULL,0,a.a009 ) + IF(b.a009 IS NULL,0,b.a009 ) + IF(c.a009 IS NULL,0,c.a009 ))) as a009
      ,IF(a.a010 IS NULL,NULL,(IF(a.a010 IS NULL,0,a.a010 ) + IF(b.a010 IS NULL,0,b.a010 ) + IF(c.a010 IS NULL,0,c.a010 ))) as a010
      ,IF(a.a011 IS NULL,NULL,(IF(a.a011 IS NULL,0,a.a011 ) + IF(b.a011 IS NULL,0,b.a011 ) + IF(c.a011 IS NULL,0,c.a011 ))) as a011
      ,IF(a.a012 IS NULL,NULL,(IF(a.a012 IS NULL,0,a.a012 ) + IF(b.a012 IS NULL,0,b.a012 ) + IF(c.a012 IS NULL,0,c.a012 ))) as a012
      ,IF(a.a013 IS NULL,NULL,(IF(a.a013 IS NULL,0,a.a013 ) + IF(b.a013 IS NULL,0,b.a013 ) + IF(c.a013 IS NULL,0,c.a013 ))) as a013
      ,IF(a.a014 IS NULL,NULL,(IF(a.a014 IS NULL,0,a.a014 ) + IF(b.a014 IS NULL,0,b.a014 ) + IF(c.a014 IS NULL,0,c.a014 ))) as a014
  FROM model.temp_sku_sales_target_principal02 a 
LEFT JOIN model.temp_sku_sales_target_principal01 b 
    ON a.sku = b.sku
LEFT JOIN model.temp_sku_sales_target_principal03 c
    ON a.sku = c.sku
LEFT JOIN model.dim_sku_sales_target_principal d 
    ON a.sku = d.sku 
ORDER BY a.deng_ji) t 
where ROUND(t.zai_ku_ku_cun/5,0) -t.total_target >0
ORDER BY t.deng_ji ASC, (ROUND(t.zai_ku_ku_cun/5,0) -t.total_target) DESC
            """)
            df = pd.read_sql(stmt, session.connection())
            return df
    except Exception as e:
        print(e)
def stream_processing():
    """
       流式处理读取的销售目标负责人表的数据
       第一轮贪心算法分配，权重分配目标表：temp_sku_sales_target_principal02
       第二轮贪心算法分配，滞销冗余1-5级梯度递减法目标表：temp_sku_sales_target_principal03
       第三轮贪心算法分配，滞销冗余6级新品平均分配目标表：temp_sku_sales_target_principal01
    :return:
    """
    # 记录每个sku,每个人分配了多少的目标销量 [sku,{'a001':数量1, 'a002'：数量2, ...}]
    sku_everyone_target_sales = []
    # 销售目标字典和销售负责人代号字典
    parse_data(data)
    # 数据库源头数据
    df = sales_targets_principle()
    # 清空目标数据表 temp_sku_sales_target_principal02
    truncate_table('temp_sku_sales_target_principal02')

    for index, row in df.iterrows():
        principal_list = row['principal'].split(',')
        del principal_list[-1]
        # 源头数据字典
        stream_sour_dic = {'sku': row['sku'], 'dan_jia': row['dan_jia'], 'zai_ku_ku_cun': row['zai_ku_ku_cun'],
                           'sku_xiao_shou_mu_biao': row['sku_xiao_shou_mu_biao'], 'principal': principal_list}
        # print(stream_sour_dic,len(stream_sour_dic['principal']))

        # 写入的字典值
        sku_sale_target = row['sku_xiao_shou_mu_biao']
        write_data_dic = {'sku': row['sku'], 'deng_ji': row['deng_ji'], 'dan_jia': row['dan_jia'],
                          'zai_ku_ku_cun': row['zai_ku_ku_cun'], 'sku_xiao_shou_mu_biao': sku_sale_target}

        # 计算基础数值,根据二八法则,将8成的数据按照比重分配,剩下的2成数据按照顺序依次分配,这2成数据作为公平性的依据
        base_allocation = int(round(int(row['sku_xiao_shou_mu_biao'] // len(principal_list)) * 0.2, 0))

        # 所有负责人的剩余销售目标总和
        total_remaining_target = 0
        # 循环遍历每个销售负责人,分配基础值，个人销售目标剔除分配完的数值
        for emo in principal_list:
            # 将基础值分配给所有销售负责人
            write_data_dic[emo] = base_allocation
            # 减去基础值的销售额
            subtract_targets_dic_values(emo, base_allocation, row['dan_jia'])
            # 剩余目标值
            total_remaining_target += targets_dic[emo][0]
            # 所剩可分配销售数量
            sku_sale_target = sku_sale_target - base_allocation
            # print(f"sku_sale_target:{sku_sale_target}")
        # print(f"剩余目标值：{total_remaining_target}")

        # 循环遍历每个销售负责人,动态权重分配
        # 总递减剩余的SKU目标销售值
        sub_dy_total_target = 0
        for emo in principal_list:
            # 动态权重系数
            if total_remaining_target == 0:
                dynamic_weight = 0
            else:
                dynamic_weight = targets_dic[emo][0] / total_remaining_target
            print(f"动态权重系数：{emo}", dynamic_weight)
            # 动态权重分配
            write_data_dic[emo] += int(round(sku_sale_target * dynamic_weight, 0))
            # 减去动态权重值
            subtract_targets_dic_values(emo, int(round(sku_sale_target * dynamic_weight, 0)), row['dan_jia'])
            # 所剩可分配销售数量
            sub_dy_total_target += int(round(sku_sale_target * dynamic_weight, 0))
        sku_sale_target = sku_sale_target - sub_dy_total_target

        # 动态系数会导致部分剩余的总销售目标为1或者-1，或者更大
        if sku_sale_target > 0:
            pass
        elif sku_sale_target < 0:
            pass

        # 写入数据库
        print("基础值：", base_allocation, "剩余销售目标：", sku_sale_target, "所有负责人的剩余销售目标总和：",
              total_remaining_target, "a001", targets_dic['a001'][0])
        write_data_dic['last_sales_num'] = sku_sale_target

        print("写入temp_sku_sales_target_principal02：", write_data_dic)
        write_target_sales_to_msyql(write_data_dic, 'temp_sku_sales_target_principal02')

    # 未分配目标继续分配

    # 对所有人未分配销售目标的sku基数1的分配
    # base_distributing()
    # 对滞销冗余的12345级进行分配
    gradient_distribution12345()
    # 对的6级进行分配
    gradient_distribution6()

    # 未完成的分配
    gradient_distribution_last_target_force()


if __name__ == "__main__":
    stream_processing()
    print("数值字典:", targets_dic)
    print("\n姓名字典:", names_dic)
    # base_distributing()



