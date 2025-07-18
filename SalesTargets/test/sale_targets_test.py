from contextlib import contextmanager
from sqlalchemy import create_engine,text
from sqlalchemy.orm import scoped_session, sessionmaker
import pandas as pd
from collections import OrderedDict

data = '''
#施璐
a001 = [179000,196900]
#董倩
a002 = [154962,170458.2]
#黄美璇
a003 = [145000,159500]
#刘林萍
a004 = [72000,79200]
#钱正宇
a005 = [66000,72600]
#张竹青
a006 = [62000,68200]
#李信
a007 = [61000,67100]
#黄美婷
a008 = [50000,55000]
#蒙昌平
a009 = [50000,55000]
#彭霞
a010 = [50000,55000]
#邹慧
a011 = [50000,55000]
#陈佳燕
a012 = [50000,55000]
#黄雪明
a013 = [36000,39600]
#朱伟豪
a014 = [18000,19800]
'''
#数值字典: {'a001': [179000, 268500], 'a002': [154962, 232443], 'a003': [145000, 217500], 'a004': [72000, 108000], 'a005': [66000, 99000], 'a006': [62000, 93000], 'a007': [61000, 91500], 'a008': [50000, 75000], 'a009': [50000, 75000], 'a010': [50000, 75000], 'a011': [50000, 75000], 'a012': [50000, 75000], 'a013': [36000, 54000], 'a014': [18000, 27000]}
targets_dic = {}
#姓名字典: {'a001': '施璐', 'a002': '董倩', 'a003': '黄美璇', 'a004': '刘林萍', 'a005': '钱正宇', 'a006': '张竹青', 'a007': '李信', 'a008': '黄美婷', 'a009': '蒙昌平', 'a010': '彭霞', 'a011': '邹慧', 'a012': '陈佳燕', 'a013': '黄雪明', 'a014': '朱伟豪'}
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
            query_item_links ="""SELECT t.sku
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
            stmt = text(""" SELECT sku,deng_ji,dan_jia,zai_ku_ku_cun,sku_xiao_shou_mu_biao,last_sales_num,a001,a002,a003,a004,a005,a006,a007,a008,a009,a010,a011,a012,a013,a014
  FROM model.temp_sku_sales_target_principal02 t
 WHERE ((t.a001  = 0) OR (t.a002  = 0) OR (t.a003  = 0) OR (t.a004  = 0) OR (t.a005  = 0) OR (t.a006  = 0) OR (t.a007  = 0) OR (t.a008  = 0) OR (t.a009  = 0) OR (t.a010  = 0) OR (t.a011  = 0) OR (t.a012  = 0) OR (t.a013  = 0) OR (t.a014  = 0) )
   and t.zai_ku_ku_cun <> 0
	 ORDER BY t.deng_ji ASC,t.sku_xiao_shou_mu_biao desc;""")
            # df = pd.read_sql(stmt, session.connection(),params={':_emo_id':emo_id})
            df = pd.read_sql(stmt, session.connection())
        return df
    except Exception as e:
        print(f"Error: {e}")



def write_target_sales_to_msyql(data_dic: dict,insert_talble: str):
    """
    插入数据到数据库销售目标表里面
    :param data_dic:
    :return:
    """
    try:
        isnert_data_dic = {'sku': None, 'deng_ji': None, 'dan_jia': None, 'zai_ku_ku_cun': None, 'sku_xiao_shou_mu_biao': None, 'last_sales_num': None,'a001': None,
         'a002': None, 'a003': None, 'a004': None, 'a005': None, 'a006': None, 'a007': None, 'a008': None, 'a009': None, 'a010': None,
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
            session.execute(stmt, {'_sku' : isnert_data_dic['sku'],
'_deng_ji' : isnert_data_dic['deng_ji'],
'_dan_jia' : isnert_data_dic['dan_jia'],
'_zai_ku_ku_cun' : isnert_data_dic['zai_ku_ku_cun'],
'_sku_xiao_shou_mu_biao' : isnert_data_dic['sku_xiao_shou_mu_biao'],
'_last_sales_num' : isnert_data_dic['last_sales_num'],
'_a001' : isnert_data_dic['a001'],
'_a002' : isnert_data_dic['a002'],
'_a003' : isnert_data_dic['a003'],
'_a004' : isnert_data_dic['a004'],
'_a005' : isnert_data_dic['a005'],
'_a006' : isnert_data_dic['a006'],
'_a007' : isnert_data_dic['a007'],
'_a008' : isnert_data_dic['a008'],
'_a009' : isnert_data_dic['a009'],
'_a010' : isnert_data_dic['a010'],
'_a011' : isnert_data_dic['a011'],
'_a012' : isnert_data_dic['a012'],
'_a013' : isnert_data_dic['a013'],
'_a014' : isnert_data_dic['a014']})
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



def get_employ_target(index:int):
    """
    根据index获取目标id
    :param index:0是自己定的目标，既下线，索引1是目标上线，暂定分配到个人的1.5倍
    :return:
    """
    pass
def subtract_targets_dic_values(emoployee_id: str, number: int,dan_jia:float):
    """

    :param emoployee_id: 员工id，需要
    :param number:
    :return:
    """
    try:
        # 如果目标销量上线大于0，则减少目标销量
        if targets_dic[emoployee_id][0] > 0:
            targets_dic[emoployee_id] = [targets_dic[emoployee_id][0] - number * dan_jia, targets_dic[emoployee_id][1] - number * dan_jia]
            if targets_dic[emoployee_id][0] < 0:
                targets_dic[emoployee_id][0] = 0
    except Exception as e:
        print(f"Error: {e}")
def gradient_allocation(x, y):
    """
    梯度下降分配，如[2, 2, 2, 2, 1, 1]
    :param x: 分配对象数量
    :param y: 可分配sku数量
    :return:
    """
    result = [0] * x
    # 先尽量给每个对象分配 1 个物品
    for i in range(min(x, y)):
        result[i] = 1
    remaining = y - min(x, y)
    index = 0
    # 分配剩余物品
    while remaining > 0:
        result[index] += 1
        remaining -= 1
        index = (index + 1) % x
    # 从大到小排序
    result.sort(reverse=True)
    return result

def base_distributing():
    """对为0的数据目标基数分配"""
    # 查询未分配目标数据
    truncate_table('temp_sku_sales_target_principal01')
    have_zero_df = select_undistributed_sku()
    for index, row in have_zero_df.iterrows():
        writ_temp_table_dic = {'sku': row['sku'], 'deng_ji': row['deng_ji'], 'dan_jia': row['dan_jia'],
                               'zai_ku_ku_cun': row['zai_ku_ku_cun'],
                               'sku_xiao_shou_mu_biao': row['sku_xiao_shou_mu_biao'],
                               'last_sales_num': row['last_sales_num']}
        for i in range(1, 15):
            if row[f'a{i:03}'] == 0:
                # print(f"{index}-{row['sku']}-a{i:03}:{row[f'a{i:03}']}")
                # 分配基础值1，确保每个人负责的sku都会分配到
                writ_temp_table_dic[f'a{i:03}'] = 1
                targets_dic[f'a{i:03}'][0] -= 1*row['dan_jia']
                if targets_dic[f'a{i:03}'][0] < 0:
                    targets_dic[f'a{i:03}'][0] = 0
                targets_dic[f'a{i:03}'][1] -= 1*row['dan_jia']

        write_target_sales_to_msyql(writ_temp_table_dic,'temp_sku_sales_target_principal01')

def gradient_distribution():
    """
    梯度分配
    :return:
    """
    truncate_table('temp_sku_sales_target_principal03')
    available_df = available_stored()
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
                               'last_sales_num': row['last_sales_num']}
        principle_list = row['principal'].split(',')

        value_ordered_list =gradient_allocation(len(principle_list),row['for_needed'])

        # 调整顺序，因为principle_list是从a001到a014次序
        for key in not_achieved_targets_dic.keys():
            if key in principle_list:
                write_temp_table_dic[key] = value_ordered_list[0]

                targets_dic[key][0] -= value_ordered_list[0] * row['dan_jia']
                if targets_dic[key][0] <= 0:
                    targets_dic[key][0] = 0
                targets_dic[key][1] -= value_ordered_list[0] * row['dan_jia']
                if targets_dic[key][1] <= 0:
                    targets_dic[key][1] = 0
                    del write_temp_table_dic[key]

                value_ordered_list.pop(0)

        write_target_sales_to_msyql(write_temp_table_dic,'temp_sku_sales_target_principal03')


def available_stored():
    """
    计算可用库存
    :return:
    """
    try:
        with get_mysql_session() as session:
            stmt = text("""SELECT
	a.sku,
	a.deng_ji,
	a.dan_jia,
	a.zai_ku_ku_cun,
	a.sku_xiao_shou_mu_biao,
	a.last_sales_num,
	a.avaible_kucun,
	b.forecast_values,
	a.avaible_kucun - b.forecast_values AS for_needed,
    TRIM(TRAILING ',' FROM a.principal) as principal
FROM
	(
	SELECT
		t.sku,
		t.deng_ji,
		t.dan_jia,
		t.zai_ku_ku_cun,
		t.sku_xiao_shou_mu_biao,
		t.last_sales_num,
		(t.zai_ku_ku_cun - t.sku_xiao_shou_mu_biao + t.last_sales_num -
		IF(t.a001 = 0, 1,0) - IF(t.a002 = 0, 1,0) - IF(t.a003 = 0, 1,0) - IF(t.a004 = 0, 1,0) - IF(t.a005 = 0, 1,0) - IF(t.a006 = 0, 1,0) - IF(t.a007 = 0, 1,0) - IF(t.a008 = 0, 1,0) - IF(t.a009 = 0, 1,0) - IF(t.a010 = 0, 1,0) - IF(t.a011 = 0, 1,0) - IF(t.a012 = 0, 1,0) - IF(t.a013 = 0, 1,0) - IF(t.a014 = 0, 1,0)) AS avaible_kucun,
		CONCAT(IF(t.a001 is not null,'a001,',''),IF(t.a002 is not null,'a002,',''),IF(t.a003 is not null,'a003,',''),IF(t.a004 is not null,'a004,',''),IF(t.a005 is not null,'a005,',''),IF(t.a006 is not null,'a006,',''),IF(t.a007 is not null,'a007,',''),IF(t.a008 is not null,'a008,',''),IF(t.a009 is not null,'a009,',''),IF(t.a010 is not null,'a010,',''),IF(t.a011 is not null,'a011,',''),IF(t.a012 is not null,'a012,',''),IF(t.a013 is not null,'a013,',''),IF(t.a014 is not null,'a014,','')) as principal
	FROM
		model.temp_sku_sales_target_principal02 t 
WHERE ((t.a001  = 0) OR (t.a002  = 0) OR (t.a003  = 0) OR (t.a004  = 0) OR (t.a005  = 0) OR (t.a006  = 0) OR (t.a007  = 0) OR (t.a008  = 0) OR (t.a009  = 0) OR (t.a010  = 0) OR (t.a011  = 0) OR (t.a012  = 0) OR (t.a013  = 0) OR (t.a014  = 0) )
   and t.zai_ku_ku_cun <> 0
	) a
	LEFT JOIN model.dim_sku_sales_target_principal b 
	       ON a.sku = b.sku 
WHERE
	a.avaible_kucun > 0 
	AND a.avaible_kucun - b.forecast_values > 0 
	AND a.a.avaible_kucun - b.forecast_values > 0
ORDER BY
	a.deng_ji ASC,
	(a.avaible_kucun - b.forecast_values) DESC,
	a.avaible_kucun DESC;
            """)
            df = pd.read_sql(stmt, session.connection())
            return df
    except Exception as e:
        print(e)

def stream_processing():
    """
       流式处理读取的销售目标负责人表的数据
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
        #源头数据字典
        stream_sour_dic = {'sku':row['sku'],'dan_jia':row['dan_jia'],'zai_ku_ku_cun':row['zai_ku_ku_cun'],'sku_xiao_shou_mu_biao':row['sku_xiao_shou_mu_biao'],'principal' : principal_list}
        print(stream_sour_dic,len(stream_sour_dic['principal']))

        # 写入的字典值
        sku_sale_target = row['sku_xiao_shou_mu_biao']
        write_data_dic = {'sku':row['sku'],'deng_ji':row['deng_ji'],'dan_jia':row['dan_jia'],'zai_ku_ku_cun':row['zai_ku_ku_cun'],'sku_xiao_shou_mu_biao':sku_sale_target}


        # 计算基础数值,根据二八法则,将8成的数据按照比重分配,剩下的2成数据按照顺序依次分配,这2成数据作为公平性的依据
        base_allocation = int(round(int(row['sku_xiao_shou_mu_biao'] // len(principal_list)) * 0.2,0))

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
        print(f"剩余目标值：{total_remaining_target}")


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
            write_data_dic[emo] += int(round(sku_sale_target * dynamic_weight,0))
            # 减去动态权重值
            subtract_targets_dic_values(emo, int(round(sku_sale_target * dynamic_weight,0)), row['dan_jia'])
            # 所剩可分配销售数量
            sub_dy_total_target += int(round(sku_sale_target * dynamic_weight,0))
        sku_sale_target = sku_sale_target - sub_dy_total_target

        # 动态系数会导致部分剩余的总销售目标为1或者-1，或者更大
        if sku_sale_target > 0:
            pass
        elif sku_sale_target < 0:
            pass


        # 写入数据库
        print("基础值：", base_allocation, "剩余销售目标：", sku_sale_target, "所有负责人的剩余销售目标总和：", total_remaining_target,"a001", targets_dic['a001'][0])
        write_data_dic['last_sales_num'] = sku_sale_target
        print("写入temp_sku_sales_target_principal02：",write_data_dic)
        write_target_sales_to_msyql(write_data_dic, 'temp_sku_sales_target_principal02')

    # 未分配目标继续分配

    # 对所有人未分配销售目标的sku基数1的分配
    base_distributing()

    #分配可用库存，如果剩余的负责人的销售目标为0，则分配可用库存
    gradient_distribution()

    # print(not_achieved_targets_dic)
    # {'a005': [3376.0699999999415, 36376.069999999956], 'a006': [3122.859999999935, 34122.859999999884],
    # 'a007': [2966.2199999999602, 33466.21999999988], 'a009': [2424.5600000000054, 27424.560000000016],
    # 'a010': [2424.5600000000054, 27424.560000000016], 'a011': [2424.5600000000054, 27424.560000000016],
    # 'a014': [1171.7300000000018, 10171.730000000012], 'a001': [0, 89333.16000000015], 'a002': [0, 77392.99000000003],
    # 'a003': [0, 72417.65999999981], 'a004': [0, 35484.87999999989], 'a008': [0, 24833.95999999996],
    # 'a012': [0, 24327.739999999994], 'a013': [0, 17870.63000000002]}
    # 对targets_dic进行排序，按照值进行排序，然后按照顺序进行分配，如果值相等，按照顺序进行分配
    # not_achieved_targets_dic = dict(
    #     sorted(targets_dic.items(),
    #            key=lambda item: (-item[1][0], -item[1][1]))
    # )
    # temp_dict = {}
    # for key, value in not_achieved_targets_dic.items():
    #     if float(value[0]) > 0:
    #         temp_dict[key] = [value[0],value[1]]
    # not_achieved_targets_dic = temp_dict
    # print("not_achieved_targets_dic",not_achieved_targets_dic)





if __name__ == "__main__":
    stream_processing()
    print("数值字典:", targets_dic)
    print("\n姓名字典:", names_dic)
    # base_distributing()



