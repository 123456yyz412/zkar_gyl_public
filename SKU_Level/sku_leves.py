import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
from shukuajingAPI.config.gettablecnf import mysql_user,mysql_secret
from contextlib import contextmanager
import pendulum
from pendulum import timezone

# 当前时间
shanghai_tz = timezone('Asia/Shanghai')
now = pendulum.now(tz=shanghai_tz)
today_date = str(now.strftime('%Y-%m-%d'))
# 1年前的时间
last_year_date = str(now.subtract(years=1).strftime('%Y-%m-%d'))
print(f"当前时间：{today_date}")
print(f"1年前时间：{last_year_date}")

# 创建全局引擎（单例模式）- 兼容2.0语法
_engine = create_engine(
    f"mysql+mysqlconnector://{mysql_user}:{mysql_secret}@8.129.20.246:3306/model",
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

def fetch_data_from_mysql():
    try:
        with get_mysql_session() as session:
            # 修改SQL查询语句
            query = """
                SELECT
                    num AS `sales_normalized`,
                    revenue AS `revenue_normalized`,
                    profit AS `profit_normalized`
                FROM model.temp_sku_grade t
                WHERE t.profit > 0
            """
            df = pd.read_sql(query, session.connection())
            print(f"成功读取 {len(df)}条记录")
        return df
    except Exception as e:
        print(f"数据查询失败: {str(e)}")
        return None


# 主计算逻辑（保持原有计算流程）
def calculate_weights():
    # 读取数据
    df = fetch_data_from_mysql()
    if df is None:
        return

    p = df[['sales_normalized', 'revenue_normalized', 'profit_normalized']].copy()
    p = p.div(p.sum(axis=0), axis=1)
    p = p.replace(0, 1e-10)

    # 后续原有计算逻辑保持不变...
    n = p.shape[0]
    k = 1.0 / np.log(n)
    p_log = np.where(p > 0, p * np.log(p), 0)
    e = -k * p_log.sum(axis=0)
    d = 1 - e
    weights = d / d.sum()

    print("各指标权重：", list(weights))

    return list(weights)

# 修改数据库操作函数
def delete_from_sku_grade(condition_str):
    try:
        with get_mysql_session() as session:
            # 使用text()包装SQL（符合2.0规范）
            stmt = text("DELETE FROM dw.dw_sku_grade WHERE sku_level = :level")
            result = session.execute(stmt, {"level": condition_str})
            # 移除显式commit（由上下文管理器处理）
            print(f"delete受影响记录数{condition_str}级：{result.rowcount}")
    except Exception as e:
        print(f"delete_from_sku_grade error: {str(e)}")
        raise

def level6_insert_into_sku_grade():
    """
    6级新产品，
    :return:
    """
    delete_from_sku_grade(6)
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                INSERT INTO dw.dw_sku_grade (sku, sku_level, start_time, end_time)
                SELECT t.sku, 6 ,'{last_year_date}','{today_date}' FROM model.dim_new_sku t
            """)
            result = session.execute(stmt)
            print(f"6级受影响记录数：{result.rowcount}")
    except Exception as e:
        print(f"level6_insert_into_sku_grade: {str(e)}")
        raise

def level4_insert_into_sku_grade():
    """
    4级负利润
    :return:
    """
    delete_from_sku_grade(4)
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                INSERT INTO dw.dw_sku_grade (sku, sku_level,type,num,revenue,profit, start_time, end_time)
                SELECT t.sku, 4 ,type,num,revenue,profit,'{last_year_date}','{today_date}' FROM model.temp_sku_grade t
                WHERE t.profit < 0
            """)
            result = session.execute(stmt)
            print(f"4级受影响记录数：{result.rowcount}")
    except Exception as e:
        print(f"level4_insert_into_sku_grade: {str(e)}")
        raise


def insert_level123total():
    """
    3级新上架
    :return:
    """
    delete_from_sku_grade(1)
    delete_from_sku_grade(2)
    delete_from_sku_grade(3)
    delete_from_sku_grade(123)
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                INSERT INTO dw.dw_sku_grade (sku, sku_level,type,num,revenue,profit,num_mms,revenue_mms,profit_mms, start_time, end_time)
                SELECT t.sku, 123,type ,num,revenue,profit,num_mms,revenue_mms,profit_mms, '{last_year_date}','{today_date}' FROM model.temp_sku_grade t
                WHERE t.profit > 0
            """)
            result = session.execute(stmt)
            print(f"123级受影响记录数：{result.rowcount}")
    except Exception as e:
        print(f"insert_level123total: {str(e)}")
        raise

def update_level123scores():
    weight_list = calculate_weights()
    num_weight = weight_list[0]
    revenue_weight = weight_list[1]
    profit_weight = weight_list[2]
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                UPDATE dw.dw_sku_grade t
	               SET t.scores = {num_weight} * t.num_mms + {revenue_weight} * t.revenue_mms + {profit_weight} * t.profit_mms
                 WHERE t.sku_level = 123;
                """)
            result = session.execute(stmt)
            print(f"123级scores更新受影响记录数：{result.rowcount}")
    except Exception as e:
        print(f"update_level123scores: {str(e)}")
        raise

# Wheel Hub 品牌和非品牌123级划分
def update_wheel_hub_notbrand():
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                UPDATE dw.dw_sku_grade t
                   SET t.type = 'Wheel Hub非品牌'
                WHERE t.type = 'Wheel Hub'
                  AND t.sku LIKE 'RB%'
            """)
            result = session.execute(stmt)
            print(f"Wheel Hub非品牌受影响记录数：{result.rowcount}")
    except Exception as e:
        print(f"update_wheel_hub_notbrand: {str(e)}")
        raise

def update_wheel_hub_brand():
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                UPDATE dw.dw_sku_grade t
                   SET t.type = 'Wheel Hub品牌'
                WHERE t.type = 'Wheel Hub'
                  AND t.sku NOT LIKE 'RB%'
            """)
            result = session.execute(stmt)
            print(f"Wheel Hub品牌受影响记录数：{result.rowcount}")
    except Exception as e:
        print(f"update_wheel_hub_brand: {str(e)}")
        raise
# Suspension Parts 品牌和非品牌123级划分
def update_Suspension_Parts_notbrand():
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                UPDATE dw.dw_sku_grade t
                   SET t.type = 'Suspension Parts非品牌'
                WHERE t.type = 'Suspension Parts'
                  AND t.sku LIKE 'RB%'
            """)
            result = session.execute(stmt)
            print(f"Suspension Parts非品牌受影响记录数：{result.rowcount}")
    except Exception as e:
        print(f"update_wheel_hub_notbrand: {str(e)}")
        raise

def update_Suspension_Parts_brand():
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                UPDATE dw.dw_sku_grade t
                   SET t.type = 'Suspension Parts品牌'
                WHERE t.type = 'Suspension Parts'
                  AND t.sku NOT LIKE 'RB%'
            """)
            result = session.execute(stmt)
            print(f"Suspension Parts品牌受影响记录数：{result.rowcount}")
    except Exception as e:
        print(f"update_wheel_hub_brand: {str(e)}")
        raise

def sku_level123_by_type():
    """
    通过对子查询中dw.dw_sku_grade的表条件限制控制更细的
    :return: None
    """
    try:
        # 更新产品一级分类的type的品牌非品牌
        update_wheel_hub_notbrand()
        update_wheel_hub_brand()
        update_Suspension_Parts_notbrand()
        update_Suspension_Parts_brand()
        # 1-Wheel Hub非品牌123级
        level1_insert_into_sku_grade("and t.type = 'Wheel Hub非品牌'")
        level2_insert_into_sku_grade("and t.type = 'Wheel Hub非品牌'")
        level3_insert_into_sku_grade("and t.type = 'Wheel Hub非品牌'")
        # 2-Wheel Hub品牌123级
        level1_insert_into_sku_grade("and t.type = 'Wheel Hub品牌'")
        level2_insert_into_sku_grade("and t.type = 'Wheel Hub品牌'")
        level3_insert_into_sku_grade("and t.type = 'Wheel Hub品牌'")
        # 3-Suspension Parts非品牌123级
        level1_insert_into_sku_grade("and t.type = 'Suspension Parts非品牌'")
        level2_insert_into_sku_grade("and t.type = 'Suspension Parts非品牌'")
        level3_insert_into_sku_grade("and t.type = 'Suspension Parts非品牌'")
        # 4-Suspension Parts品牌123级
        level1_insert_into_sku_grade("and t.type = 'Suspension Parts品牌'")
        level2_insert_into_sku_grade("and t.type = 'Suspension Parts品牌'")
        level3_insert_into_sku_grade("and t.type = 'Suspension Parts品牌'")
        # 更新利润非负的sku的1、2、3级
        for type_40 in types_of_more_than_40_sku():
            print(f"type_40: {type_40}")
            level1_insert_into_sku_grade(f"and t.type = '{type_40}'")
            level2_insert_into_sku_grade(f"and t.type = '{type_40}'")
            level3_insert_into_sku_grade(f"and t.type = '{type_40}'")
        #对剩下的sku统一划分1、2、3级
        level1_insert_into_sku_grade("")
        level2_insert_into_sku_grade("")
        level3_insert_into_sku_grade("")

    except Exception as e:
        print(f"sku_level123_by_type: {str(e)}")
        return None

def types_of_more_than_40_sku():
    """
    找出除了wheel hub和suspension part其他分类中，非负利润的sku数量大于等于40的分类
    :return:
    """
    try:
        with get_mysql_session() as session:
            # 执行查询语句
            stmt = text(f"""
                SELECT t.type, count(1) as type_count 
                FROM dw.dw_sku_grade t 
                WHERE t.type <> 'Wheel Hub品牌' 
                  AND t.type <> 'Wheel Hub非品牌' 
                  AND t.type <> 'Suspension Parts品牌' 
                  AND t.type <> 'Suspension Parts非品牌' 
                  AND t.sku_level = 123 
                GROUP BY t.type 
                HAVING count(1) >= 40
            """)
            result = session.execute(stmt)
            # 将结果存储在字典中
            result_dict = {row[0]: row[1] for row in result}
            return_list = []
            for key in result_dict.keys():
                return_list.append(key)
            return return_list
    except Exception as e:
        print(f"update_Suspension_Parts_brand: {str(e)}")
        return None
def level1_insert_into_sku_grade(condition_sql_str=''):
    """
    根据限制条件更新1级数据
    :param condition_sql_str: 更新123临时等级的sku中sku某个分类的数据-> 1级：and type = 'xxx' \n更新所有的123临时等级的sku -> 1级：''
    :return: None
    """
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                UPDATE dw.dw_sku_grade t
  JOIN (
SELECT t2.sku,
       t2.rn,
			 t2.total_prifot,
			 t2.accum
  FROM(
SELECT t1.sku,
       t1.num,
			 t1.revenue,
			 t1.profit,
			 t1.scores,
			 t1.rn,
			 t1.total_prifot,
			 SUM(t1.profit) over(ORDER BY t1.rn) as accum
  FROM(
      SELECT t.sku,
             t.num,
			 t.revenue,
			 t.profit,
             t.scores,
			 ROW_NUMBER() over(ORDER BY t.scores desc) as rn,
			 SUM(t.profit) over() as total_prifot
        from dw.dw_sku_grade t 
       where t.sku_level = 123
             {condition_sql_str}
     ) t1 ) t2
WHERE t2.accum <= 0.8 * t2.total_prifot) a
   ON t.sku = a.sku
  SET t.sku_level = 1
                     """)
            result = session.execute(stmt)
            print(f"1级受影响记录数：{result.rowcount}")
    except Exception as e:
        print(f"level1_insert_into_sku_grade: {str(e)}")
        raise

def level2_insert_into_sku_grade(condition_sql_str=''):
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                UPDATE dw.dw_sku_grade t
  JOIN (
      SELECT t2.sku,
             t2.rn,
			 t2.total_prifot,
			 t2.accum
  FROM(
      SELECT t1.sku,
             t1.num,
			 t1.revenue,
			 t1.profit,
			 t1.scores,
			 t1.rn,
			 t1.total_prifot,
			 SUM(t1.profit) over(ORDER BY t1.rn) as accum
  FROM(
      SELECT t.sku,
             t.num,
			 t.revenue,
			 t.profit,
             t.scores,
			 ROW_NUMBER() over(ORDER BY t.scores desc) as rn,
			 SUM(t.profit) over() as total_prifot
        from dw.dw_sku_grade t 
    where t.sku_level = 123
         {condition_sql_str}
  ) t1 ) t2
WHERE t2.accum <= 0.8 * t2.total_prifot) a
   ON t.sku = a.sku
  SET t.sku_level = 2
            """)
            result = session.execute(stmt)
            print(f"2级受影响记录数：{result.rowcount}")
    except Exception as e:
        print(f"level2_insert_into_sku_grade: {str(e)}")
        raise

def level3_insert_into_sku_grade(condition_sql_str):
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                UPDATE dw.dw_sku_grade t
                   SET t.sku_level = 3
                 WHERE t.sku_level = 123
                      {condition_sql_str}
            """)
            result = session.execute(stmt)
            print(f"3级受影响记录数：{result.rowcount}")
    except Exception as e:
        print(f"level3_insert_into_sku_grade: {str(e)}")
        raise

def level5_insert_into_sku_grade():
    delete_from_sku_grade(5)
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                UPDATE dw.dw_sku_grade b
                  JOIN
	                 (
                       SELECT a.sku
                         FROM (
                                SELECT t.sku
			                           ,t.num
                                       ,avg(t.num) over() as avg_num
                                  FROM dw.dw_sku_grade t 
                                 WHERE t.profit < 0) a
                        WHERE a.num < avg_num) c
	                       ON b.sku = c.sku
                          SET b.sku_level = 5;
                    """)
            result = session.execute(stmt)
            print(f"5级受影响记录数：{result.rowcount}")
    except Exception as e:
        print(f"level5_insert_into_sku_grade: {str(e)}")
        raise

def cellect_by_sku_between_date():
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                call p_temp_sku_grade('{last_year_date}','{today_date}')
            """)
            result = session.execute(stmt)
            print(f"执行存储过程call p_temp_sku_grade({last_year_date},{today_date})")
    except Exception as e:
        print(f"cellect_by_sku_between_date: {str(e)}")
        raise

def truncate_dw_sku_grade():
    try:
        with get_mysql_session() as session:
            stmt = text(f"""
                truncate table dw.dw_sku_grade
            """)
            result = session.execute(stmt)
            print(f"清除等级表truncate table dw.dw_sku_grade")
    except Exception as e:
        print(f"truncate_dw_sku_grade: {str(e)}")
        raise



def sku_leves():
    # 调用存储过程，给mdodel的源表temp_grade_sku跑出数据
    cellect_by_sku_between_date()
    # 清空等级表dw.dw_sku_grade
    truncate_dw_sku_grade()
    # 插入6级新品数据
    level6_insert_into_sku_grade()
    # 插入4级负利润数据
    level4_insert_into_sku_grade()

    # 插入123级数据
    insert_level123total()
    # 更新123级评分数据
    update_level123scores()

    # 更新123级等级数据
    sku_level123_by_type()

    # 从4级负利润中划分淘汰sku为5级
    level5_insert_into_sku_grade()

if __name__ == '__main__':
    sku_leves()