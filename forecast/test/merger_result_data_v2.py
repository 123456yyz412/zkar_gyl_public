import pandas as pd
# 在以第一个月第二个月的销售 不进行延期的基础做的
#初始配置
type_min_num = {'Wheel Hub':12,'Brake Pads':30,'ABS Sensor':50}

def merge_data(type:str,forecast_date:list):
    """
    对数据值比较小的数据进行合并
    :param type: 产品一级分类
    :param forecast_date: 预测值
    :return:
    """
    # 设置最小下单数据
    min_num = 16
    for key,value in type_min_num.items():
        if type == key:
            min_num = value


    if forecast_date[-1]==0:
        return forecast_date

    if forecast_date[-1]<min_num:
        # 分配次数
        x : int = int(sum_list(forecast_date[2:])/min_num)
        if x>0:
            return distribute_value(min_num,x,get_non_zero_indices(forecast_date))

        else:
            return distribute_value(min_num,1,get_non_zero_indices(forecast_date))

    return forecast_date
def count_non_zero(lst):
    """
    统计列表中非零元素的数量
    :param lst: 输入列表
    :return: 非零元素个数
    """
    return sum(1 for x in lst if x != 0)

def average_non_zero(lst):
    """
    计算列表中非零元素的平均值（当所有元素都为0时返回0）

    参数:
        lst (list): 数值型元素列表

    返回:
        float: 非零元素的平均值，若无非零元素则返回0

    示例:
        >>> average_non_zero([0, 0, 0])
        0
        >>> average_non_zero([0, 1, 2, 0, 3])
        2.0
        >>> average_non_zero([1, 2, 3, 4])
        2.5
    """
    non_zero = [x for x in lst if x != 0]
    count = len(non_zero)
    return round(sum(non_zero)/count, 2) if count else 0

def sum_list(numbers):
    """
    计算数值型列表所有元素的总和

    参数:
        numbers (list): 由数字组成的列表（int/float）

    返回:
        int/float: 列表元素总和

    示例:
        >>> sum_list([0, 0, 0])
        0
        >>> sum_list([0, 1, 2, 0, 3])
        6
        >>> sum_list([1.5, 2.5, 3])
        7.0
    """
    return sum(numbers)

def get_non_zero_indices(lst):
    """
    获取数值型列表中非零元素的索引列表

    参数:
        lst (list): 由数字组成的列表（int/float）

    返回:
        list: 所有非零元素的索引位置列表

    示例:
        >>> get_non_zero_indices([0,0,0,1,5])
        [3, 4]
        >>> get_non_zero_indices([0, 0, 0])
        []
        >>> get_non_zero_indices([1, 0, 2, 0, 3])
        [0, 2, 4]
    """
    # return [i for i, x in enumerate(lst) if x != 0]
    result_list = []
    for index,x in enumerate(lst):
        if x != 0 and index not in [0,1]:
            result_list.append(index)
    return result_list


# def distribute_value(value: int, num: int, index_list: list) -> list:
#     """
#     将指定数量的数值随机分布到索引位置对应的列表中
#
#     参数:
#         value (int): 要填充的数值
#         num (int): 需要填充的数量
#         index_list (list): 可供选择的索引位置列表
#
#     返回:
#         list: 长度为6的列表，包含指定数量的value值和其他位置的0
#
#     示例:
#         >>> distribute_value(15, 2, [3,4,5])
#         [0, 0, 0, 15, 15, 0]  # 示例输出，实际结果随机
#     """
#     result = [0] * 6
#
#     # 当num为0时不填充任何值
#     if num <= 0:
#         return result
#
#     # 从可用索引中随机选择num个不重复索引
#     selected_indices = random.sample(index_list, num)
#
#     # 填充选定位置
#     for idx in selected_indices:
#         result[idx] = value
#
#     return result
def distribute_value(value: int, num: int, index_list: list) -> list:
    """
    将指定数量的数值均匀分布到索引位置对应的列表中

    参数:
        value (int): 要填充的数值
        num (int): 需要填充的数量
        index_list (list): 可供选择的索引位置列表

    返回:
        list: 长度为6的列表，包含指定数量的value值和其他位置的0

    示例:
        >>> distribute_value(15, 2, [3,4,5])
        [0, 0, 0, 15, 0, 15]  # 实际结果均匀分布
        >>> distribute_value(10, 3, [0,1,2,3,4,5])
        [10, 0, 10, 0, 10, 0]  # 均匀间隔分布
    """
    result = [0] * 6

    # 边界条件处理
    if num <= 0 or not index_list:
        return result

    sorted_indices = sorted(index_list)
    m = len(sorted_indices)

    # 当num大于等于可用索引数时，全部填充
    if num >= m:
        selected_indices = sorted_indices
    else:
        # 核心算法：线性间隔分布
        step = (m - 1) / (num - 1) if num > 1 else 0
        selected_indices = []
        for i in range(num):
            # 特殊处理单个元素场景
            if num == 1:
                pos = m // 2  # 取中间位置
            else:
                pos = round(i * step)
                pos = max(0, min(pos, m - 1))  # 限制索引范围

            selected_indices.append(sorted_indices[pos])

    # 填充结果
    for idx in selected_indices:
        if idx < 6:  # 确保索引有效
            result[idx] = value

    return result


def process_excel(input_file_path, output_file_path):
    # 读取Excel文件
    df = pd.read_excel(input_file_path)

    # 遍历每条记录
    for index, row in df.iterrows():
        # 获取原始预测值列表
        forecast_values = [
            int(row['f01']),
            int(row['f02']),
            int(row['f03']),
            int(row['f04']),
            int(row['f05']),
            int(row['f06'])
        ]

        # 调用 merge_data 处理数据
        result_list = merge_data(row['产品一级分类'], forecast_values)
        # print(row['产品型号'],row['产品一级分类'],result_list)

        # 确保结果有效且长度正确
        if result_list is None or len(result_list) != 6:
            print(f"警告：行 {index} 处理结果无效，跳过")
            continue

        # 将结果依次赋值给 f01 到 f06
        for i in range(6):
            # 检查是否为 None
            if result_list[i] is None:
                print(f"警告：行 {index} 的 f0{i + 1} 位置为 None，保留原值")
            else:
                df.at[index, f'f0{i + 1}'] = result_list[i]

    # 处理奇数
    for index, row in df.iterrows():
        # 获取原始预测值列表
        forecast_values = [
            int(row['f01']),
            int(row['f02']),
            int(row['f03']),
            int(row['f04']),
            int(row['f05']),
            int(row['f06'])
        ]

        for i in range(6):
            if forecast_values[i] % 2 != 0:
                df.at[index, f'f0{i + 1}'] = df.at[index, f'f0{i + 1}']-1

    df.to_excel(output_file_path, index=False)



if __name__ == '__main__':
    input_file_path = r'D:\Desktop\yyz\3-延期紧急订单\7-18\供应商展示ebay预测值.xlsx'
    output_file_path = r'D:\Desktop\yyz\3-延期紧急订单\7-18\供应商展示ebay预测值结果.xlsx'
    # input_file_path = r'C:\Users\YYZ\Desktop\work\销售预测以及等级划分\7-10\供应商展示ebay预测值.xlsx'
    # output_file_path = r'C:\Users\YYZ\Desktop\work\销售预测以及等级划分\7-10\供应商展示ebay预测值结果.xlsx'

    process_excel(input_file_path, output_file_path)