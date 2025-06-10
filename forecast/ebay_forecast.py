"""只针对库存数据修改销售数据，不加库存特征工程；同时加上时间特征工程"""
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine

# 数据连接配置
engine = create_engine('mysql+pymysql://u123:u123@localhost/test')

def main_pipeline():
    # 1. 加载并预处理数据
    raw_df = load_and_preprocess(engine)

    # 2. 仅处理Ebay平台数据
    Ebay_skus = raw_df.index.get_level_values('sku').unique()
    all_predictions = []

    # 加载数据并处理
    for sku in Ebay_skus:
        sku_data = raw_df.xs(sku, level='sku')

        # 应用有效窗口截取
        valid_data = get_valid_window(sku_data)
        if valid_data.empty:
            continue

        # 库存修正处理
        adjusted_data = adjust_sales_with_inventory(valid_data)

        monthly_sales = adjusted_data.resample('ME').sum()

        # 检查数据长度是否为1,模型选择并记录类型
        if len(monthly_sales) < 2:    #大概率为新品，仅在最近一个月有销量
            model_type = -1  # 简单平均法标记
            raw_pred = simple_average_forecast(adjusted_data)  #简单平均法预测
        else:

            if should_use_ets(adjusted_data):
                model_type = 0  # ETS标记
                raw_pred = ets_forecast(adjusted_data)  #原始ETS预测
            else:
                model_type = 1  # LSTM标记
                raw_pred = lstm_forecast(adjusted_data)   #原始LSTM预测

        # 统一后处理（新增关键步骤）
        processed_pred = post_processing(raw_pred, adjusted_data)

        # 构建结果DataFrame（新增model_type列）
        all_predictions.append(pd.DataFrame({
            'platform': 'Ebay',
            'sku': sku,
            'date': processed_pred.index,
            'predicted_sales': processed_pred.values,
            'model_type': model_type
        }))

    return pd.concat(all_predictions)


def post_processing(pred_series, adjusted_series):
    """增强型后处理"""
    # 拷贝数据避免污染原始预测
    processed = pred_series.copy()

    # 负数归零
    processed[processed < 0] = 0

    # 对于更长预测周期，添加指数衰减因子
    decay_factor = np.linspace(1.0, 0.95, len(processed))
    processed = processed * decay_factor

    # 动态小数处理（根据历史销售特征）
    monthly_sales_avg = adjusted_series.resample('ME').mean().mean()   # 获取历史均值
    if monthly_sales_avg <= 5:  # 低销量商品向上取整
        processed = np.ceil(processed)
    else:              # 常规商品四舍五入
        processed = np.round(processed)

    # 确保整数类型
    return processed.astype(int)


def load_and_preprocess(engine):
    # 查询语句（包含库存数据）
    """1、读取销售数据，列名分别为 platform, sku, sales_date, sales_qty, 分别代表平台、sku名称、销售时间，销量（查询前对数据进行'x'转'X'变化、去重or聚合操作）。
       2、读取库存数据，列名分别为 sku, date, total_available_quantity, 分别代表sku名称，库存日期，库存量（查询前对数据进行'x'转'X'变化、去重or聚合操作）,如果是ebay平台就用大仓库存数据，amazon平台就用全部库存数据"""
    query = """
        SELECT
            s.sku,
            s.sales_date,
            SUM(s.sales_qty) AS total_sales_qty,
            MAX(i.total_available_quantity) AS total_available_quantity
        FROM m_sales_quantity s
        LEFT JOIN inventory_warehouse i
            ON CONVERT(s.sku USING utf8mb4) COLLATE utf8mb4_0900_ai_ci = i.sku
            AND s.sales_date = i.date
        WHERE s.platform = 'Ebay'
            AND s.sales_date BETWEEN '2020-01-01' AND '2025-05-31'
        GROUP BY s.sku, s.sales_date;
    """
    raw_df = pd.read_sql(query, engine)


    # 创建完整日期索引并填充
    date_range = pd.date_range(start="2020-01-01", end="2025-05-31")
    sku_list = raw_df['sku'].unique()

    full_index = pd.MultiIndex.from_product(
        [sku_list, date_range],
        names=['sku', 'sales_date']
    )

    # 创建临时标志列，标识库存数据是否缺失
    raw_df['inventory_missing'] = raw_df['total_available_quantity'].isna()

    # 重新索引并填充
    filled_df = raw_df.set_index(['sku', 'sales_date']) \
                     .reindex(full_index) \
                     .sort_index()

    # 对销量和库存分别处理
    # 销量：缺失值填充为0
    filled_df['total_sales_qty'] = filled_df['total_sales_qty'].fillna(0)

    # 库存：缺失值填充为NaN（保留缺失状态）
    # 但添加一个标记列，记录库存是否缺失
    filled_df['inventory_missing'] = filled_df['inventory_missing'].fillna(True)

    return filled_df

def get_valid_window(series):
    """获取有效数据窗口：从第一个销售日到固定结束日"""
    non_zero = series['total_sales_qty'].gt(0)
    if non_zero.sum() == 0:
        return pd.Series([]), pd.Series([])  # 返回空数据

    first_valid = series[non_zero].index.min()
    # 固定结束日期为2025-05-31
    end_date = pd.Timestamp('2025-05-31')
    return series.loc[first_valid:end_date]

def adjust_sales_with_inventory(sku_data, window=14, drop_threshold=0.5): #原数据，窗口大小，销量波动率阈值
    """库存修正销量逻辑-考虑库存缺失的情况"""
    series = sku_data['total_sales_qty'].copy()
    inventory = sku_data['total_available_quantity']
    inventory_missing = sku_data['inventory_missing']

    for i in range(window, len(series)):
        # 如果库存数据缺失，跳过修正
        if inventory_missing.iloc[i]:
            continue

        current_sales = series.iloc[i]
        prev_mean = series.iloc[i-window:i].mean()

        # 跳过前窗口期无销售的情况
        if prev_mean == 0:
            continue

        drop_ratio = (prev_mean - current_sales) / prev_mean
        current_inv = inventory.iloc[i]

        # 计算前窗口期的平均库存（排除库存为0的异常值）
        prev_inv = inventory.iloc[i-window:i]
        valid_prev_inv = prev_inv[prev_inv > 0]

        # 如果有效库存数据不足，跳过修正
        if len(valid_prev_inv) < window/2:  # 至少需要一半的有效库存数据
            continue

        mean_inv = valid_prev_inv.mean()

        # 库存修正条件：
        # 1. 销量骤降超过阈值
        # 2. 当前库存显著低于历史平均库存（设为历史平均的30%）
        if (drop_ratio > drop_threshold and
            current_inv < mean_inv * 0.3 ):  # 双重检查确保库存确实低

            # 取前window天非零销量的均值
            valid_prev_sales = series.iloc[i-window:i][series.iloc[i-window:i] > 0]

            if not valid_prev_sales.empty:
                # 使用加权平均值：近期数据权重更高
                weights = np.linspace(0.5, 1.0, len(valid_prev_sales))
                weighted_mean = np.average(valid_prev_sales, weights=weights)
                series.iloc[i] = weighted_mean

    return series

def should_use_ets(sku_series):   #传入纯时间序列数据
    """判断是否使用指数平滑法"""
    monthly_sales = sku_series.resample('ME').mean()

    # 对于短时间序列或低销量数据使用ETS
    if len(monthly_sales) < 18:  # 由LSTM历史窗口长度和预测时间步长决定（36+27=63，所以至少为63*7=441天，可取1.5年，即18个月）
        return True
    if monthly_sales.mean() <= 3:  #月均销量低于3使用ETS（也可结合预测评估效果判断采用5或者8是否更好）
        return True
    return False

def simple_average_forecast(series, forecast_periods=6):
    """使用简单平均法对未来进行预测"""
    # 确保数据不为空
    if series.empty:
        raise ValueError("输入数据为空")
    # 计算每月销量
    monthly_sales = series.resample('ME').sum()
    # 获取非零值
    non_zero_series = monthly_sales[monthly_sales > 0]

    if len(non_zero_series) == 1:
        # 只有一个非零值，用它来预测未来
        last_value = non_zero_series.iloc[0]
    elif len(series[series > 0]) == 0:
        # 所有值都为0，预测也全为0
        last_value = 0
    else:
        raise ValueError("历史数据中包含多个非0值，请使用其他预测方法")

    # 创建未来日期索引（按月）
    last_date = series.index[-1]
    future_dates = pd.date_range(start=last_date, periods=forecast_periods + 1, freq='ME')[1:]  # 下一个月开始

    # 生成预测值（每期都等于 last_value）
    forecast = pd.Series(last_value, index=future_dates)

    return forecast


def ets_forecast(series, forecast_periods=6):
    """改进的指数平滑预测函数"""
    # 按月聚合数据
    monthly_data = series.resample('ME').sum()
    n_points = len(monthly_data)

    # 处理数据不足的特殊情况
    if n_points == 0:
        return pd.Series([0] * forecast_periods)

    if n_points < 6:
        # 计算稳健统计量 (处理全零/异常值)
        non_zero_vals = monthly_data[monthly_data > 0]

        if len(non_zero_vals) == 0:
            # 全零数据直接返回零预测
            return pd.Series([0] * forecast_periods)

        # 动态边界计算 (基于数据分布)
        data_max = monthly_data.max()
        q75 = monthly_data.quantile(0.75)

        # 边界规则：
        # - 下限：非零均值的80% 与 历史最小值的90% 取较保守值
        # - 上限：历史最大值的1.3倍与Q75的1.6倍取较大值
        lower_bound = min(
            non_zero_vals.mean() * 0.8,
            monthly_data.min() * 0.9
        )
        upper_bound = max(
            data_max * 1.3,
            q75 * 1.6 if not np.isnan(q75) else data_max * 1.4
        )

        # 使用无季节性指数平滑 (带趋势)
        model = ExponentialSmoothing(monthly_data,
                                     trend='add',
                                     seasonal=None,
                                     damped_trend=True).fit()  # 对于较长的预测周期（如6个月），添加阻尼趋势项

        forecast = model.forecast(forecast_periods)

        # 应用动态边界限制
        forecast = forecast.clip(lower=lower_bound, upper=upper_bound)
        return forecast

    else:
        use_seasonal = n_points >= 24  # 至少需要24个月才能启用季节性
        model = ExponentialSmoothing(monthly_data,
                                     trend='add',
                                     seasonal='add' if use_seasonal else None,
                                     seasonal_periods=12 if use_seasonal else None,
                                     damped_trend=True).fit()  # 对于较长的预测周期（如6个月），添加阻尼趋势项

        return model.forecast(forecast_periods)

def lstm_forecast(series, n_steps=36, forecast_window=27):     #预测6个月，则历史序列长度可取36（可结合预测效果进行适当修改），预测窗口取27
    """添加时间特征工程的LSTM预测流程"""
    # 时间特征生成函数
    def create_time_features(df):
        df = df.to_frame()  # 转换为DataFrame
        df['dayofweek'] = df.index.dayofweek
        df['dayofmonth'] = df.index.day
        df['weekofyear'] = df.index.isocalendar().week.astype(int)
        df['month'] = df.index.month
        # 添加季度特征
        df['quarter'] = df.index.quarter
        # 添加年度位置特征
        df['year_progress'] = df.index.dayofyear / 365.0
        return df

    # 数据预处理（周数据）
    weekly_data = series.resample('W').sum()
    #weekly_data_new = winsorize_data(weekly_data)   #由于进行了库存修正销量，所以无需缩尾处理

    #processed_data = create_time_features(weekly_data)      # 添加时间特征

    # 添加时间特征前进行数据平滑
    smoothed_weekly = weekly_data.rolling(window=4, min_periods=1).mean()

    processed_data = create_time_features(smoothed_weekly)  # 使用平滑后的数据

    # 特征列定义
    feature_columns = ['total_sales_qty', 'dayofweek', 'dayofmonth', 'weekofyear', 'month', 'quarter', 'year_progress']

    # 分特征标准化
    scalers = {}
    scaled_features = []
    for col in feature_columns:
        scaler = MinMaxScaler()
        scaled = scaler.fit_transform(processed_data[col].values.reshape(-1, 1))
        scalers[col] = scaler
        scaled_features.append(scaled)

    scaled_data = np.hstack(scaled_features)  # 形状：[n_samples, n_features]

    # 修改后的数据集类
    class TimeFeatureDataset(Dataset):
        def __init__(self, data, seq_length=36, pred_length=27):
            self.data = data
            self.seq_length = seq_length
            self.pred_length = pred_length

            # 确保数据足够生成至少一个样本
            assert len(self.data) >= seq_length + pred_length, \
                "数据长度不足，无法生成任何训练样本！"

        def __len__(self):
            return max(0, len(self.data) - self.seq_length - self.pred_length + 1)

        def __getitem__(self, idx):
            end_idx = idx + self.seq_length
            pred_end = end_idx + self.pred_length

            return (
                self.data[idx:end_idx, :],               # 输入特征
                self.data[end_idx:pred_end, 0]           # 目标销量
            )


    # 增强的LSTM模型 - 增加层和正则化
    class EnhancedLSTM(nn.Module):
        def __init__(self, input_size=7, hidden_size=128, output_size=27):    #预测6个月，则特征数量为 7（原始销量 + 6 个时间特征），隐藏层可取128，输出长度为27（可结合预测效果适当调整）
            super().__init__()
            self.lstm1 = nn.LSTM(input_size, hidden_size, batch_first=True, num_layers=2)
            self.dropout1 = nn.Dropout(0.3)   #添加Dropout层防止过拟合！！！
            self.lstm2 = nn.LSTM(hidden_size, hidden_size//2, batch_first=True)
            self.dropout2 = nn.Dropout(0.2)
            self.fc = nn.Linear(hidden_size//2, output_size)

        def forward(self, x):
            x, _ = self.lstm1(x)
            x = self.dropout1(x)
            x, _ = self.lstm2(x)
            x = self.dropout2(x[:, -1, :])   # 只取最后一个时间步的输出作为预测结果
            return self.fc(x)

    # 创建数据集
    dataset = TimeFeatureDataset(scaled_data, n_steps, forecast_window)

    # 数据划分
    train_size = int(0.8 * len(dataset))
    train_dataset, val_dataset = torch.utils.data.random_split(
        dataset, [train_size, len(dataset)-train_size])

    # 训练配置
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = EnhancedLSTM().to(device)
    optimizer = optim.Adam(model.parameters(), lr=0.001, weight_decay=1e-5)  # 添加权重衰减
    criterion = nn.MSELoss()

    # 添加学习率调度器
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, 'min', patience=5, factor=0.5)

    # 训练循环调整 - 添加早停机制
    best_val_loss = float('inf')
    patience = 10
    patience_counter = 0

    model.train()
    train_loader = DataLoader(train_dataset, batch_size=16, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=16)

    for epoch in range(200):            #预测6个月，训练轮次从100增加为200较为合适
        model.train()
        total_loss = 0
        for inputs, targets in train_loader:
            inputs = inputs.float().to(device)  # [batch, seq_len, features]
            targets = targets.float().to(device)  # [batch, pred_len]

            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()

        # 验证阶段
        model.eval()
        val_loss = 0
        with torch.no_grad():
            for inputs, targets in val_loader:
                inputs = inputs.float().to(device)
                targets = targets.float().to(device)
                outputs = model(inputs)
                val_loss += criterion(outputs, targets).item()

        val_loss /= len(val_loader)
        scheduler.step(val_loss)

        # 早停机制
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            torch.save(model.state_dict(), 'best_model.pth')
        else:
            patience_counter += 1
            if patience_counter >= patience:
                print(f"Early stopping at epoch {epoch+1}")
                break

    # 加载最佳模型
    model.load_state_dict(torch.load('best_model.pth'))

    # 预测阶段调整
    model.eval()
    with torch.no_grad():
        # 获取最后n_steps数据（包含所有特征）
        last_sequence = scaled_data[-n_steps:]

        # 生成预测
        pred_sequence = last_sequence.copy()
        predictions = []

        for step in range(forecast_window):
            input_tensor = torch.FloatTensor(pred_sequence[-n_steps:]) \
                          .unsqueeze(0).to(device)

            # 预测下一个点
            pred = model(input_tensor).cpu().numpy()[0][0]  # 取第一个预测值

            # 生成新时间特征
            last_date = weekly_data.index[-1] + pd.DateOffset(weeks=step+1)
            new_features = [
                last_date.dayofweek,
                last_date.day,
                last_date.isocalendar().week,
                last_date.month,
                last_date.quarter,
                last_date.dayofyear/365.0
            ]

            # 标准化新特征
            scaled_features = [pred]  # 销量部分使用预测值
            for i, col in enumerate(feature_columns[1:]):  # 处理时间特征
                scaler = scalers[col]
                scaled = scaler.transform([[new_features[i]]])[0][0]
                scaled_features.append(scaled)

            # 更新序列
            new_entry = np.array(scaled_features).reshape(1, -1)
            pred_sequence = np.vstack([pred_sequence, new_entry])
            predictions.append(pred)

    # 逆标准化最终预测
    final_pred = scalers['total_sales_qty'].inverse_transform(
        np.array(predictions).reshape(-1, 1)).flatten()

    # 生成预测日期
    pred_dates = pd.date_range(
        start=weekly_data.index[-1] + pd.DateOffset(weeks=1),
        periods=forecast_window,
        freq='W')

    return pd.Series(final_pred, index=pred_dates)


def transform_to_wide_format(predictions_df):
    """将预测结果转换为宽表格式，并保留 model_type"""

    # 1. 提取月份信息
    predictions = predictions_df.copy()
    predictions['month'] = predictions['date'].dt.to_period('M').dt.strftime('%Y-%m')

    # 2. 按 SKU 和月份分组聚合（处理可能存在的多日预测）
    monthly_pred = predictions.groupby(['sku', 'month'])['predicted_sales'].sum().reset_index()

    # 3. 保留 model_type（通过 merge）
    sku_model_type = predictions[['sku', 'model_type']].drop_duplicates(subset=['sku'])

    # 4. 合并 model_type 回去
    monthly_pred = monthly_pred.merge(sku_model_type, on='sku', how='left')

    # 5. 转换为宽表
    wide_df = monthly_pred.pivot(
        index='sku',
        columns='month',
        values='predicted_sales'
    ).reset_index()

    # 6. 把 model_type 加回来作为列
    wide_df = wide_df.merge(
        sku_model_type,
        on='sku',
        how='left'
    )

    # 7. 列名格式化
    wide_df.columns.name = None
    wide_df = wide_df.rename_axis(None, axis=1)

    # 8. 按时间顺序排列列
    month_columns = sorted(
        [col for col in wide_df.columns if col not in ['sku', 'model_type']],
        key=lambda x: pd.to_datetime(x)
    )

    return wide_df[['sku', 'model_type'] + month_columns]

# 执行预测
if __name__ == "__main__":
    predictions = main_pipeline()

    #转为宽表形式，方便查阅
    wide_predictions = transform_to_wide_format(predictions)
    wide_predictions = wide_predictions.iloc[:, :-1]    #剔除最后一列预测值周期不完整的数据

    # 添加 predicted 后缀
    time_columns = [col for col in wide_predictions.columns if col not in ['sku', 'model_type']]
    wide_predictions.rename(columns={col: f"{col}_predicted" for col in time_columns}, inplace=True)

    #保存预测结果
    wide_predictions.to_excel('ebay_forecast_6_months.xlsx', index=False)
