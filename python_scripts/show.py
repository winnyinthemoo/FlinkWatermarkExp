import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# -----------------------------
# 1. 加载你的数据（示例）
# -----------------------------
# 假设你的数据是 CSV，包含 'timestamp' 列（格式如 "12:50"）
# df = pd.read_csv('your_data.csv')
# 为演示，我们生成模拟数据：
TIME_COL = 'tpep_pickup_datetime'
TAXI_COL = 'VendorID'
TIP_COL = "tip_amount"
WINDOW_HOURS = 1

# -----------------------------
# 1. Load Ground Truth
# -----------------------------
df = pd.read_parquet("C:\\Users\\alex\\Downloads\\yellow_tripdata_2024-02_first300k.parquet")
# 只保留 2024-12-01 到 2024-12-10 的数据
start_date = pd.Timestamp('2024-1-01')
end_date = pd.Timestamp('2024-4-10')

df = df[(df[TIME_COL] >= start_date) & (df[TIME_COL] <= end_date)].copy()
# 转换时间格式
df[TIME_COL] = pd.to_datetime(df[TIME_COL], format="%m/%d/%Y %I:%M:%S %p", utc=False)
df[TIME_COL] = df[TIME_COL].dt.tz_localize(None)

# 添加到达顺序
df['arrival_seq'] = df.index

# 计算理想顺序（按时间排序）
df['event_rank'] = df[TIME_COL].rank(method='first').astype(int) - 1
df['offset'] = df['arrival_seq'] - df['event_rank']
import matplotlib.pyplot as plt

fig, axs = plt.subplots(3, figsize=(14, 15))

# (1) 散点图：到达顺序 vs 实际时间
axs[0].scatter(df['arrival_seq'], df[TIME_COL], s=1, alpha=0.5)
axs[0].set_title('Arrival Sequence vs Actual Time')
axs[0].set_ylabel('Actual Time')
axs[0].set_xlabel('Arrival Sequence')
axs[0].grid(True)

# (2) 直方图：偏移量分布
axs[1].hist(df['offset'], bins=2000, color='orange', alpha=0.7)
axs[1].set_title('Distribution of Order Offset\n(>0: late, <0: early)')
axs[1].set_xlabel('Offset (arrival_rank - event_rank)')
axs[1].set_xlim(-15000, 15000)  # 只看 ±5000 范围内的乱序
axs[1].grid(True)

# (3) 滚动乱序率
window = 1000
df['is_out_of_order'] = df['offset'] != 0
rolling_rate = df['is_out_of_order'].rolling(window=window).mean()
axs[2].plot(rolling_rate, color='purple', linewidth=0.8)
axs[2].set_title(f'Rolling Out-of-Order Rate (window={window})')
axs[2].set_xlabel('Arrival Sequence')
axs[2].set_ylabel('Fraction out-of-order')
axs[2].grid(True)

plt.tight_layout()
plt.show()