import pandas as pd
import numpy as np

# -----------------------------
# 配置
# -----------------------------
CSV_FILE = "C:\\Users\\alex\\Downloads\\Taxi_Trips_(2024-)_20251206_head100000.csv"
OUTPUT_FILE = "C:\\Users\\alex\\Downloads\\Taxi_Trips_(2024-)_20251206_shuffled.csv"

TIME_COL = 'Trip Start Timestamp'
TAXI_COL = 'Taxi ID'
TIP_COL = "Tips"
DELTA_MINUTES = 120  # 最大 ±120 分钟乱序

# -----------------------------
# 1. 读取 CSV 并预处理
# -----------------------------
df = pd.read_csv(CSV_FILE)
# 只取前 100000 条
df = df.head(100000)
# 去掉 $ 符号并转 float
df[TIP_COL] = df[TIP_COL].replace({r'\$': ''}, regex=True).astype(float)

# 解析 AM/PM，生成 naive datetime（禁用 UTC）
df['event_time'] = pd.to_datetime(df[TIME_COL], format="%m/%d/%Y %I:%M:%S %p", utc=False)
df['event_time'] = df['event_time'].dt.tz_localize(None)

# -----------------------------
# 3. 独立乱序处理
# -----------------------------
np.random.seed(42)  # 固定随机种子可复现
random_offsets = np.random.uniform(-DELTA_MINUTES*60, DELTA_MINUTES*60, size=len(df))  # 秒为单位
df['event_time'] = df['event_time'] + pd.to_timedelta(random_offsets, unit='s')
# 按 event_time 升序排列
df_sorted = df.sort_values('event_time').reset_index(drop=True)

# -----------------------------
# 4. 保存 CSV
# -----------------------------
df_sorted.to_csv(OUTPUT_FILE, index=False)

print(f"独立乱序 CSV 已保存到: {OUTPUT_FILE}")
