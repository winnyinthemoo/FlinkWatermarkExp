import pandas as pd

# -----------------------------
# 配置
# -----------------------------
CSV_FILE = "C:\\Users\\alex\\Downloads\\Taxi_Trips_(2024-)_20251206_head100000.csv"
FLINK_OUTPUT_FILE = "C:\\Users\\alex\\Desktop\\long_rides\\window_result.txt"

TIME_COL = 'Trip Start Timestamp'
TAXI_COL = 'Taxi ID'
TIP_COL = "Tips"
WINDOW_HOURS = 1

# -----------------------------
# 1. 读取 CSV 并预处理（无时区）
# -----------------------------
df = pd.read_csv(CSV_FILE)

# 去掉 $ 符号并转 float
df[TIP_COL] = df[TIP_COL].replace({r'\$': ''}, regex=True).astype(float)

# 解析 AM/PM，生成 naive datetime（禁用 UTC）
df[TIME_COL] = pd.to_datetime(df[TIME_COL], format="%m/%d/%Y %I:%M:%S %p", utc=False)
df[TIME_COL] = df[TIME_COL].dt.tz_localize(None)

# 按小时窗口分组
df["hour_window"] = df[TIME_COL].dt.floor(f"{WINDOW_HOURS}h")

# 聚合：每个 (hour_window, Taxi ID) 的小费总和
ground_truth_df = df.groupby(["hour_window", TAXI_COL])[TIP_COL].sum().reset_index()
# 转为字典：key = (hour_window, taxi_id), value = sum_tip
ground_truth = {
    (row["hour_window"], row[TAXI_COL]): row[TIP_COL]
    for _, row in ground_truth_df.iterrows()
}

print(f"Ground truth: {len(ground_truth)} (window, taxi) records computed.")

# -----------------------------
# 2. 读取 Flink 输出（也转成 naive datetime）
# -----------------------------
flink_results = {}
tot_delay = 0
tot_cnt = 0
with open(FLINK_OUTPUT_FILE, "r") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        if line.startswith("(") and line.endswith(")"):
            parts = line[1:-1].split(",", 3)
            window_start_ms, taxi_id, sum_tip ,delay= parts
            window_start = pd.to_datetime(int(window_start_ms.strip()), unit='ms').tz_localize(None)
            taxi_id = taxi_id.strip()
            sum_tip = float(sum_tip.strip())
            delay = int(delay.strip())
            tot_delay = delay
            tot_cnt = 1
            flink_results[(window_start, taxi_id)] = sum_tip

print(f"Flink results: {len(flink_results)} (window, taxi) records read.")
print(f"Average delay: {tot_delay/(1000*tot_cnt*60) if tot_cnt > 0 else 0} min")
# -----------------------------
# 3. 对比所有 (window, taxi_id) -> sum_tip 记录
# -----------------------------
all_keys = set(ground_truth.keys()) | set(flink_results.keys())

correct_count = 0
incorrect_count = 0
missing_in_flink = 0
extra_in_flink = 0

for key in all_keys:
    true_val = ground_truth.get(key, None)
    flink_val = flink_results.get(key, None)

    if true_val is not None and flink_val is not None:
        # 双方都有
        if abs(true_val - flink_val) < 1e-3:
            correct_count += 1
        else:
            incorrect_count += 1
    elif true_val is not None:
        # Flink 缺失
        missing_in_flink += 1
        incorrect_count += 1
    else:
        # Flink 多出（ground truth 没有）
        extra_in_flink += 1
        incorrect_count += 1

total_records = len(all_keys)
accuracy = correct_count / total_records * 100 if total_records > 0 else 0

print("======================================")
print(f"Total unique (window, taxi) records: {total_records}")
print(f"Correct: {correct_count}")
print(f"Incorrect: {incorrect_count}")
print(f"  - Missing in Flink: {missing_in_flink}")
print(f"  - Extra in Flink: {extra_in_flink}")
print(f"Accuracy: {accuracy:.2f}%")