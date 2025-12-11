import pandas as pd

df = pd.read_parquet("C:\\Users\\alex\\Downloads\\yellow_tripdata_2024-02_first300k.parquet")

print(df)              # 打印全部数据
print(df.head())       # 查看前几条
for i in range(0,100):
    print(df.iloc[i])
