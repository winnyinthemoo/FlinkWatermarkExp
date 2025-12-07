### 任务1
使用清洗+乱序处理的芝加哥出租车数据集，进行出租车司机每小时小费求和任务（参考flink提供的taxifare任务）。

**数据集处理**

下载链接：
https://data.cityofchicago.org/Transportation/Taxi-Trips-2024-/ajtu-isnz/explore/query/SELECT%0A%20%20%60trip_id%60%2C%0A%20%20%60taxi_id%60%2C%0A%20%20%60trip_start_timestamp%60%2C%0A%20%20%60trip_end_timestamp%60%2C%0A%20%20%60fare%60%2C%0A%20%20%60tips%60%0AWHERE%20%60tips%60%20%3E%200/page/column_manager

下载后使用python_scripts目录下的data_clean.py进行清洗（要修改其中的文件路径）


**运行指令**
将MaxTipsJob.java中的两个文件路径替换为实际的文件路径。（一个是清洗后数据集的路径，一个是结果输出文件的路径）
生成jar包后在flink.1.9.3目录下运行以下指令。
注意，要将jar包目录换成你的实际jar包目录。最后的参数为水位线参数。
```
./bin/flink run -c com.hazel.watermark.MaxTipsJob D:\FlinkWatermarkExp\target\FlinkWatermarkExp-1.0-SNAPSHOT.jar 60
```

**查看实验结果**
实验完成后可在flink窗口中查看丢失数据总数，例如：
```angular2html
Total Late Data> 5541
Total Late Data> 5542
Total Late Data> 5543
Total Late Data> 5544
Total Late Data> 5545
Total Late Data> 5546
Total Late Data> 5547
Total Late Data> 5548
Total Late Data> 5549
Total Late Data> 5550
```

最后，运行python_scripts下的result_compare.py（需修改其中的文件路径，一个是清洗后数据集的路径，一个是结果输出文件的路径）
它会输出以下格式内容：
```angular2html
Ground truth: 86330 (window, taxi) records computed.
Flink results: 81741 (window, taxi) records read.
Average delay: 60.0 min
======================================
Total unique (window, taxi) records: 86330
Correct: 80752
Incorrect: 5578
  - Missing in Flink: 4589
  - Extra in Flink: 0
Accuracy: 93.54%
```