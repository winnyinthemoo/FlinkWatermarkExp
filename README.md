### 实验负载

**给定出租车行程数据集，对每小时内各个出租车车行获得的小费总额进行统计**

**1.查阅flink提供的example，认为其提供的使用窗口算子的几个示例中，窗口计数问题最适用于探索水位线设置问题**

[flink/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/windowing at master · apache/flink](https://github.com/apache/flink/tree/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/windowing)

**2.查阅flink提供的flink-training，认为其提供的HourlyTips问题适用于探索水位线设置问题**

[flink-training/hourly-tips/src/solution/java/org/apache/flink/training/solutions/hourlytips/HourlyTipsSolution.java at master · apache/flink-training](https://github.com/apache/flink-training/blob/master/hourly-tips/src/solution/java/org/apache/flink/training/solutions/hourlytips/HourlyTipsSolution.java)

**两个问题本质均为对固定窗口的数据进行统计计数，前者没有对应的数据集，因此考虑使用后者的问题，并查找合适数据集进行实验**

**为研究对于滑动窗口与滚动窗口两种不同负载下水位线设置的影响，设计在同一任务下使用两种窗口分别进行测试（滚动窗口为：每小时统计一次前一小时的小费总额；滑动窗口为：每半小时统计一次前一小时的小费总额）**

****

### 实验数据集

找到两个合适的实验负载：

**1.芝加哥出租车数据集**

优势：每单行程提供司机ID，小费，总金额，起止时间

劣势：起止时间粒度较大（以15min为粒度，即：12:00, 12:15, 12:30，不存在12:17这种数据)，且数据完全顺序，需人为制造乱序

**2.NYC出租车数据集**

优势：每单行程提供小费，总金额，起止时间，车行ID；数据天然有一定的乱序

劣势：未提供每单行程的司机ID（出于隐私保护），因此无法直接进行flink-training中的HourlyTips任务（每小时各出租车司机的小费总和）

**经过衡量，认为可以通过使用车行ID代替司机ID来进行HourlyTips任务，而对芝加哥出租车数据集进行细粒度化与较好的乱序处理的难度较大，因此选用后者**





**数据集处理**

下载链接：
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet

**运行指令**
将MaxTipsJob.java中的两个文件路径替换为实际的文件路径。（一个是清洗后数据集的路径，一个是结果输出文件的路径）
生成jar包后在flink.1.9.3目录下运行以下指令（MaxTipsJob或MaxTipsSlidesJob）。
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