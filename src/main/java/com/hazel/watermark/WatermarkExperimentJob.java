package com.hazel.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class WatermarkExperimentJob {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 为了方便观察实验结果，建议并行度设为 1
        env.setParallelism(1);

        // 2. 设置时间语义为 EventTime (必须步骤)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 3. 添加数据源
        DataStream<SensorReading> stream = env.addSource(new OutOfOrderSource());

        // 4. 分配水位线 (Watermark)
        // 这里的 Time.seconds(3) 是你的核心实验变量：最大允许延迟时间
        SingleOutputStreamOperator<SensorReading> dataStream = stream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.timestamp;
                    }
                }
        );

        // 5. 先进行 Map 转换：把 SensorReading 转换成 Tuple2<id, 1>
        // 这样后面就可以直接 sum(1) 来计数了
        SingleOutputStreamOperator<Tuple2<String, Integer>> sensorTupleStream = dataStream
                .map(new MapFunction<SensorReading, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(SensorReading value) throws Exception {
                        return new Tuple2<>(value.id, 1);
                    }
                });

        // 6. 定义侧输出流标签 (注意：因为上面转换成了Tuple，这里迟到数据也是Tuple类型)
        OutputTag<Tuple2<String, Integer>> lateTag = new OutputTag<Tuple2<String, Integer>>("late"){};

        // 7. 分组 -> 开窗 -> 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = sensorTupleStream
                .keyBy(0) // 按照 Tuple 的第0个字段(id)分组
                .timeWindow(Time.seconds(10)) // 10秒滚动窗口
                .allowedLateness(Time.seconds(0)) // 允许窗口触发后继续等待迟到数据x秒,设为0,更纯粹地观察水位线的影响
                .sideOutputLateData(lateTag) // 收集彻底迟到的数据
                .sum(1); // 对 Tuple 的第1个字段(count)求和

        // 8. 打印正常窗口计算结果
        result.print("Main Flow");

        // 9. 打印被丢弃的迟到数据
        // 注意：这里的迟到数据将显示为 (sensor_1, 1) 这种形式，代表这个ID有一个数据迟到了
        result.getSideOutput(lateTag).print("Late Data");

        // 10. 执行任务
        env.execute("Flink Watermark Experiment");
    }
}
