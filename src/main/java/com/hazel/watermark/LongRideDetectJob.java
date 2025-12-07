package com.hazel.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Collector;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.HashMap;
import java.util.Map;


public class LongRideDetectJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // ====== 1. 加载 Parquet 数据 ======
        DataStream<TaxiRide> rides = //"D:\\FlinkWatermarkExp\\dataset\\reordered_2025-10.parquet"
                env.addSource(new TaxiRideParquetSource("D:\\FlinkWatermarkExp\\dataset\\yellow_tripdata_2025-10.parquet"));


        // ====== 2. 分配水位线（模仿你的示例：最大乱序 1 小时）======
        SingleOutputStreamOperator<TaxiRide> withWatermark =
                rides.assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<TaxiRide>(Time.minutes(60)) {
                            @Override
                            public long extractTimestamp(TaxiRide element) {
                                return element.getEventTimeMillis();
                            }
                        }
                );

        // ====== 3. 定义迟到事件标签 ======
        OutputTag<TaxiRide> lateTag = new OutputTag<TaxiRide>("late") {
        };


        // ====== 4. 滑动窗口：窗口长度 1 小时，每 0.5小时 滑动一次 ======
        SingleOutputStreamOperator<Long> alerts =
                withWatermark
                        .windowAll(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(30)))
                        .allowedLateness(Time.minutes(60))   // 完全不等迟到
                        .sideOutputLateData(lateTag)        // 捕获迟到事件
                        .process(new LongRideSlidingWindowFn());
        // 使用你之前写的 SingleFileSink 输出到文件
        SingleFileSink sink = new SingleFileSink("C:\\Users\\alex\\Desktop\\long_rides\\window_result.txt");
        alerts.addSink(sink);
//        // ====== 5. 输出正常窗口结果 ======
//        alerts.print("WINDOW_RESULT");
//
//        // ====== 6. 输出迟到事件 ======
//        alerts.getSideOutput(lateTag).print("LATE_EVENT");

        env.execute("Long Ride Sliding Window Detect");
    }


    // ======================================================
    // 自定义窗口函数（适配事件：start 和 end）
    // ======================================================
    public static class LongRideSlidingWindowFn
            extends ProcessAllWindowFunction<TaxiRide, Long, TimeWindow> {

        @Override
        public void process(
                Context ctx,
                Iterable<TaxiRide> events,
                Collector<Long> out) {

            // 用 Map 按 rideId 分组
            Map<Long, TaxiRide> startMap = new HashMap<>();
            Map<Long, TaxiRide> endMap   = new HashMap<>();

            for (TaxiRide r : events) {
                if (r.isStart) startMap.put(r.rideId, r);
                else endMap.put(r.rideId, r);
                out.collect(r.rideId);
            }

//            long windowStart = ctx.window().getStart();
//            long mid = windowStart + (30 * 60 * 1000L); // 窗口中点
//
//            for (Long rideId : startMap.keySet()) {
//                TaxiRide start = startMap.get(rideId);
//                TaxiRide end   = endMap.get(rideId);
//
//
//                if (start.eventTime < mid) {
//                    if (end == null || (end.eventTime - start.eventTime) > 1800 * 1000L) {
//                        out.collect(rideId);
//                    }
//                }
//            }
        }
    }
}
