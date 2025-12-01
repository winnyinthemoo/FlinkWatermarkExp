package com.hazel.watermark;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class OutOfOrderSource implements SourceFunction<SensorReading> {
    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random rand = new Random();
        String[] sensorIds = {"sensor_1", "sensor_2", "sensor_3"};

        // 基准时间：当前系统时间
        long baseTime = System.currentTimeMillis();

        while (running) {
            // 1. 随机选择一个传感器
            String id = sensorIds[rand.nextInt(sensorIds.length)];

            // 2. 模拟事件时间：当前时间 减去 一个随机的延迟（0~10秒），制造乱序
            // 有些数据很新（延迟0），有些数据是很久之前的（延迟大）
            long randomDelay = rand.nextInt(10000);
            long eventTime = System.currentTimeMillis() - randomDelay;

            // 为了实验观察明显，我们把温度设为固定值或随机值
            double temp = 20.0 + rand.nextGaussian();

            // 发送数据
            ctx.collect(new SensorReading(id, eventTime, temp));

            // 3. 控制发送频率 (比如每200毫秒发一条)，这就是你所谓的“负载”压力
            // 暂停时间越短，数据量越大
            TimeUnit.MILLISECONDS.sleep(500);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
