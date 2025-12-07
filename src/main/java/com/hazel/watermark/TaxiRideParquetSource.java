package com.hazel.watermark;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.avro.AvroParquetReader;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class TaxiRideParquetSource implements SourceFunction<TaxiRide> {

    private final String parquetPath;
    private static final int BATCH_SIZE = 5;
    private volatile boolean running = true;

    public TaxiRideParquetSource(String parquetPath) {
        this.parquetPath = parquetPath;
    }

    @Override
    public void run(SourceContext<TaxiRide> ctx) throws Exception {

        ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(new Path(parquetPath))
                .build();

        GenericRecord record;
        long rideId = 0;
        long maxStartTime = 0;
        List<TaxiRide> startEvents = new ArrayList<TaxiRide>(BATCH_SIZE);
        PriorityQueue<TaxiRide> endEventQ = new PriorityQueue<>(100);

        while (running && (record = reader.read())!=null) {

            // NYC Taxi schema: tpep_pickup_datetime / tpep_dropoff_datetime
            // 假设字段是 long 类型（微秒时间戳）
            long pickupMills = ((Number) record.get("tpep_pickup_datetime")).longValue()/1000;
            long dropoffMills = ((Number) record.get("tpep_dropoff_datetime")).longValue()/1000;
            long vendor_id=((Number) record.get("VendorID")).longValue();

            // 转换为毫秒（Instant.ofEpochMilli 需要毫秒）
            Instant pickupTime = Instant.ofEpochMilli(pickupMills);
            Instant dropoffTime = Instant.ofEpochMilli(dropoffMills);

            // 1) Emit START event
            TaxiRide startEvent = new TaxiRide(
                    rideId,
                    true,
                    pickupMills,
                    vendor_id
            );
            ctx.collect(startEvent);
            //startEvents.add(startEvent);
            maxStartTime = Math.max(maxStartTime, startEvent.getEventTimeMillis());

            // 2) Emit END event
            TaxiRide endEvent = new TaxiRide(
                    rideId,
                    false,
                    dropoffMills,
                    vendor_id
            );
            ctx.collect(endEvent);
            rideId++;
            if(rideId % 1000 == 0) {
                Thread.sleep(10);
            }
//            endEventQ.add(endEvent);
//            rideId++;
//
//            if(rideId % BATCH_SIZE == 0) {
//                while (endEventQ.peek().getEventTimeMillis() <= maxStartTime) {
//                    TaxiRide ride = endEventQ.poll();
//                    ctx.collect(ride);
//                }
//                startEvents.iterator().forEachRemaining(r -> ctx.collect(r));
//                startEvents.clear();
//                maxStartTime=0;
//            }
            // Optional speed control
            // Thread.sleep(5);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

