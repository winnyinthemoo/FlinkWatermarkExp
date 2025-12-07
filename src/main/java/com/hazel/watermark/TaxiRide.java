package com.hazel.watermark;


import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * A TaxiRide is a taxi ride event. There are two types of events, a taxi ride start event and a
 * taxi ride end event. The isStart flag specifies the type of the event.
 *
 * <p>A TaxiRide consists of - the rideId of the event which is identical for start and end record -
 * the type of the event (start or end) - the time of the event - the longitude of the start
 * location - the latitude of the start location - the longitude of the end location - the latitude
 * of the end location - the passengerCnt of the ride - the taxiId - the driverId
 */
public class TaxiRide implements Comparable<TaxiRide>, Serializable {

    public TaxiRide(
            long rideId,
            boolean isStart) {
        this.rideId = rideId;
        this.isStart = isStart;
        this.eventTime = System.currentTimeMillis();
    }
    /** Creates a TaxiRide with the given parameters. */
    public TaxiRide(
            long rideId,
            boolean isStart,
            long eventTime) {
        this.rideId = rideId;
        this.isStart = isStart;
        this.eventTime = eventTime;
    }

    public TaxiRide(
            long rideId,
            boolean isStart,
            long eventTime,
            long vendor_id) {
        this.rideId = rideId;
        this.isStart = isStart;
        this.eventTime = eventTime;
        this.vendor_id = vendor_id;
    }

    public long rideId;
    public boolean isStart;
    public long eventTime;
    public long tip;
    public long driver_id;
    public long vendor_id;

    @Override
    public String toString() {

        return rideId
                + ","
                + (isStart ? "START" : "END")
                + ","
                + eventTime;
    }

    /**
     * Compares this TaxiRide with the given one.
     *
     * <ul>
     *   <li>sort by timestamp,
     *   <li>putting START events before END events if they have the same timestamp
     * </ul>
     */
    @Override
    public int compareTo(@Nullable TaxiRide other) {
        if (other == null) {
            return 1;
        }

        // 用 long 比较时间
        int compareTimes = Long.compare(this.eventTime, other.eventTime);

        if (compareTimes == 0) {
            // 如果时间相同，再用 isStart 排序：START < END
            if (this.isStart == other.isStart) {
                return 0;
            } else {
                return this.isStart ? -1 : 1;
            }
        } else {
            return compareTimes;
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaxiRide taxiRide = (TaxiRide) o;
        return rideId == taxiRide.rideId
                && isStart == taxiRide.isStart
                && Objects.equals(eventTime, taxiRide.eventTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rideId,
                isStart,
                eventTime);
    }

    /** Gets the ride's time stamp as a long in millis since the epoch. */
    public long getEventTimeMillis() {
        return eventTime;
    }

    /** Creates a StreamRecord, using the ride and its timestamp. Used in tests. */
    @VisibleForTesting
    public StreamRecord<TaxiRide> asStreamRecord() {
        return new StreamRecord<>(this, this.getEventTimeMillis());
    }

    /** Creates a StreamRecord from this taxi ride, using its id and timestamp. Used in tests. */
    @VisibleForTesting
    public StreamRecord<Long> idAsStreamRecord() {
        return new StreamRecord<>(this.rideId, this.getEventTimeMillis());
    }
}

