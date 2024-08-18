package org.muieer.flink_practice.java.time;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class CustomWatermarkGenerator
        implements WatermarkGenerator<Tuple4<String, String, String, Long>> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CustomWatermarkGenerator.class);
    private long maxTimestamp = 0L;

    @Override
    public void onEvent(
            Tuple4<String, String, String, Long> event,
            long eventTimestamp,
            WatermarkOutput output) {

        long outOfOrdernessMillis = 5000;
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);

        Instant instant = Instant.ofEpochMilli(maxTimestamp - outOfOrdernessMillis);
        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(instant, Clock.systemDefaultZone().getZone());
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
        String formatDate = localDateTime.format(timeFormatter);
        LOGGER.info("\n@muieer, event {}, emit watermark time {}", event, formatDate);

        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        long outOfOrdernessMillis = 5000;

        Instant instant = Instant.ofEpochMilli(maxTimestamp - outOfOrdernessMillis);
        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(instant, Clock.systemDefaultZone().getZone());
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
        String formatDate = localDateTime.format(timeFormatter);
        LOGGER.info("@muieer, watermark emit time {}", formatDate);

        //        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis));
    }
}
