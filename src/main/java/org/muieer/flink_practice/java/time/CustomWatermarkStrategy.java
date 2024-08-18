package org.muieer.flink_practice.java.time;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomWatermarkStrategy
        implements WatermarkStrategy<Tuple4<String, String, String, Long>> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CustomWatermarkStrategy.class);

    @Override
    public WatermarkGenerator<Tuple4<String, String, String, Long>> createWatermarkGenerator(
            WatermarkGeneratorSupplier.Context context) {
        return new CustomWatermarkGenerator();
    }
}
