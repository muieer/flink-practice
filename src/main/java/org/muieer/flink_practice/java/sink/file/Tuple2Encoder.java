package org.muieer.flink_practice.java.sink.file;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class Tuple2Encoder implements Encoder<Tuple2<Long, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Tuple2Encoder.class);

    @Override
    public void encode(Tuple2<Long, String> element, OutputStream stream) throws IOException {
        var str = "input time is " + element.f0 + ", input is " + element.f1;
        LOGGER.info(str);
        stream.write(str.getBytes(StandardCharsets.UTF_8));
        stream.write('\n');
    }

}
