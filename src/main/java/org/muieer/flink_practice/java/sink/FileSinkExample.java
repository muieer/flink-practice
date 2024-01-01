package org.muieer.flink_practice.java.sink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class FileSinkExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSinkExample.class);

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        var stream = env.socketTextStream("localhost", 9999).map(str -> Tuple2.of(LocalDateTime.now(), str));
    }
}
