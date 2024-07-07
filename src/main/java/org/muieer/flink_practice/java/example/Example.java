package org.muieer.flink_practice.java.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setBufferTimeout(10000);
        env.socketTextStream("localhost", 9999).print();

        env.execute();
    }
}
