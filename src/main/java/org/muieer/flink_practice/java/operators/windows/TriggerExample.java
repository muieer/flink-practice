package org.muieer.flink_practice.java.operators.windows;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

public class TriggerExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        source.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .trigger(CountTrigger.of(3)) // 在不满足条件的情况下，不会触发
                .reduce(((value1, value2) -> value1 + value2))
                .print();
        env.execute();
    }
}
