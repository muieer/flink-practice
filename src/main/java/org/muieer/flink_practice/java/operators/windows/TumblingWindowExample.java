package org.muieer.flink_practice.java.operators.windows;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalTime;

/*
* https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/windows/
* */
public class TumblingWindowExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        runByReduceFunction(env);
    }

    public static void runByReduceFunction(StreamExecutionEnvironment env) throws Exception {

        var stream = env.socketTextStream("localhost", 9999);
        stream
            .map(str -> Tuple2.of(str, 1)).returns(new TypeHint<Tuple2<String, Integer>>() {})
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            .reduce((tupleA, tupleB) -> new Tuple2<>(tupleA.f0, tupleA.f1 + tupleB.f1))
            .map(tuple2 -> String.format("current second:%d, 10s count: %d", LocalTime.now().getSecond(), tuple2.f1))
            .print();

        env.execute();
    }

}
