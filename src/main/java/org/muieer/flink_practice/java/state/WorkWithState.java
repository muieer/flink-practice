package org.muieer.flink_practice.java.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WorkWithState {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L),
                Tuple2.of(1L, 8L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L)
            )
            .keyBy(value -> value.f0)
            .flatMap(new CountWindowAverage())
            .print();

        env.execute();
    }
}
