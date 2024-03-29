package org.muieer.flink_practice.java.operators.windows;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.muieer.flink_practice.java.function.DistinctAggregateFunction;
import org.muieer.flink_practice.java.function.UserBillProcessWindowFunction;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Random;

/*
* https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/windows/
* */
public class TumblingWindowExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        runByReduceFunction(env);
//        runByAggregateFunction(env);
        runByProcessWindowFunction(env);
    }

    public static void runByProcessWindowFunction(StreamExecutionEnvironment env) throws Exception {

        var stream = env.socketTextStream("localhost", 9999);
        var random = new Random();

        stream
            .map(str -> {
                // f0 是用户名，f1 使用消费金额
                Tuple2<String, Integer> tuple2 = Tuple2.of(str, random.nextInt(100) + 1);
                System.out.println(String.format("user %s spend %d yuan at %s", tuple2.f0, tuple2.f1, LocalDateTime.now()));
                return tuple2;
            }).returns(new TypeHint<Tuple2<String, Integer>>() {})
            .keyBy(tuple -> tuple.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .process(new UserBillProcessWindowFunction())
            .print();

        env.execute();
    }

    public static void runByAggregateFunction(StreamExecutionEnvironment env) throws Exception {

        var stream = env.socketTextStream("localhost", 9999);
        stream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new DistinctAggregateFunction())
                .map(count -> String.format("该窗口出现过 %d 种不同的字符串", count))
                .print();

        env.execute();
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
