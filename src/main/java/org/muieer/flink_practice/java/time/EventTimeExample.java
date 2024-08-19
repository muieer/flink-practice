package org.muieer.flink_practice.java.time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

public class EventTimeExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventTimeExample.class);

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(Duration.ofSeconds(5).toMillis());

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> sourceStream =
                env.socketTextStream("localhost", 9999)
                        .map(
                                line -> {
                                    long eventTimeMillis =
                                            Instant.now().toEpochMilli()
                                                    - ThreadLocalRandom.current().nextInt(60_000);
                                    String eventTimeFormatDate = getFormatterDate(eventTimeMillis);

                                    long processTimeMillis = Instant.now().toEpochMilli();
                                    String processTimeFormatDate =
                                            getFormatterDate(processTimeMillis);

                                    Tuple4<String, String, String, Long> tuple4 =
                                            Tuple4.of(
                                                    line,
                                                    "处理时间: " + processTimeFormatDate,
                                                    "事件时间: " + eventTimeFormatDate,
                                                    eventTimeMillis);
                                    return tuple4;
                                })
                        .returns(new TypeHint<Tuple4<String, String, String, Long>>() {});

        WatermarkStrategy<Tuple4<String, String, String, Long>> customWatermarkStrategy =
                new CustomWatermarkStrategy()
                        .withIdleness(Duration.ofSeconds(10))
                        .withTimestampAssigner((tuple4, timestamp) -> tuple4.f3);

        //        WatermarkStrategy<Tuple4<String, String, String, Long>> watermarkStrategy =
        //                WatermarkStrategy.<Tuple4<String, String, String,
        // Long>>forBoundedOutOfOrderness(
        //                                Duration.ofSeconds(5))
        //                        .withIdleness(Duration.ofSeconds(10))
        //                        .withTimestampAssigner((tuple4, timestamp) -> tuple4.f3);

        final OutputTag<Tuple4<String, String, String, Long>> lateOutputTag = new OutputTag<>("late-data") {};

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> dataStream = sourceStream
                .assignTimestampsAndWatermarks(customWatermarkStrategy)
                .keyBy(tuple4 -> tuple4.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5L))
                .sideOutputLateData(lateOutputTag)
                .reduce(
                        (tupleA, tupleB) -> {
                            if (tupleA.f3 >= tupleB.f3) {
                                return tupleB;
                            } else {
                                return tupleA;
                            }
                        },
                        new MyProcessWindowFunction());

        dataStream.getSideOutput(lateOutputTag)
                        .map(tuple4 -> {
                            LOGGER.info("@muieer, late output, {}", tuple4);
                            return 1L;
                        });

        env.execute();
    }

    static String getFormatterDate(long millis) {
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
        Instant instant = Instant.ofEpochMilli(millis);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, Clock.systemDefaultZone().getZone());
        return localDateTime.format(timeFormatter);
    }

    private static final class MyProcessWindowFunction
            extends ProcessWindowFunction<
                    Tuple4<String, String, String, Long>,
                    Tuple4<String, String, String, Long>,
                    String,
                    TimeWindow> {

        private static final Logger LOGGER = LoggerFactory.getLogger(MyProcessWindowFunction.class);

        @Override
        public void process(
                String s,
                ProcessWindowFunction<
                                        Tuple4<String, String, String, Long>,
                                        Tuple4<String, String, String, Long>,
                                        String,
                                        TimeWindow>
                                .Context
                        context,
                Iterable<Tuple4<String, String, String, Long>> elements,
                Collector<Tuple4<String, String, String, Long>> out)
                throws Exception {

            Tuple4<String, String, String, Long> res = elements.iterator().next();
            LOGGER.info("@muieer, process, res {}", res);

            TimeWindow window = context.window();
            String startDate = getFormatterDate(window.getStart());
            String endDate = getFormatterDate(window.getEnd());
            LOGGER.info("@muieer, process, start window {}, end window {}", startDate, endDate);
            LOGGER.info(
                    "@muieer, process, watermark {}, current {}",
                    getFormatterDate(context.currentWatermark()),
                    getFormatterDate(context.currentProcessingTime()));

            out.collect(res);
        }
    }
}
