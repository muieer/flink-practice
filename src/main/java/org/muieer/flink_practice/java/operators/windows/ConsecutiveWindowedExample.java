package org.muieer.flink_practice.java.operators.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

public class ConsecutiveWindowedExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = streamExecutionEnvironment.socketTextStream("localhost", 9999);
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = socketTextStream
                .rescale()
                .map(str -> Tuple2.of(str, 1)).returns(new TypeHint<Tuple2<String, Integer>>() {})
                .keyBy(tuple2 -> tuple2.f0);

        wordCountWindow(keyedStream);
        wordSortDescWindow(socketTextStream);

        streamExecutionEnvironment.execute();
    }

    static void wordSortDescWindow(DataStreamSource<String> socketTextStream) {
        socketTextStream.windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .aggregate(new WordCountAggregateFunction(), new WordSortDescProcessAllWindowFunction())
                .shuffle()
                .print();
    }

    static void wordCountWindow(KeyedStream<Tuple2<String, Integer>, String> keyedStream) {
        keyedStream
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .evictor(new BlankWordEvictor())
                .aggregate(new Tuple2WordCountAggregateFunction())
                .filter(map -> !map.isEmpty())
                .print();
    }

    static class BlankWordEvictor implements Evictor<Tuple2<String, Integer>, TimeWindow> {

        @Override
        public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
            for (Iterator<TimestampedValue<Tuple2<String, Integer>>> iterator = elements.iterator(); iterator.hasNext();) {
                if (iterator.next().getValue().f0.isBlank()) {
                    iterator.remove();
                }
            }
        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

        }
    }

    static class WordSortDescProcessAllWindowFunction extends ProcessAllWindowFunction<Map<String, Integer>, List<String>, TimeWindow> {

        @Override
        public void process(ProcessAllWindowFunction<Map<String, Integer>, List<String>, TimeWindow>.Context context, Iterable<Map<String, Integer>> elements, Collector<List<String>> out) throws Exception {
            Map<String, Integer> map = elements.iterator().next();
            List<String> list = map.entrySet().stream().sorted((entity1, entity2) -> entity2.getValue() - entity1.getValue())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            out.collect(list.stream().filter(str -> !str.isBlank()).collect(Collectors.toList()));
        }
    }

    static class WordCountAggregateFunction implements AggregateFunction<String, Map<String, Integer>, Map<String, Integer>> {

        @Override
        public Map<String, Integer> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<String, Integer> add(String str, Map<String, Integer> accumulator) {
            accumulator.put(str, accumulator.getOrDefault(str, 0) + 1);
            return accumulator;
        }

        @Override
        public Map<String, Integer> getResult(Map<String, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Map<String, Integer> merge(Map<String, Integer> a, Map<String, Integer> b) {
            a.putAll(b);
            return a;
        }
    }

    static class Tuple2WordCountAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, Map<String, Integer>, Map<String, Integer>> {

        @Override
        public Map<String, Integer> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<String, Integer> add(Tuple2<String, Integer> tuple2, Map<String, Integer> accumulator) {
            accumulator.put(tuple2.f0, accumulator.getOrDefault(tuple2.f0, 0) + 1);
            return accumulator;
        }

        @Override
        public Map<String, Integer> getResult(Map<String, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Map<String, Integer> merge(Map<String, Integer> a, Map<String, Integer> b) {
            a.putAll(b);
            return a;
        }
    }
}
