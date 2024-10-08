package org.muieer.flink_practice.java.batch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.List;

public class BatchProcessExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(args[0]))
                        .build();

        // 统计单词出现的频次
        CloseableIterator<Tuple2<String, Integer>> closeableIterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "")
                        .flatMap(
                                (line, collector) -> {
                                    for (String word : line.split(" ")) {
                                        collector.collect(word);
                                    }
                                })
                        .returns(new TypeHint<>() {})
                        .map(
                                str -> {
                                    return Tuple2.of((String) str, 1);
                                })
                        .returns(new TypeHint<Tuple2<String, Integer>>() {})
                        .keyBy(tuple2 -> tuple2.f0)
                        .reduce(
                                (tuple2A, tuple2B) ->
                                        new Tuple2<>(tuple2A.f0, tuple2A.f1 + tuple2B.f1))
                        .executeAndCollect();

        try {
            List<String> list = new ArrayList<>();
            while (closeableIterator.hasNext()) {
                Tuple2<String, Integer> next = closeableIterator.next();
                list.add(String.format("%s_%s", next.f0, next.f1));
            }
            System.out.println(list);
        } finally {
            closeableIterator.close();
        }
    }
}
