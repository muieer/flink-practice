package org.muieer.flink_practice.java.operators.async;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class AsyncIODemo2 {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        run(env, parameterTool);
        env.execute();
    }

    static void run(StreamExecutionEnvironment env, ParameterTool parameterTool) {

        DataStream<Record> recordStream =
                env.socketTextStream("localhost", 9999)
                        .setParallelism(1)
                        .map(Record::new)
                        .setParallelism(1);

        /*
        * 当一条数据的重试次数等于最大重试值时，将不再重试，会发送这条数据给下一个子任务
        * */
        AsyncRetryStrategies.FixedDelayRetryStrategy<Record> retryStrategy =
                new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<Record>(3, 3000)
                        .ifResult(new EmptyPredicate())
                        .build();

        /*
        * 当一个分区或子任务的容量耗尽之后，阻塞新输入的数据
        * */
        AsyncDataStream.unorderedWaitWithRetry(
                        recordStream, new AsyncOperator(), 7, TimeUnit.SECONDS, 2, retryStrategy)
                .setParallelism(1)
                .filter(record -> record.retryCount == 0)
                .setParallelism(1)
                .print()
                .setParallelism(1);
    }

    static final class AsyncOperator extends RichAsyncFunction<Record, Record> {

        @Override
        public void asyncInvoke(Record input, ResultFuture<Record> resultFuture) throws Exception {
            CompletableFuture.runAsync(
                    () -> {
                        if (input.retryCount > 0) {
                            System.out.println("retry, " + input);
                        }
                        resultFuture.complete(List.of(input));
                    });
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        // 一条数据超时后，如果不调用 resultFuture.complete(...) 方法，这条数据将会保存在该子任务的异步队列中，不再发送到下游，
        // 消耗异步队列容量，对任务执行有负面影响
        @Override
        public void timeout(Record input, ResultFuture<Record> resultFuture) throws Exception {
            System.out.println("timeout, "+ input);
            resultFuture.complete(List.of(input));
        }
    }

    static final class EmptyPredicate
            implements Predicate<Collection<Record>>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean test(Collection<Record> collection) {
            Record[] array = collection.toArray(new Record[0]);
            Record record = array[0];
            boolean retry = record.content.length() % 2 == 1;
            record.retryCount += 1;
            return retry;
        }
    }

    @Getter
    @Setter
    @ToString
    static final class Record {
        private String content;
        private int retryCount = 0;

        public Record(String content) {
            this.content = content;
        }
    }
}
