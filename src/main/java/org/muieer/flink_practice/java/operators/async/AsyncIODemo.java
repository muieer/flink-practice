package org.muieer.flink_practice.java.operators.async;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.muieer.flink_practice.java.function.AsyncReadFunction;
import org.muieer.flink_practice.java.function.SyncReadFunction;

import java.util.concurrent.TimeUnit;

/*
* 所有算子的并行度设置为 1，异步或同步执行时，只有一个线程在处理任务，等价于只有一个 taskManager 和一个 slot。
* 一个工作线程，同步执行，只有当上一个操作执行结束才能执行下一个。但是在异步执行的方式中，上一个没有执行结束就可以执行下一个。
* https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/asyncio/
* */
public class AsyncIODemo {

    public static void main(String[] args) throws Exception {

        taskAsyncExecute();
//        taskSyncExecute();
    }

    public static void taskSyncExecute() throws Exception {

        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableClosureCleaner(); // 闭包清理策略

        environment
                .socketTextStream("localhost", 9999).setParallelism(1)
                .map(new SyncReadFunction()).setParallelism(1)
                .print().setParallelism(1);

        environment.execute();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void taskAsyncExecute() throws Exception {

        var environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = environment.socketTextStream("localhost", 9999).setParallelism(1);
        AsyncRetryStrategy asyncRetryStrategy = new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder(3, 1000)
                .ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
                .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
                .build();

        // 容量是指同时异步处理的元素最大个数，如：当已经有 3 个元素处于异步处理的状态，则新来的元素会处于阻塞状态
        AsyncDataStream
            .unorderedWaitWithRetry(stream, new AsyncReadFunction(), 10, TimeUnit.SECONDS, 3, /*NO_RETRY_STRATEGY*/asyncRetryStrategy)
            .setParallelism(1).print().setParallelism(1);

        environment.execute();
    }

}
