package org.muieer.flink_practice.java.operators;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
        DataStream<String> stream = environment.socketTextStream("localhost", 9999).setParallelism(1);
        stream.map(new SyncReadFunction()).setParallelism(1).print().setParallelism(1);
        environment.execute();
    }

    public static void taskAsyncExecute() throws Exception {
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = environment.socketTextStream("localhost", 9999).setParallelism(1);
        DataStream<String> asyncStream = AsyncDataStream.unorderedWait(stream, new AsyncReadFunction(), 10, TimeUnit.SECONDS).setParallelism(1);
        asyncStream.print().setParallelism(1);
        environment.execute();
    }

}
