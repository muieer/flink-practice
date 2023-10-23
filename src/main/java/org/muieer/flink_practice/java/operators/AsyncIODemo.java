package org.muieer.flink_practice.java.operators;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.muieer.flink_practice.java.function.AsyncReadFunction;
import org.muieer.flink_practice.java.function.SyncReadFunction;

import java.util.concurrent.TimeUnit;

public class AsyncIODemo {

    public static void main(String[] args) throws Exception {
        taskAsyncExecute();
//        taskSyncExecute();
    }

    public static void taskSyncExecute() throws Exception {
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableClosureCleaner();
        DataStream<String> stream = environment.socketTextStream("localhost", 9999);
        stream.map(new SyncReadFunction()).print();
        environment.execute();
    }

    public static void taskAsyncExecute() throws Exception {
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = environment.socketTextStream("localhost", 9999);
        DataStream<String> asyncStream = AsyncDataStream.unorderedWait(stream, new AsyncReadFunction(), 10, TimeUnit.SECONDS);
        asyncStream.print();
        environment.execute();
    }

}