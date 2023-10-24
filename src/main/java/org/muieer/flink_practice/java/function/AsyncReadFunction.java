package org.muieer.flink_practice.java.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.muieer.flink_practice.java.mock.MockDataBaseClient;

import java.util.List;
import java.util.concurrent.*;

public class AsyncReadFunction extends RichAsyncFunction<String, String> {

    private transient MockDataBaseClient client;
    private transient ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new MockDataBaseClient();
        executorService = Executors.newFixedThreadPool(8);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
//        invokeAsyncRead(input, resultFuture);
//        syncRead(input, resultFuture);
        wrapperSyncRead(input, resultFuture);
    }

    void invokeAsyncRead(String input, ResultFuture<String> resultFuture) {
        Future<String> res = client.asyncRead(input);
        CompletableFuture.supplyAsync(() -> {
                    try {
                        return res.get();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return "";
                    }
                })
                .thenAccept(str -> resultFuture.complete(List.of(str))); // 虽然是同步操作，但不会阻塞，因为上又是异步
        System.out.println(input);
    }

    void syncRead(String input, ResultFuture<String> resultFuture) {
        String res = client.syncRead(input);
        resultFuture.complete(List.of(res));
    }

    // 并发度受 executorService 线程数和平台 CPU 核心数影响
    void wrapperSyncRead(String input, ResultFuture<String> resultFuture) {
        var callable = new Callable<String>() {
            @Override
            public String call() throws Exception {
                return client.syncRead(input);
            }
        };
        Future<String> res = executorService.submit(callable);
        CompletableFuture.supplyAsync(() -> {
                    try {
                        return res.get();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return "";
                    }
                })
                .thenAccept(str -> resultFuture.complete(List.of(str))); // 虽然是同步操作，但不会阻塞，因为上又是异步
        System.out.println(input);
    }


    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {
        System.out.println(input + " timeout");
        resultFuture.complete(List.of());
    }
}
