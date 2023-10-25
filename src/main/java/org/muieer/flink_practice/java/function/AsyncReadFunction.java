package org.muieer.flink_practice.java.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.muieer.flink_practice.java.mock.MockDataBaseClient;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class AsyncReadFunction extends RichAsyncFunction<String, String> {

    private transient MockDataBaseClient client;
    private transient ExecutorService executorService;
    private transient Random random;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new MockDataBaseClient();
        executorService = Executors.newFixedThreadPool(8);
        random = new Random();
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    // 借助 ResultFuture 完成异步回调
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        invokeAsyncRead(input, resultFuture);
//        syncRead(input, resultFuture);
//        wrapperSyncRead(input, resultFuture);
    }

    void invokeAsyncRead(String input, ResultFuture<String> resultFuture) {
        CompletableFuture.supplyAsync(() -> {
                    try {
                        return client.asyncRead(input).get(random.nextInt(5), TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new RuntimeException("has exception");
                    }
                })
                .handle((res, throwable) -> {
                    if (throwable != null) {
                        // 最终结果是这个的话，会停止服务
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        resultFuture.complete(List.of(res));
                    }
                    return res;
                }); // 虽然是同步操作，但不会阻塞，因为上一步是异步

        System.out.println("asyncInvoke, input is " + input);
    }

    void syncRead(String input, ResultFuture<String> resultFuture) {
        String res = client.syncRead(input);
        resultFuture.complete(List.of(res));
    }

    // 并发度受 executorService 线程数和 CPU 核心数影响
    void wrapperSyncRead(String input, ResultFuture<String> resultFuture) {
        var callable = new Callable<String>() {
            @Override
            public String call() throws Exception {
                return client.syncRead(input);
            }
        };
        CompletableFuture.supplyAsync(() -> {
                    try {
                        return executorService.submit(callable).get(random.nextInt(5), TimeUnit.SECONDS);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return "has exception";
                    }
                })
                .thenAccept(str -> {
                    if ("has exception".equals(str)) {
                        resultFuture.complete(List.of());
                    } else {
                        resultFuture.complete(List.of(str));
                    }
                }); // 虽然是同步操作，但不会阻塞，因为上一步是异步
        System.out.println("asyncInvoke, input is " + input);
    }

    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {
        System.out.println("asyncInvoke, input is " + input + " timeout");
    }

}
