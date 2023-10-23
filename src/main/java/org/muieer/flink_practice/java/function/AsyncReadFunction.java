package org.muieer.flink_practice.java.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.muieer.flink_practice.java.mock.MockDataBaseClient;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class AsyncReadFunction extends RichAsyncFunction<String, String> {

    private transient MockDataBaseClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new MockDataBaseClient();
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        Future<String> res = client.read(input);
        CompletableFuture.supplyAsync(() -> {
            try {
                return res.get();
            } catch (Exception e) {
                e.printStackTrace();
                return "";
            }
        }).thenAccept(str -> resultFuture.complete(List.of(str)));
    }

    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
    }
}
