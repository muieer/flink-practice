package org.muieer.flink_practice.java.mock;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.*;

public class MockDataBaseClient {

    private static final Random random = new Random();
    private static final ExecutorService executorService = Executors.newFixedThreadPool(2);

    public Future<String> read(String input) {
        var callable = new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.sleep(1000 * (random.nextInt(5) + 2));
                return input + " query result output time is " + LocalDateTime.now();
            }
        };
        return executorService.submit(callable);
    }

    public void close() {
        System.out.println("连接关闭！");
    }

}