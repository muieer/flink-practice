package org.muieer.flink_practice.java.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.muieer.flink_practice.java.mock.MockDataBaseClient;

public class SyncReadFunction extends RichMapFunction<String, String> {

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
    public String map(String input) throws Exception {
        return client.read(input).get();
    }
}
