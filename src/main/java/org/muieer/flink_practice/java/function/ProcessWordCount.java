package org.muieer.flink_practice.java.function;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class ProcessWordCount extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {

    // key, count, timestamp
    private transient ValueState<Tuple3<String, Integer, Long>> state;
    private transient Map<String, String> configMap;
    private transient int ttlSeconds;

    @Override
    public void open(Configuration parameters) throws Exception {

        configMap = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        ttlSeconds = Integer.parseInt(configMap.getOrDefault("ttlSeconds", "5"));

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(ttlSeconds + 1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        TypeInformation<Tuple3<String, Integer, Long>> typeInformation
                                            = TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>() {});

        ValueStateDescriptor<Tuple3<String, Integer, Long>> stateDescriptor
                                                        = new ValueStateDescriptor<>("word", typeInformation);
        stateDescriptor.enableTimeToLive(ttlConfig);

        state = getRuntimeContext().getState(stateDescriptor);
    }


    @Override
    public void processElement(
            Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>,
            Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Tuple3<String, Integer, Long> currentValue = state.value();
        if (currentValue == null) {
            currentValue = new Tuple3<>(value.f0, 0, 0L);
        }
        currentValue.f1 ++;
        currentValue.f2 = ctx.timerService().currentProcessingTime();
        state.update(currentValue);
        ctx.timerService().registerProcessingTimeTimer(currentValue.f2 + ttlSeconds * 1000L);
    }

    @Override
    public void onTimer(
            long timestamp, KeyedProcessFunction<String, Tuple2<String, Integer>,
            Tuple2<String, Integer>>.OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Tuple3<String, Integer, Long> currentValue = state.value();
        if (currentValue.f2 + ttlSeconds * 1000L <= timestamp) {
            out.collect(new Tuple2<>(currentValue.f0, currentValue.f1));
            state.clear();
        }
    }
}
