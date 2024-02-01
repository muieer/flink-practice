package org.muieer.flink_practice.java.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/*
* 计算一个窗口中出现过多少种不同的字符串
* */
public class DistinctAggregateFunction implements AggregateFunction<String, Set<String>, Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistinctAggregateFunction.class);

    @Override
    public Set<String> createAccumulator() {
        return new HashSet<>();
    }

    @Override
    public Set<String> add(String value, Set<String> accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public Integer getResult(Set<String> accumulator) {
        LOGGER.info(String.format("出现的字符串有 %s", accumulator));
        return accumulator.size();
    }

    @Override
    public Set<String> merge(Set<String> a, Set<String> b) {
        a.addAll(b);
        return a;
    }

}
