package org.muieer.flink_practice.java.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FlinkMetricUtil {

    private static final Map<String, SimpleGauge> gaugeMap = new ConcurrentHashMap<>(16);
    private static final Map<String, MeterView> meterViewMap = new ConcurrentHashMap<>(16);

    // 异步并发调用可能导致重复创建 MeterView，产生 bug，所以此处做线程安全优化
    public static void addMonitorMetric(MetricGroup metricGroup, String metricName) {
        MeterView meterView = meterViewMap.get(metricName);
        if (meterView == null) {
            synchronized (FlinkMetricUtil.class) {
                if (meterViewMap.get(metricName) == null) {
                    meterView = metricGroup.meter(metricName, new MeterView(60));
                    meterViewMap.put(metricName, meterView);
                    meterView.markEvent();
                }
            }
        } else {
            meterView.markEvent();
        }
    }

    public static void gauge(MetricGroup metricGroup, String gaugeName, long value) {
        SimpleGauge gauge = gaugeMap.get(gaugeName);
        if (gauge == null) {
            synchronized (FlinkMetricUtil.class) {
                if (gaugeMap.get(gaugeName) == null) {
                    gauge = new SimpleGauge(value);
                    gaugeMap.put(gaugeName, gauge);
                    metricGroup.gauge(gaugeName, gauge);
                }
            }
        } else {
            gauge.updateValue(value);
        }
    }

    private static class SimpleGauge implements Gauge<Long> {

        private long value;

        public void updateValue(long value) {
            this.value = value;
        }

        public SimpleGauge(long value) {
            this.value = value;
        }

        @Override
        public Long getValue() {
            return value;
        }
    }

    private FlinkMetricUtil() {}
}
