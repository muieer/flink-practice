package org.muieer.flink_practice.java.state;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
* https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/fault-tolerance/checkpointing/
* https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/concepts/stateful-stream-processing/
* */
public class EnableCheckpointing {

    public static void enableCheckpointing(StreamExecutionEnvironment env) {

//        env.enableCheckpointing(1000);

        /*
        * CheckpointInterval 和 MinPauseBetweenCheckpoints 之间的关联
        * 假设执行一次 cp 的时间是 t，上一次开始执行 cp 的时间是 c
        * if t + MinPauseBetweenCheckpoints < CheckpointInterval，下一次执行 cp 的时间是 c + CheckpointInterval
        * else 下一次执行 cp 的时间是 c + t + MinPauseBetweenCheckpoints
        * */
        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        // 周期性执行 cp 的等待时长
        config.setCheckpointInterval(3000);
        config.setCheckpointTimeout(10_000);
        config.setMaxConcurrentCheckpoints(2);
        // 上一次成功 cp 后，触发下一次 cp 的最小等待时间
        config.setMinPauseBetweenCheckpoints(3000);
        config.setTolerableCheckpointFailureNumber(1);
    }
}
