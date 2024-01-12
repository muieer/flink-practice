package org.muieer.flink_practice.java.sink.file;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.muieer.flink_practice.java.state.EnableCheckpointing.enableCheckpointing;

import java.time.Duration;
import java.time.Instant;

/*
* https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/filesystem/#file-sink
* */
public class FileSinkExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSinkExample.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        enableCheckpointing(env);

        var stream = env.socketTextStream("localhost", 9999)
                .map(str -> Tuple2.of(Instant.now().toEpochMilli(), str))
                // 解决泛型擦除无法推断正确类型的问题
                .returns(new TypeHint<Tuple2<Long, String>>() {});

        /*
        * 输出的文件有三种状态。分别是 In-progress（正在写）、Pending（关闭写等待提交）、Finished（提交成功）
        * 处于流处理模式时，Pending 文件只有在下一次 cp 成功后才会变成 Finished 状态
        * */
        // 满足以下三种条件之一就会结束当前文件分片的写入
        DefaultRollingPolicy<Tuple2<Long, String>, String> rollingPolicy = DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofSeconds(10))
                .withInactivityInterval(Duration.ofSeconds(10))
                .withMaxPartSize(MemorySize.parse("100", MemorySize.MemoryUnit.KILO_BYTES))
                .build();

        FileSink<Tuple2<Long, String>> fileSink = FileSink.forRowFormat(new Path(args[0]), new Tuple2Encoder())
                .withRollingPolicy(rollingPolicy)
                .withBucketAssigner(new CustomDateTimeBucketAssigner())
                .build();

        stream.sinkTo(fileSink);
        env.execute();
    }
}
