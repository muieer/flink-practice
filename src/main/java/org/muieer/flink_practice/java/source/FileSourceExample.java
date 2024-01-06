package org.muieer.flink_practice.java.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/*
* https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/filesystem/#filesystem
* */
public class FileSourceExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSourceExample.class);

    public static void main(String[] args) throws Exception{

        var path = args[0];
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(path))
                .monitorContinuously(Duration.ofSeconds(10))
                .build();

        WatermarkStrategy<String> noWatermarks = WatermarkStrategy.noWatermarks();
        env.fromSource(fileSource, noWatermarks, "fileSource")
                .map(line -> {
                    LOGGER.info("input is : {}", line);
                    return line;
                });

        env.execute();
    }

}
