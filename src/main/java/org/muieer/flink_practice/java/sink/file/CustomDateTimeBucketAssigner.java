package org.muieer.flink_practice.java.sink.file;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class CustomDateTimeBucketAssigner implements BucketAssigner<Tuple2<Long, String>, String> {

    private static final String DEFAULT_FORMAT_STRING = "yyyyMMdd_HHmm";
    private transient DateTimeFormatter dateTimeFormatter;

    public CustomDateTimeBucketAssigner() {
    }

    @Override
    public String getBucketId(Tuple2<Long, String> element, Context context) {
        if (dateTimeFormatter == null) {
            dateTimeFormatter = DateTimeFormatter.ofPattern(DEFAULT_FORMAT_STRING).withZone(ZoneId.systemDefault());
        }
        return dateTimeFormatter.format(Instant.ofEpochMilli(element.f0));
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }

}
