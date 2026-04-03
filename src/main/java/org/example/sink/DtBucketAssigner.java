package org.example.sink;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import static org.example.utils.DtUtils.getCurrentEpoch;
import static org.example.utils.DtUtils.getDtFromEpoch;

public class DtBucketAssigner implements BucketAssigner<GenericRecord, String> {

    @Override
    public String getBucketId(GenericRecord genericRecord, Context context) {
        return String.format("temp/%s/dt=%s", genericRecord.get("pipelineName"), getDtFromEpoch(getCurrentEpoch()));
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
