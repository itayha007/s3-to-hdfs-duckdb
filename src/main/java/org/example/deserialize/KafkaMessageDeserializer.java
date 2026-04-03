package org.example.deserialize;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.example.model.Headers;
import org.example.model.KafkaMessage;
import org.example.model.KafkaReference;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Log4j2
public class KafkaMessageDeserializer implements KafkaRecordDeserializationSchema<KafkaMessage> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<KafkaMessage> out) throws IOException {
        try {
            String sourceType = getHeader(record, "source.type");
            String pipelineName = getHeader(record, "pipeline.name");

            KafkaReference ref = MAPPER.readValue(record.value(), KafkaReference.class);

            out.collect(KafkaMessage.builder()
                    .headers(Headers.builder()
                            .sourceType(sourceType)
                            .pipelineName(pipelineName)
                            .build())
                    .kafkaReference(ref)
                    .build());
        } catch (Exception e) {
            log.error("Failed to deserialize Kafka message from topic={} partition={} offset={}: {}",
                    record.topic(), record.partition(), record.offset(), e.getMessage());
        }
    }

    @Override
    public TypeInformation<KafkaMessage> getProducedType() {
        return TypeInformation.of(KafkaMessage.class);
    }

    private String getHeader(ConsumerRecord<byte[], byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        if (header == null || header.value() == null) return null;
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
