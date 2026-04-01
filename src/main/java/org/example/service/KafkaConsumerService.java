package org.example.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.model.Headers;
import org.example.model.KafkaReference;
import org.example.model.PipelineSchema;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;


@Log4j2
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final DuckDbService duckDbService;
    private final SchemaService schemaService;
    private final BatchManager batchManager;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecord<byte[], byte[]> record, Acknowledgment acknowledgment) {
        Headers headers = extractHeaders(record);
        log.info("Received: sourceType={}, pipelineName={}, partition={}, offset={}",
                headers.getSourceType(), headers.getPipelineName(),
                record.partition(), record.offset());

        if (!"s3".equals(headers.getSourceType())) {
            log.warn("Skipping non-s3 message (sourceType={})", headers.getSourceType());
            acknowledgment.acknowledge();
            return;
        }

        KafkaReference ref;
        try {
            ref = objectMapper.readValue(record.value(), KafkaReference.class);
        } catch (Exception e) {
            log.error("Failed to deserialize KafkaReference, skipping poison pill", e);
            acknowledgment.acknowledge();
            return;
        }


        String pipelineName = headers.getPipelineName();
        PipelineSchema schema = schemaService.getSchema(pipelineName);
        this.processKey(ref, pipelineName, schema, acknowledgment);
    }

    /**
     * Per-key flow:
     *  1. Validate JSON against Avro schema (missing required fields, wrong types, bad arrays)
     *  2. DuckDB reads JSON directly from S3 → small Parquet  (off-heap, schema-typed)
     *  3. Hand Parquet to BatchManager — flushed to HDFS when 128 MB or 5 min threshold hit
     */
    private void processKey(KafkaReference ref, String pipelineName,
                             PipelineSchema schema, Acknowledgment acknowledgment) {
        Path parquetTempFile = null;
        try {
            parquetTempFile = duckDbService.convertJsonToParquet(ref, schema);
            batchManager.add(pipelineName, parquetTempFile, acknowledgment);
            parquetTempFile = null; // BatchManager owns it now
        } catch (Exception e) {
            log.error("Failed to process {}", ref.toS3Uri(), e);
            deleteSilently(parquetTempFile);
            throw new RuntimeException("Failed to process " + ref.toS3Uri(), e);
        }
    }

    private Headers extractHeaders(ConsumerRecord<byte[], byte[]> record) {
        return Headers.builder()
                .sourceType(getHeaderValue(record.headers(), "source.type"))
                .pipelineName(getHeaderValue(record.headers(), "pipeline.name"))
                .build();
    }

    private String getHeaderValue(org.apache.kafka.common.header.Headers headers, String name) {
        return Optional.ofNullable(headers.lastHeader(name))
                .map(h -> new String(h.value()))
                .orElse("");
    }

    private void deleteSilently(Path path) {
        if (path != null) {
            try { Files.deleteIfExists(path); } catch (Exception ignored) {}
        }
    }
}
