package org.example;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.example.config.DuckDbConfig;
import org.example.config.FlinkJobConfig;
import org.example.config.FlinkKafkaConfig;
import org.example.config.HdfsConfig;
import org.example.config.S3Config;
import org.example.deserialize.KafkaMessageDeserializer;
import org.example.model.KafkaMessage;
import org.example.service.DuckDbEnricherFunction;
import org.example.service.SchemaService;
import org.example.sink.FlinkHdfsSink;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.regex.Pattern;

@Log4j2
@Component
@RequiredArgsConstructor
public class FlinkPipelineRunner implements CommandLineRunner {

    private final FlinkJobConfig flinkJobConfig;
    private final FlinkKafkaConfig kafkaConfig;
    private final HdfsConfig hdfsConfig;
    private final DuckDbConfig duckDbConfig;
    private final S3Config s3Config;
    private final SchemaService schemaService;
    private final FlinkHdfsSink flinkHdfsSink;

    // A minimal schema used for type information — pipelineName is what DtBucketAssigner reads
    private static final String EMPTY_SCHEMA_JSON =
            "{\"type\":\"record\", \"name\":\"EmptyRecord\", \"fields\":[]}";

    @Override
    public void run(String... args) throws Exception {
        org.apache.flink.configuration.Configuration cfg =
                new org.apache.flink.configuration.Configuration();
        cfg.setString("metrics.reporters", "prom");
        cfg.setString("metrics.reporter.prom.factory.class",
                "org.apache.flink.metrics.prometheus.PrometheusReporterFactory");
        cfg.setString("metrics.reporter.prom.port", "9250-9255");

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(cfg)
                .enableCheckpointing(flinkJobConfig.getCheckpointIntervalMs())
                .setParallelism(flinkJobConfig.getParallelism());
        env.getConfig().enableObjectReuse();

        KafkaSource<KafkaMessage> kafkaSource = KafkaSource.<KafkaMessage>builder()
                .setBootstrapServers(kafkaConfig.getBootstrapServers())
                .setTopicPattern(Pattern.compile(kafkaConfig.getTopicPattern()))
                .setGroupId(kafkaConfig.getGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaMessageDeserializer())
                .setProperty("partition.discovery.interval.ms", "3000")
                .build();

        DuckDbEnricherFunction enricher = new DuckDbEnricherFunction(
                duckDbConfig.getProperties(),
                s3Config.getEndpoint(),
                s3Config.getAccessKey(),
                s3Config.getSecretKey(),
                s3Config.getRegion(),
                s3Config.isPathStyleAccess(),
                schemaService
        );

        SingleOutputStreamOperator<GenericRecord> enriched = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .filter(msg -> msg != null
                        && msg.getHeaders() != null
                        && "s3".equals(msg.getHeaders().getSourceType()))
                .uid("s3-filter")
                .flatMap(enricher)
                .uid("duckdb-enricher")
                .filter(Objects::nonNull)
                .uid("null-filter");

        enriched
                .returns(new GenericRecordAvroTypeInfo(
                        new Schema.Parser().parse(EMPTY_SCHEMA_JSON)))
                .uid("type-info")
                .sinkTo(flinkHdfsSink.build(schemaService))
                .uid("hdfs-sink");

        log.info("Starting Flink pipeline [checkpoint={}ms, parallelism={}, compactOnCheckpoint={}, sizeThresholdMb={}]",
                flinkJobConfig.getCheckpointIntervalMs(),
                flinkJobConfig.getParallelism(),
                hdfsConfig.getCompactOnCheckpoint(),
                hdfsConfig.getSizeThresholdMb());

        env.execute("s3-to-hdfs-duckdb-flink");
    }

}
