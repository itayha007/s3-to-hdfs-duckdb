package org.example.service;

import lombok.extern.log4j.Log4j2;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.example.model.KafkaMessage;
import org.example.model.PipelineSchema;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

@Log4j2
public class DuckDbEnricherFunction extends RichFlatMapFunction<KafkaMessage, GenericRecord> {

    private final Properties duckDbProperties;
    private final String s3Endpoint;
    private final String s3AccessKey;
    private final String s3SecretKey;
    private final String s3Region;
    private final boolean s3PathStyle;
    private final SchemaService schemaService;

    private transient Connection connection;

    public DuckDbEnricherFunction(Properties duckDbProperties,
                                  String s3Endpoint,
                                  String s3AccessKey,
                                  String s3SecretKey,
                                  String s3Region,
                                  boolean s3PathStyle,
                                  SchemaService schemaService) {
        this.duckDbProperties = duckDbProperties;
        this.s3Endpoint = s3Endpoint;
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
        this.s3Region = s3Region;
        this.s3PathStyle = s3PathStyle;
        this.schemaService = schemaService;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:duckdb:", duckDbProperties);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSTALL httpfs");
            stmt.execute("LOAD httpfs");
            configureS3(stmt);
        }
    }

    @Override
    public void flatMap(KafkaMessage message, Collector<GenericRecord> out) throws Exception {
        if (message == null || message.getHeaders() == null || message.getKafkaReference() == null) {
            return;
        }

        String pipelineName = message.getHeaders().getPipelineName();
        String s3Uri = message.getKafkaReference().toS3Uri();

        PipelineSchema schema;
        try {
            schema = schemaService.getSchema(pipelineName);
        } catch (IllegalArgumentException e) {
            log.error("Unknown pipeline [{}], skipping message: {}", pipelineName, s3Uri);
            return;
        }

        Path tempParquet = Paths.get(System.getProperty("java.io.tmpdir"),
                "duckdb-flink-" + UUID.randomUUID() + ".parquet");

        try {
            convertToParquet(s3Uri, schema, tempParquet);
            emitRecords(tempParquet, pipelineName, out);
        } catch (IllegalArgumentException e) {
            log.error("Schema validation failed [pipeline={}, s3={}]: {}", pipelineName, s3Uri, e.getMessage());
        } catch (Exception e) {
            log.error("Failed to process [pipeline={}, s3={}]: {}", pipelineName, s3Uri, e.getMessage(), e);
        } finally {
            try {
                Files.deleteIfExists(tempParquet);
            } catch (IOException ignored) {
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private void convertToParquet(String s3Uri, PipelineSchema schema, Path output) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS _staging");
            loadIntoStaging(stmt, s3Uri, schema);
            validateRequiredFields(stmt, schema);
            writeParquet(stmt, output);
        }
    }

    private void loadIntoStaging(Statement stmt, String s3Uri, PipelineSchema schema) throws SQLException {
        String columnsDefinition = schema.getColumns().stream()
                .map(col -> String.format("'%s': '%s'", col.getName(), col.getDuckDbType()))
                .collect(Collectors.joining(", ", "{", "}"));
        try {
            stmt.execute(String.format(
                    "CREATE TEMP TABLE _staging AS " +
                    "SELECT * FROM read_json('%s', format='auto', columns=%s)",
                    s3Uri, columnsDefinition));
        } catch (SQLException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private void validateRequiredFields(Statement stmt, PipelineSchema schema) throws SQLException {
        for (var col : schema.getColumns()) {
            if (!col.isNullable()) {
                String checkQuery = String.format(
                        "SELECT 1 FROM _staging WHERE \"%s\" IS NULL LIMIT 1", col.getName());
                try (ResultSet rs = stmt.executeQuery(checkQuery)) {
                    if (rs.next()) {
                        throw new IllegalArgumentException(
                                String.format("Required field '%s' is missing or null", col.getName()));
                    }
                }
            }
        }
    }

    private void writeParquet(Statement stmt, Path output) throws SQLException {
        stmt.execute(String.format(
                "COPY (SELECT * FROM _staging) TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                output.toAbsolutePath()));
    }

    private void emitRecords(Path parquetPath, String pipelineName, Collector<GenericRecord> out)
            throws IOException {
        // Force local FS so Hadoop doesn't resolve the temp path via HDFS.
        // Also project into the expected Avro schema so the GenericRecord's field
        // indices match exactly what FlinkHdfsSink's ParquetWriterFactory expects.
        org.apache.hadoop.conf.Configuration localConf = new org.apache.hadoop.conf.Configuration();
        localConf.set("fs.defaultFS", "file:///");
        Schema targetSchema = schemaService.getFinalSchemaByPipelineName(pipelineName);
        AvroReadSupport.setAvroReadSchema(localConf, targetSchema);

        org.apache.hadoop.fs.Path hadoopPath =
                new org.apache.hadoop.fs.Path(parquetPath.toAbsolutePath().toString());
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(hadoopPath)
                .withConf(localConf)
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                record.put("pipelineName", pipelineName);
                out.collect(record);
            }
        }
    }

    private void configureS3(Statement stmt) throws SQLException {
        URI endpoint = URI.create(this.s3Endpoint);
        stmt.execute(String.format(
                "CREATE OR REPLACE SECRET (TYPE 'S3', KEY_ID '%s', SECRET '%s', REGION '%s', " +
                "ENDPOINT '%s:%d', USE_SSL %s, URL_STYLE '%s')",
                this.s3AccessKey,
                this.s3SecretKey,
                this.s3Region,
                endpoint.getHost(),
                endpoint.getPort(),
                "https".equalsIgnoreCase(endpoint.getScheme()),
                this.s3PathStyle ? "path" : "vhost"));
    }
}
