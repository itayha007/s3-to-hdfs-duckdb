package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.example.config.DuckDbConfig;
import org.example.config.S3Config;
import org.example.model.ColumnDefinition;
import org.example.model.KafkaReference;
import org.example.model.PipelineSchema;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Log4j2
@Service
@RequiredArgsConstructor
public class DuckDbService {

    private final DuckDbConfig duckDbConfig;
    private final S3Config s3Config;

    public Path convertJsonToParquet(KafkaReference ref, PipelineSchema schema) throws IOException, SQLException {
        String tempDir = duckDbConfig.getProperties().getProperty("temp_directory", "/tmp");
        Files.createDirectories(Path.of(tempDir));

        Path output = createTempParquetPath();
        String s3Uri = ref.toS3Uri();

        log.info("Converting {} -> Parquet [pipeline={}]", s3Uri, schema.getPipelineName());

        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            this.loadFromS3IntoRawStagingTable(stmt, s3Uri, schema);
            this.validateRequiredFields(stmt, schema);
            this.writeParquet(stmt, output);
        }

        log.info("Parquet written: {} ({} KB)", output, Files.size(output) / 1024);
        return output;
    }

    public Path mergeParquets(List<Path> files) throws IOException, SQLException {
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("files must not be null or empty");
        }

        Path output = createTempParquetPath();
        log.info("Merging {} Parquet file(s) -> {}", files.size(), output);

        String fileList = files.stream()
                .map(path -> String.format("'%s'", path.toAbsolutePath()))
                .collect(Collectors.joining(", ", "[", "]"));

        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(
                    "COPY (SELECT * FROM read_parquet(%s)) TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                    fileList,
                    output.toAbsolutePath()));
        }

        return output;
    }

    private void loadFromS3IntoRawStagingTable(Statement stmt, String s3Uri, PipelineSchema schema) throws SQLException {
        String columnsDefinition = schema.getColumns().stream()
                .map(col -> String.format("'%s': '%s'", col.getName(), col.getDuckDbType()))
                .collect(Collectors.joining(", ", "{", "}"));

        stmt.execute("DROP TABLE IF EXISTS _staging");
        try {
            stmt.execute(String.format(
                    "CREATE TEMP TABLE _staging AS " +
                            "SELECT * FROM read_json('%s', format='auto', columns=%s)",
                    s3Uri,
                    columnsDefinition));
        }catch (SQLException e){
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private void validateRequiredFields(Statement stmt, PipelineSchema schema) throws SQLException {
        for (ColumnDefinition col : schema.getColumns()) {
            if (!col.isNullable()) {
                String checkQuery = String.format("SELECT 1 FROM _staging WHERE %s IS NULL LIMIT 1", col.getName());
                if (exists(stmt, checkQuery)) {
                    throw new IllegalArgumentException(
                            String.format("Required field '%s' is missing or null",
                                    col.getName()));
                }
            }
        }
    }

    private void writeParquet(Statement stmt, Path output) throws SQLException {
        stmt.execute(String.format(
                "COPY (SELECT * FROM _staging) TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                output.toAbsolutePath()));
    }

    private Connection connect() throws SQLException {
        Properties props = duckDbConfig.getProperties();
        Connection conn = DriverManager.getConnection("jdbc:duckdb:", props);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL httpfs");
            stmt.execute("LOAD httpfs");
            configureS3(stmt);
        }
        return conn;
    }

    private void configureS3(Statement stmt) throws SQLException {
        URI endpoint = URI.create(this.s3Config.getEndpoint());
        stmt.execute(String.format(
                "CREATE OR REPLACE SECRET (" +
                        "TYPE 'S3', " +
                        "KEY_ID '%s', " +
                        "SECRET '%s', " +
                        "REGION '%s', " +
                        "ENDPOINT '%s:%d', " +
                        "USE_SSL %s, " +
                        "URL_STYLE '%s'" +
                        ")",
                this.s3Config.getAccessKey(),
                this.s3Config.getSecretKey(),
                this.s3Config.getRegion(),
                endpoint.getHost(),
                endpoint.getPort(),
                "https".equalsIgnoreCase(endpoint.getScheme()),
                this.s3Config.isPathStyleAccess() ? "path" : "vhost"));
    }

    private boolean exists(Statement stmt, String query) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(query)) {
            return rs.next();
        }
    }

    private Path createTempParquetPath() throws IOException {
        Path path = Files.createTempFile("duckdb-", ".parquet");
        Files.delete(path);
        return path;
    }
}