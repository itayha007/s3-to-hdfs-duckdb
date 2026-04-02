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
        Files.createDirectories(Path.of(duckDbConfig.getProperties().getProperty("temp_directory", "/tmp")));
        Path output = createTempParquetPath();
        String s3Uri = ref.toS3Uri();

        log.info("Converting {} -> Parquet [pipeline={}]", s3Uri, schema.getPipelineName());

        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            this.loadFromS3IntoRawStagingTable(stmt, s3Uri);
            this.validateRequiredFields(stmt, schema);
            this.createTypedTable(stmt, schema);
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
                .map(path -> "'" + escapeSqlString(path.toAbsolutePath().toString()) + "'")
                .collect(Collectors.joining(", ", "[", "]"));

        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(
                    "COPY (SELECT * FROM read_parquet(%s)) TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                    fileList,
                    escapeSqlString(output.toAbsolutePath().toString())));
        }

        log.info("Merged Parquet written: {} ({} KB)", output, Files.size(output) / 1024);
        return output;
    }

    private void loadFromS3IntoRawStagingTable(Statement stmt, String s3Uri) throws SQLException {
        stmt.execute("DROP TABLE IF EXISTS _staging");
        stmt.execute(String.format(
                "CREATE TEMP TABLE _staging AS " +
                        "SELECT * FROM read_json('%s', format='auto')",
                escapeSqlString(s3Uri)));
    }

    private void validateRequiredFields(Statement stmt, PipelineSchema schema) throws SQLException {
        for (ColumnDefinition col : schema.getColumns()) {
            if (!col.isNullable()) {
                String quotedColumn = quoteIdentifier(col.getName());
                if (exists(stmt, "SELECT 1 FROM _staging WHERE " + quotedColumn + " IS NULL LIMIT 1")) {
                    throw new IllegalArgumentException(
                            String.format("Required field '%s' is missing or null", col.getName()));
                }
            }
        }
    }

    private void createTypedTable(Statement stmt, PipelineSchema schema) throws SQLException {
        stmt.execute("DROP TABLE IF EXISTS _typed");

        String typedProjection = schema.getColumns().stream()
                .map(col -> {
                    String quotedColumn = quoteIdentifier(col.getName());
                    return "CAST(" + quotedColumn + " AS " + col.getDuckDbType() + ") AS " + quotedColumn;
                })
                .collect(Collectors.joining(", "));

        try {
            stmt.execute("CREATE TEMP TABLE _typed AS SELECT " + typedProjection + " FROM _staging");
        } catch (SQLException e) {
            throw new IllegalArgumentException(
                    e.getMessage()
            );
        }
    }

    private boolean exists(Statement stmt, String sql) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next();
        }
    }

    private void writeParquet(Statement stmt, Path output) throws SQLException {
        stmt.execute(String.format(
                "COPY (SELECT * FROM _typed) TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                escapeSqlString(output.toAbsolutePath().toString())));
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
                        "TYPE '%s', " +
                        "KEY_ID '%s', " +
                        "SECRET '%s', " +
                        "REGION '%s', " +
                        "ENDPOINT '%s:%d', " +
                        "USE_SSL %s, " +
                        "URL_STYLE '%s'" +
                        ")",
                "s3",
                escapeSqlString(this.s3Config.getAccessKey()),
                escapeSqlString(this.s3Config.getSecretKey()),
                escapeSqlString(this.s3Config.getRegion()),
                escapeSqlString(endpoint.getHost()),
                endpoint.getPort(),
                Boolean.toString("https".equalsIgnoreCase(endpoint.getScheme())),
                this.s3Config.isPathStyleAccess() ? "path" : "vhost"));
    }

    private Path createTempParquetPath() throws IOException {
        Path path = Files.createTempFile("duckdb-", ".parquet");
        Files.delete(path);
        return path;
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private String escapeSqlString(String value) {
        return value.replace("'", "''");
    }
}