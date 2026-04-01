package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.example.config.DuckDbConfig;
import org.example.config.S3Config;
import org.example.model.KafkaReference;
import org.example.model.PipelineSchema;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

@Log4j2
@Service
@RequiredArgsConstructor
public class DuckDbService {

    private final DuckDbConfig duckDbConfig;
    private final S3Config s3Config;

    public Path convertJsonToParquet(KafkaReference ref, PipelineSchema schema) throws IOException, SQLException {
        Files.createDirectories(Path.of(duckDbConfig.getProperties().getProperty("temp_directory")));
        Path output = createTempParquetPath();
        String s3Uri  = ref.toS3Uri();

        log.info("Converting {} → Parquet [pipeline={}]", s3Uri, schema.getPipelineName());

        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            this.loadFromS3IntoStagingTable(stmt, s3Uri, schema);
            this.assertRequiredFieldsPresent(stmt, s3Uri, schema);
            this.writeParquet(stmt, output);
        }

        log.info("Parquet written: {} ({} KB)", output, Files.size(output) / 1024);
        return output;
    }

    public Path mergeParquets(List<Path> files) throws IOException, SQLException {
        Path output = createTempParquetPath();

        log.info("Merging {} Parquet file(s) → {}", files.size(), output);

        String fileList = files.stream()
                .map(p -> "'" + p.toAbsolutePath() + "'")
                .collect(Collectors.joining(", ", "[", "]"));

        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(
                    "COPY (SELECT * FROM read_parquet(%s)) TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                    fileList, output.toAbsolutePath()));
        }

        log.info("Merged Parquet: {} ({} MB)", output, Files.size(output) / (1024 * 1024));
        return output;
    }


    private void loadFromS3IntoStagingTable(Statement stmt, String s3Uri, PipelineSchema schema) throws SQLException {

        String columns = schema.getColumns().stream()
                .map(c -> c.getName() + ": '" + c.getDuckDbType() + "'")
                .collect(Collectors.joining(", ", "{", "}"));

        stmt.execute(String.format(
                "CREATE TEMP TABLE _staging AS SELECT * FROM read_json('%s', columns=%s, format='auto', maximum_object_size=%d)",
                s3Uri, columns, duckDbConfig.getMaximumObjectSize()));
    }


    private void assertRequiredFieldsPresent(Statement stmt, String s3Uri, PipelineSchema schema)
            throws SQLException {

        for (var col : schema.getColumns()) {
            if (col.isNullable()) continue;

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM _staging WHERE \"" + col.getName() + "\" IS NULL")) {
                rs.next();
                if (rs.getLong(1) > 0) {
                    throw new IllegalArgumentException(
                            "Required field '" + col.getName() + "' is missing or null in " + s3Uri);
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
        Connection conn = DriverManager.getConnection("jdbc:duckdb:", this.duckDbConfig.getProperties());
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("LOAD httpfs");
            configureS3(stmt);
        }
        return conn;
    }

    private void configureS3(Statement stmt) throws SQLException {
        URI endpoint = URI.create(this.s3Config.getEndpoint());
        stmt.execute(String.format(
                "CREATE OR REPLACE SECRET (" +
                "  TYPE '%s', KEY_ID '%s', SECRET '%s'," +
                "  REGION '%s', ENDPOINT '%s:%d', USE_SSL %b, URL_STYLE '%s'" +
                ")",
                "s3",
                this.s3Config.getAccessKey(),
                this.s3Config.getSecretKey(),
                this.s3Config.getRegion(),
                endpoint.getHost(), endpoint.getPort(),
                "https".equals(endpoint.getScheme()),
                this.s3Config.isPathStyleAccess() ? "path" : "vhost"));
    }

    private Path createTempParquetPath() throws IOException {
        Path p = Files.createTempFile("duckdb-", ".parquet");
        Files.delete(p);
        return p;
    }
}
