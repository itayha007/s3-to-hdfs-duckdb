package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.example.config.DuckDbConfig;
import org.example.config.S3Config;
import org.example.model.PipelineSchema;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
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

    /**
     * Converts a single S3 JSON object (array or single object) to a small Parquet file
     * using the pipeline schema for typed columns.
     * Called once per Kafka message; the resulting Parquet is buffered in BatchManager.
     */
    public Path convertJsonToParquet(String bucket, String key, PipelineSchema schema)
            throws IOException, SQLException {

        Files.createDirectories(Path.of(duckDbConfig.getTempDirectory()));
        Path out = newTempParquet();

        String s3Uri = "s3://" + bucket + "/" + key;
        log.info("JSON→Parquet: {} [pipeline={}]", s3Uri, schema.getPipelineName());

        String columnsMap = schema.getColumns().stream()
                .map(c -> c.getName() + ": '" + c.getDuckDbType() + "'")
                .collect(Collectors.joining(", ", "{", "}"));

        String sql = String.format(
                "COPY (SELECT * FROM read_json('%s', columns=%s, format='auto')) " +
                "TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                s3Uri, columnsMap, out.toAbsolutePath());

        execute(sql);
        log.info("Small Parquet: {} ({} KB)", out, Files.size(out) / 1024);
        return out;
    }

    /**
     * Merges buffered small Parquet files into one file.
     * Parquet is self-describing so no schema is needed here —
     * read_parquet() reads the embedded schema from the files.
     */
    public Path mergeParquets(List<Path> parquetFiles) throws IOException, SQLException {
        Path out = newTempParquet();

        log.info("Merging {} Parquet file(s) → {}", parquetFiles.size(), out);

        String fileList = parquetFiles.stream()
                .map(p -> "'" + p.toAbsolutePath() + "'")
                .collect(Collectors.joining(", ", "[", "]"));

        String sql = String.format(
                "COPY (SELECT * FROM read_parquet(%s)) " +
                "TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                fileList, out.toAbsolutePath());

        execute(sql);
        log.info("Merged Parquet: {} ({} MB)", out, Files.size(out) / (1024 * 1024));
        return out;
    }

    private void execute(String sql) throws IOException, SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            stmt.execute("SET memory_limit='" + duckDbConfig.getMemoryLimit() + "'");
            stmt.execute("SET temp_directory='" + duckDbConfig.getTempDirectory() + "'");
            stmt.execute("SET threads=" + duckDbConfig.getThreads());

            stmt.execute("INSTALL httpfs");
            stmt.execute("LOAD httpfs");
            URI endpoint = URI.create(s3Config.getEndpoint());
            stmt.execute("SET s3_endpoint='" + endpoint.getHost() + ":" + endpoint.getPort() + "'");
            stmt.execute("SET s3_region='" + s3Config.getRegion() + "'");
            stmt.execute("SET s3_access_key_id='" + s3Config.getAccessKey() + "'");
            stmt.execute("SET s3_secret_access_key='" + s3Config.getSecretKey() + "'");
            stmt.execute("SET s3_use_ssl=" + ("https".equals(endpoint.getScheme()) ? "true" : "false"));
            stmt.execute("SET s3_url_style='" + (s3Config.isPathStyleAccess() ? "path" : "vhost") + "'");

            log.debug("DuckDB SQL: {}", sql);
            stmt.execute(sql);
        }
    }

    private Path newTempParquet() throws IOException {
        Path p = Files.createTempFile("duckdb-", ".parquet");
        Files.delete(p); // DuckDB requires the target to not exist
        return p;
    }
}
