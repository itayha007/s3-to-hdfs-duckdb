package org.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.avro.Schema;
import org.example.config.DuckDbConfig;
import org.example.config.S3Config;
import org.example.model.PipelineSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link DuckDbService#explodeStagingTable} against the array-explosion fixtures
 * (mirrored from kafka-s3-flink2). Each fixture supplies an Avro schema, a JSON payload,
 * and the expected exploded rows.
 *
 * <p>Each case runs end-to-end through an in-memory DuckDB exactly as production does:
 * the payload is loaded into a typed {@code _staging} table via
 * {@code read_json(..., columns=...)}, {@code explodeStagingTable} swaps in the exploded
 * table, and the resulting {@code _staging} rows are compared — raw, with no normalisation —
 * against the fixture's expected output as one {@code ArrayNode} vs another.
 */
class ExplodeStagingTableTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DuckDbService duckDbService = new DuckDbService(new DuckDbConfig(), new S3Config());

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void explodesAsExpected(Case tc) throws Exception {
        PipelineSchema schema = AvroToDuckDbConverter.convert(tc.name, tc.avroSchema);

        Path payloadFile = Files.createTempFile("explode-payload-", ".json");
        try {
            Files.write(payloadFile, MAPPER.writeValueAsBytes(tc.payload));

            ArrayNode actual = runExplosion(schema, payloadFile);

            assertThat(actual).isEqualTo(tc.expected);
        } finally {
            Files.deleteIfExists(payloadFile);
        }
    }

    /** Loads the payload into {@code _staging}, runs {@code explodeStagingTable}, returns the exploded rows as a JSON array. */
    private ArrayNode runExplosion(PipelineSchema schema, Path payloadFile) throws Exception {
        String columns = schema.getColumns().stream()
                .map(c -> String.format("'%s': '%s'", c.getName(), c.getDuckDbType()))
                .collect(Collectors.joining(", ", "{", "}"));

        ArrayNode rows = MAPPER.createArrayNode();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            stmt.execute(String.format(
                    "CREATE TEMP TABLE _staging AS SELECT * FROM read_json('%s', format='auto', columns=%s)",
                    payloadFile.toAbsolutePath(), columns));

            duckDbService.explodeStagingTable(stmt, schema);

            try (ResultSet rs = stmt.executeQuery("SELECT to_json(t) AS j FROM _staging t")) {
                while (rs.next()) {
                    rows.add(MAPPER.readTree(rs.getString("j")));
                }
            }
        }
        return rows;
    }

    // -------------------------------------------------------------------------
    // Fixture loading
    // -------------------------------------------------------------------------

    static class Case {
        final String name;
        final Schema avroSchema;
        final JsonNode payload;
        final JsonNode expected;

        Case(String name, Schema avroSchema, JsonNode payload, JsonNode expected) {
            this.name = name;
            this.avroSchema = avroSchema;
            this.payload = payload;
            this.expected = expected;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<Case> cases() throws Exception {
        URL url = ExplodeStagingTableTest.class.getResource("/array-explosion-tests");
        Objects.requireNonNull(url, "Fixture directory not found");
        Path dir = Paths.get(url.toURI());

        try (Stream<Path> walk = Files.walk(dir)) {
            return walk
                    .filter(p -> p.toString().endsWith(".json"))
                    .sorted()
                    .map(ExplodeStagingTableTest::loadCase)
                    .collect(Collectors.toList())
                    .stream();
        }
    }

    private static Case loadCase(Path file) {
        try {
            JsonNode root = MAPPER.readTree(file.toFile());
            JsonNode input = root.get("input");
            Schema avro = new Schema.Parser().parse(MAPPER.writeValueAsString(input.get("schema")));
            JsonNode payload = input.get("payload");
            JsonNode expected = root.get("output");
            String name = file.getFileName().toString().replace(".json", "");
            return new Case(name, avro, payload, expected);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load fixture: " + file, e);
        }
    }
}
