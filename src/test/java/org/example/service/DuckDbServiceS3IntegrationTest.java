package org.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.config.DuckDbConfig;
import org.example.config.S3Config;
import org.example.model.KafkaReference;
import org.example.model.PipelineSchema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

/**
 * Integration tests for {@link DuckDbService}.
 *
 * Fixtures live under src/test/resources/json/orders/{valid|invalid}/...
 * Each JSON file has the shape:
 *
 *   { "input": [...], "expectedErrorMessage": "fieldName" }
 *
 * Valid fixtures have an empty "expectedErrorMessage".
 * The test discovers all .json files under each directory automatically —
 * adding a new case requires only dropping in a new file.
 */
@Testcontainers
class DuckDbServiceS3IntegrationTest {

    static final String PIPELINE = "etl_ecommerce_orders";

    @Container
    static LocalStackContainer localStack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.2"))
            .withServices(S3);

    static S3Client s3Client;
    static DuckDbService duckDbService;
    static SchemaService schemaService;

    static final String BUCKET = "duckdb-test-bucket";
    static final AtomicInteger KEY_SEQ = new AtomicInteger();
    static final ObjectMapper MAPPER = new ObjectMapper();

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @BeforeAll
    static void setup() {
        s3Client = S3Client.builder()
                .endpointOverride(localStack.getEndpointOverride(S3))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                .region(Region.of(localStack.getRegion()))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();

        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());

        S3Config s3Config = new S3Config();
        s3Config.setEndpoint(localStack.getEndpointOverride(S3).toString());
        s3Config.setRegion(localStack.getRegion());
        s3Config.setAccessKey(localStack.getAccessKey());
        s3Config.setSecretKey(localStack.getSecretKey());
        s3Config.setPathStyleAccess(true);

        DuckDbConfig duckDbConfig = new DuckDbConfig();
        duckDbConfig.getProperties().setProperty("temp_directory", "/tmp/duckdb_s3_integration_test");

        duckDbService = new DuckDbService(duckDbConfig, s3Config);
        schemaService  = new SchemaService();
    }

    @AfterAll
    static void teardown() {
        if (s3Client != null) s3Client.close();
    }

    // -------------------------------------------------------------------------
    // TestCase — loaded from each fixture file
    // -------------------------------------------------------------------------

    static class TestCase {
        /** Relative path inside the scanned directory, used as the display name. */
        final String name;
        /** Raw JSON array extracted from the fixture's "input" field. */
        final String inputJson;
        /** Substring the thrown exception must contain; empty means valid (no error). */
        final String expectedErrorMessage;

        TestCase(String name, String inputJson, String expectedErrorMessage) {
            this.name = name;
            this.inputJson = inputJson;
            this.expectedErrorMessage = expectedErrorMessage;
        }

        @Override
        public String toString() { return name; }
    }

    // -------------------------------------------------------------------------
    // Directory scanner — discovers all .json fixtures under a classpath dir
    // -------------------------------------------------------------------------

    static Stream<TestCase> discoverCases(String classPathDir) throws Exception {
        URL url = DuckDbServiceS3IntegrationTest.class.getResource(classPathDir);
        Objects.requireNonNull(url, "Fixture directory not found: " + classPathDir);
        Path dir = Paths.get(url.toURI());

        try (Stream<Path> walk = Files.walk(dir)) {
            return walk
                    .filter(p -> p.toString().endsWith(".json"))
                    .sorted()
                    .map(p -> {
                        try {
                            JsonNode root = MAPPER.readTree(p.toFile());
                            String inputJson = MAPPER.writeValueAsString(root.get("input"));
                            String error     = root.path("expectedErrorMessage").asText("");
                            String name      = dir.relativize(p).toString().replace(".json", "");
                            return new TestCase(name, inputJson, error);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to parse fixture: " + p, e);
                        }
                    })
                    .collect(Collectors.toList())
                    .stream();
        }
    }

    // -------------------------------------------------------------------------
    // S3 upload helper
    // -------------------------------------------------------------------------

    static KafkaReference upload(String inputJson) {
        String s3Key = "test/" + KEY_SEQ.incrementAndGet() + "/input.json";
        s3Client.putObject(
                PutObjectRequest.builder().bucket(BUCKET).key(s3Key).build(),
                RequestBody.fromString(inputJson));
        return KafkaReference.builder().bucket(BUCKET).key(s3Key).build();
    }

    static void deleteQuietly(Path p) {
        if (p != null) try { Files.deleteIfExists(p); } catch (Exception ignored) {}
    }

    // =========================================================================
    // Valid — all fixtures under /json/orders/valid/
    // =========================================================================

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class Valid {

        @ParameterizedTest(name = "{0}")
        @MethodSource("cases")
        void producesParquet(TestCase tc) throws Exception {
            PipelineSchema schema = schemaService.getSchema(PIPELINE);
            Path parquet = duckDbService.convertJsonToParquet(upload(tc.inputJson), schema);

            assertThat(parquet).exists().isRegularFile();
            assertThat(Files.size(parquet)).isGreaterThan(0);
            deleteQuietly(parquet);
        }

        Stream<TestCase> cases() throws Exception {
            return discoverCases("/json/orders/valid");
        }
    }

    // =========================================================================
    // Invalid — all fixtures under /json/orders/invalid/ (scanned recursively)
    //   non-nested/ — null required scalar (non-nested) field
    //   array/      — null or non-castable values in arrays; simple/ (flat arrays) and nested/ (array of objects)
    //   object/     — null struct or non-castable value inside a nested struct field
    //   batch/      — multi-record file where ≥1 row is invalid
    // =========================================================================

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class Invalid {

        @ParameterizedTest(name = "{0}")
        @MethodSource("cases")
        void throwsError(TestCase tc)  {
            PipelineSchema schema = schemaService.getSchema(PIPELINE);

            assertThatThrownBy(() -> duckDbService.convertJsonToParquet(upload(tc.inputJson), schema))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(tc.expectedErrorMessage);
        }

        Stream<TestCase> cases() throws Exception {
            return discoverCases("/json/orders/invalid");
        }
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    @Test
    void unknownPipeline_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> schemaService.getSchema("etl_unknown_pipeline"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("etl_unknown_pipeline");
    }

    @Test
    void mergeParquets_twoValidFiles_producesUnifiedParquet() throws Exception {
        PipelineSchema schema = schemaService.getSchema(PIPELINE);
        String inputJson = discoverCases("/json/orders/valid").findFirst().orElseThrow().inputJson;

        Path parquet1 = duckDbService.convertJsonToParquet(upload(inputJson), schema);
        Path parquet2 = duckDbService.convertJsonToParquet(upload(inputJson), schema);
        Path merged   = duckDbService.mergeParquets(List.of(parquet1, parquet2));

        assertThat(merged).exists().isRegularFile();
        assertThat(Files.size(merged)).isGreaterThan(0);

        deleteQuietly(parquet1);
        deleteQuietly(parquet2);
        deleteQuietly(merged);
    }
}
