package org.example.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.example.model.KafkaReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

/**
 * End-to-end Kafka → S3 → DuckDB integration tests.
 *
 * Sends real Kafka messages referencing JSON files in LocalStack S3 and
 * verifies that the consumer correctly routes them through DuckDB validation
 * before handing off to BatchManager (mocked to avoid HDFS dependency).
 */
@SpringBootTest(properties = {
        "hdfs.namenode=file:///tmp",
        "duckdb.properties.temp_directory=/tmp/duckdb_kafka_integration_test",
        "kafka.topic=test.integration.orders",
        "kafka.group-id=test-integration-group"
})
@EmbeddedKafka(
        partitions = 1,
        topics = "test.integration.orders",
        bootstrapServersProperty = "kafka.bootstrap-servers"
)
@Testcontainers
@DirtiesContext
class KafkaConsumerIntegrationTest {

    static final String TOPIC = "test.integration.orders";
    static final String BUCKET = "kafka-consumer-test-bucket";

    @Container
    static LocalStackContainer localStack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.2"))
            .withServices(S3);

    @DynamicPropertySource
    static void s3Properties(DynamicPropertyRegistry registry) {
        registry.add("s3.endpoint", () -> localStack.getEndpointOverride(S3).toString());
        registry.add("s3.region", localStack::getRegion);
        registry.add("s3.access-key", localStack::getAccessKey);
        registry.add("s3.secret-key", localStack::getSecretKey);
        registry.add("s3.path-style-access", () -> "true");
    }

    /** Replaces the real BatchManager so HDFS is never invoked. */
    @MockBean
    BatchManager batchManager;

    @Autowired
    EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    ObjectMapper objectMapper;

    S3Client s3Client;
    KafkaTemplate<byte[], byte[]> kafkaTemplate;

    @BeforeEach
    void setUp() {
        s3Client = S3Client.builder()
                .endpointOverride(localStack.getEndpointOverride(S3))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                .region(Region.of(localStack.getRegion()))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();

        try {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        } catch (Exception ignored) { /* already exists across tests */ }

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    @AfterEach
    void tearDown() {
        if (s3Client != null) s3Client.close();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    KafkaReference uploadJson(String json, String s3Key) {
        s3Client.putObject(
                PutObjectRequest.builder().bucket(BUCKET).key(s3Key).build(),
                RequestBody.fromString(json, StandardCharsets.UTF_8));
        return KafkaReference.builder().bucket(BUCKET).key(s3Key).build();
    }

    void sendS3Message(String pipelineName, KafkaReference ref) throws Exception {
        byte[] body = objectMapper.writeValueAsBytes(ref);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, null, null, null, body,
                List.of(
                        new RecordHeader("source.type", "s3".getBytes()),
                        new RecordHeader("pipeline.name", pipelineName.getBytes())
                ));
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);
    }

    void sendNonS3Message(String pipelineName, KafkaReference ref) throws Exception {
        byte[] body = objectMapper.writeValueAsBytes(ref);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, null, null, null, body,
                List.of(
                        new RecordHeader("source.type", "kafka".getBytes()),
                        new RecordHeader("pipeline.name", pipelineName.getBytes())
                ));
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);
    }

    // =========================================================================
    // Happy-path: valid JSON → DuckDB validates → BatchManager.add called
    // =========================================================================

    @Test
    void validOrderMessage_batchManagerAddIsCalled() throws Exception {
        String json = "[{\"pipelineName\":\"etl_ecommerce_orders\",\"orderId\":\"ORD-001\",\"eventType\":\"placed\",\"occurredAt\":\"2024-01-15T10:00:00Z\",\"customerId\":\"CUST-123\",\"salesChannel\":\"web\",\"currency\":\"USD\",\"pricing\":{\"subtotal\":100.0,\"tax\":10.0,\"discounts\":5.0,\"totalAmount\":105.0},\"items\":[{\"sku\":\"SKU-A\",\"quantity\":2,\"unitPrice\":50.0}],\"shipping\":{\"city\":\"New York\",\"method\":\"standard\",\"promisedDate\":\"2024-01-20\"}}]";
        KafkaReference ref = uploadJson(json, "kafka/orders/valid.json");
        sendS3Message("etl_ecommerce_orders", ref);

        verify(batchManager, timeout(15_000).times(1))
                .add(eq("etl_ecommerce_orders"), any(Path.class), any());
    }

    @Test
    void validSupportTicketMessage_batchManagerAddIsCalled() throws Exception {
        String json = "[{\"pipelineName\":\"etl_support_tickets\",\"ticketId\":\"TKT-001\",\"customerId\":\"CUST-AAA\",\"channel\":\"email\",\"priority\":\"high\",\"openedAt\":\"2024-01-10T08:30:00Z\"}]";
        KafkaReference ref = uploadJson(json, "kafka/support_tickets/valid.json");
        sendS3Message("etl_support_tickets", ref);

        verify(batchManager, timeout(15_000).times(1))
                .add(eq("etl_support_tickets"), any(Path.class), any());
    }

    @Test
    void validPaymentMessage_batchManagerAddIsCalled() throws Exception {
        String json = "[{\"pipelineName\":\"etl_payments_clearing\",\"settlementId\":\"SETTLE-001\",\"transactionId\":\"TXN-PAY-001\",\"merchantId\":\"MERCH-XYZ\",\"processor\":\"stripe\",\"currency\":\"USD\",\"amount\":1500.75,\"fees\":{\"interchange\":22.51,\"processing\":7.50}}]";
        KafkaReference ref = uploadJson(json, "kafka/payments/valid.json");
        sendS3Message("etl_payments_clearing", ref);

        verify(batchManager, timeout(15_000).times(1))
                .add(eq("etl_payments_clearing"), any(Path.class), any());
    }

    // =========================================================================
    // Non-S3 source type: message is acked immediately, no DuckDB call
    // =========================================================================

    @Test
    void nonS3SourceType_messageIsSkippedWithoutCallingBatchManager() throws Exception {
        KafkaReference ref = KafkaReference.builder().bucket(BUCKET).key("some/key.json").build();
        sendNonS3Message("etl_support_tickets", ref);

        // Wait long enough to be confident the consumer processed but did not call batchManager
        verify(batchManager, after(5_000).never()).add(any(), any(), any());
    }

    // =========================================================================
    // Malformed message body: deserialization failure, message is skipped
    // =========================================================================

    @Test
    void malformedJsonBody_messageIsSkippedWithoutCallingBatchManager() throws Exception {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, null, null, null,
                "{ this is not valid json }".getBytes(),
                List.of(
                        new RecordHeader("source.type", "s3".getBytes()),
                        new RecordHeader("pipeline.name", "etl_support_tickets".getBytes())
                ));
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        verify(batchManager, after(5_000).never()).add(any(), any(), any());
    }
}
