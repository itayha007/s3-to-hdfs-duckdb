# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build
mvn clean install

# Run all tests
mvn clean test

# Run a specific test class
mvn test -Dtest=DuckDbServiceS3IntegrationTest
mvn test -Dtest=KafkaConsumerIntegrationTest

# Run a specific test method
mvn test -Dtest=DuckDbServiceS3IntegrationTest#testInvalidJson

# Start local infrastructure (S3, Kafka, Zookeeper)
docker-compose up -d

# Run application
mvn spring-boot:run
```

## Architecture

This is a Spring Boot pipeline that reads JSON files from S3 via Kafka messages, validates and converts them to Parquet using DuckDB, batches them, and writes to HDFS.

### Data Flow

```
Kafka Message (headers: source.type, pipeline.name)
    → KafkaConsumerService  (deserializes S3 bucket/key reference)
    → DuckDbService         (reads S3 JSON, validates schema, writes temp Parquet)
    → BatchManager          (accumulates Parquets per pipeline; flush on 128 MB or 5 min)
    → DuckDbService.mergeParquets()  (merges batch into single Parquet)
    → HdfsWriterService     (writes to HDFS at /{basePath}/{pipeline}/{yyyyMMdd}/{uuid}.parquet)
    → Kafka ACK             (only after successful HDFS write)
```

### Key Services

- **`KafkaConsumerService`** — Consumes Kafka messages, skips non-S3 sources, routes to DuckDB per pipeline key.
- **`DuckDbService`** — Core logic: loads S3 JSON into a staging table, validates non-nullable columns, CASTs to typed columns, writes Parquet (SNAPPY). Also merges Parquet files. Uses DuckDB's httpfs extension for S3 access.
- **`BatchManager`** — Thread-safe per-pipeline batch accumulator. Scheduled flush check every 10 seconds. On flush, merges all batch Parquets, writes to HDFS, ACKs Kafka, cleans temp files.
- **`SchemaService`** — Registry of 10 hardcoded Avro schemas (named `etl_*`). Converts Avro schema to `PipelineSchema` (list of `ColumnDefinition` with DuckDB SQL types).
- **`AvroToDuckDbConverter`** — Maps Avro types to DuckDB types: Record→STRUCT, Array→Type[], Map→MAP(VARCHAR, Type), Union with null → nullable column.

### Configuration (`application.yml`)

Key tunables:
- `duckdb.memoryLimit` — DuckDB memory (default 16 GB)
- `duckdb.maxObjectSize` — Max single object size (default 2 GB)
- `batch.maxParquetBytes` — Flush threshold (default 128 MB)
- `batch.flushIntervalMs` — Time-based flush (default 60 s, effectively 5 min in practice)
- `hdfs.outputBasePath` — HDFS output root (default `/data`)
- `s3.endpoint` — S3 endpoint (default `http://localhost:4566` for LocalStack)

### Testing

Integration tests use **Testcontainers + LocalStack** for S3. `DuckDbServiceS3IntegrationTest` auto-discovers fixtures from `src/test/resources/json/orders/`:
- `valid/` — JSON files expected to convert successfully
- `invalid/primitive/` — Missing required scalar fields
- `invalid/array/` — Null array fields
- `invalid/object/` — Wrong type in nested structs
- `invalid/batch/` — Multi-record files with partial invalidity

`KafkaConsumerIntegrationTest` uses `@EmbeddedKafka` + LocalStack and mocks `BatchManager` to avoid HDFS dependency.

Kafka consumer uses **manual acknowledgment** — messages are only ACKed after successful HDFS write. Failures leave messages unacknowledged for redelivery on restart.
