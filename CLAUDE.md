# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
mvn compile          # compile
mvn package          # build fat jar
mvn test             # run tests (none yet)
mvn spring-boot:run  # run locally (requires LocalStack + Kafka + HDFS)
```

No linter is configured. Java 11, Spring Boot 2.7.3.

## Architecture

This is a Spring Boot service that consumes Kafka messages referencing S3 JSON files and writes them to HDFS as Parquet, batched by pipeline.

### Per-message flow

```
Kafka message (headers: source.type, pipeline.name)
  → KafkaConsumerService.consume()
  → deserialize body → KafkaReference (bucket + key)
  → DuckDbService.convertJsonToParquet(bucket, key, schema)
      1. CREATE TEMP TABLE _staging AS SELECT * FROM read_json('s3://...', columns={...})
         — type cast errors throw here (wrong types, bad arrays)
      2. For each required (non-nullable) field: COUNT(*) WHERE field IS NULL → throws if > 0
      3. COPY (SELECT * FROM _staging) TO '/tmp/....parquet'
  → BatchManager.add(pipelineName, parquetFile)
      — flushes when accumulated size >= 128 MB OR on 5-minute scheduled timer
      — flush: DuckDbService.mergeParquets([...]) → HdfsWriterService.writeToHdfs()
```

### Schema pipeline

Avro schemas are defined inline in `SchemaService` (mirroring `kafka-s3-flink2`'s `SchemaService.FINAL_SCHEMA_MAP`). `AvroToDuckDbConverter` maps them to DuckDB type strings and tracks nullability (`ColumnDefinition.nullable = false` when the Avro field is non-union and has no default). The `PipelineSchema` / `ColumnDefinition` objects are the only schema representation used at runtime — the Avro `Schema` object is not passed beyond `SchemaService`.

### S3 access

DuckDB reads S3 directly via its `httpfs` extension — there is no Java S3 client. Credentials are wired from `S3Config` (prefix `s3:`) into DuckDB `SET s3_*` statements inside `DuckDbService.configureDuckDb()`. The `s3.endpoint` supports custom endpoints (e.g. LocalStack at `http://localhost:4566`) with automatic SSL and path-style detection.

### HDFS output path

`{hdfs.output-base-path}/{pipelineName}/{yyyyMMdd}/{uuid}.parquet`

### Key design constraints

- DuckDB connections are opened per operation (no connection pool) — each is a fresh in-memory instance.
- `DuckDbService.newTempParquet()` creates then immediately deletes the temp file because DuckDB's `COPY TO` requires the target to not exist.
- `BatchManager` synchronizes on individual `PipelineBatch` objects; the flush itself runs outside the lock to avoid blocking ingestion.
- The Kafka listener runs with `concurrency: 1` — all processing is single-threaded per consumer.
