package org.example.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "batch")
public class BatchConfig {

    /** Accumulated Parquet bytes per pipeline before flushing to HDFS. Default: 128 MB. */
    private long maxParquetBytes = 134_217_728L; // 128 MB

    /** Maximum milliseconds before a non-empty batch is flushed regardless of size. */
    private long flushIntervalMs = 300_000L; // 5 minutes
}
