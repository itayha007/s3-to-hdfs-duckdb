package org.example.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "flink")
public class FlinkJobConfig {

    private long checkpointIntervalMs = 60_000L;
    private int parallelism = 4;
}
