package org.example.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class FlinkKafkaConfig {

    private String bootstrapServers = "localhost:9092";
    private String groupId = "s3-to-hdfs-consumer";
    private String topicPattern = "etl.*";
}
