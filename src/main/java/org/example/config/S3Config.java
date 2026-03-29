package org.example.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "s3")
public class S3Config {

    private String endpoint;
    private String region;
    private String accessKey;
    private String secretKey;
    private boolean pathStyleAccess = true;
}
