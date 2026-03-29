package org.example.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "duckdb")
public class DuckDbConfig {

    private String memoryLimit = "4GB";
    private String tempDirectory = "/tmp/duckdb_tmp";
    private int threads = 4;
}
