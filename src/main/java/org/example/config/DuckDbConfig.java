package org.example.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Data
@Configuration
@ConfigurationProperties(prefix = "duckdb")
public class DuckDbConfig {

    private Properties properties = new Properties();
}
