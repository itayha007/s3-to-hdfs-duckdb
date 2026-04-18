package org.example.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "postgres")
public class PostgresConfig {

    private ServerConfig source = new ServerConfig();
    private ServerConfig target = new ServerConfig();

    @Data
    public static class ServerConfig {
        private String host = "localhost";
        private int port = 5432;
        private String dbname;
        private String user;
        private String password;
        private String schema = "public";

        public String toConnectionString() {
            return String.format("host=%s port=%d dbname=%s user=%s password=%s",
                    host, port, dbname, user, password);
        }
    }
}
