package org.example.config;

import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@Data
@org.springframework.context.annotation.Configuration
@ConfigurationProperties(prefix = "hdfs")
public class HdfsConfig {

    private String namenode = "hdfs://localhost:8020";
    private String outputBasePath = "/data";

    @Bean
    public FileSystem hdfsFileSystem() throws IOException {
        Configuration hadoopConf = new Configuration();
        // core-site.xml and hdfs-site.xml on the classpath are auto-loaded by Hadoop
        hadoopConf.set("fs.defaultFS", namenode);
        return FileSystem.get(hadoopConf);
    }
}
