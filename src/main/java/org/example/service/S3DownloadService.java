package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Log4j2
@Service
@RequiredArgsConstructor
public class S3DownloadService {

    private final S3Client s3Client;

    /**
     * Downloads an S3 object to a temporary file on local disk.
     * Uses ResponseTransformer.toFile() which writes bytes directly
     * from the network stream to disk via the OS, bypassing JVM heap.
     */
    public Path downloadToTempFile(String bucket, String key) throws IOException {
        Path tempFile = Files.createTempFile("s3-download-", ".json");
        Files.delete(tempFile); // ResponseTransformer.toFile() uses Files.copy which requires the file not to exist
        log.info("Downloading s3://{}/{} to {}", bucket, key, tempFile);

        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        s3Client.getObject(request, ResponseTransformer.toFile(tempFile));

        log.info("Download complete: {} ({} bytes)", tempFile, Files.size(tempFile));
        return tempFile;
    }
}
