package org.example.service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Getter
@RequiredArgsConstructor
class PipelineBatch {

    @Value
    static class BatchEntry {
        Path path;
        long bytes;
        @JsonIgnore Acknowledgment ack;
    }

    private final String pipelineName;
    private final List<BatchEntry> entries = new ArrayList<>();
    private long totalBytes = 0;
    private long lastFlushedAt = System.currentTimeMillis();

    void add(Path file, long bytes, Acknowledgment ack) {
        entries.add(new BatchEntry(file, bytes, ack));
        totalBytes += bytes;
    }

    boolean isEmpty() {
        return entries.isEmpty();
    }

    List<BatchEntry> drain() {
        List<BatchEntry> snapshot = new ArrayList<>(entries);
        entries.clear();
        totalBytes = 0;
        lastFlushedAt = System.currentTimeMillis();
        return snapshot;
    }
}
