package org.example.service;

import org.springframework.kafka.support.Acknowledgment;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Mutable, unsynchronized buffer of small Parquet temp files for one pipeline.
 * All access must be guarded by synchronized(this) in BatchManager.
 */
class PipelineBatch {

    private final String pipelineName;
    private final List<Path> files = new ArrayList<>();
    private final List<Acknowledgment> acks = new ArrayList<>();
    private long totalBytes = 0;

    PipelineBatch(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    void add(Path file, long bytes, Acknowledgment ack) {
        files.add(file);
        acks.add(ack);
        totalBytes += bytes;
    }

    boolean isEmpty() {
        return files.isEmpty();
    }

    String getPipelineName() {
        return pipelineName;
    }

    long getTotalBytes() {
        return totalBytes;
    }

    List<Path> drainFiles() {
        List<Path> snapshot = new ArrayList<>(files);
        files.clear();
        totalBytes = 0;
        return snapshot;
    }

    List<Acknowledgment> drainAcks() {
        List<Acknowledgment> snapshot = new ArrayList<>(acks);
        acks.clear();
        return snapshot;
    }
}
