package org.example.service;

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
    private long totalBytes = 0;

    PipelineBatch(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    void add(Path file, long bytes) {
        files.add(file);
        totalBytes += bytes;
    }

    boolean isEmpty() {
        return files.isEmpty();
    }

    String getPipelineName() {
        return pipelineName;
    }

    /** Returns a snapshot of the current file list. */
    List<Path> drainFiles() {
        List<Path> snapshot = new ArrayList<>(files);
        files.clear();
        totalBytes = 0;
        return snapshot;
    }

    long getTotalBytes() {
        return totalBytes;
    }
}
