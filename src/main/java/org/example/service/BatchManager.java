package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.example.config.BatchConfig;
import org.example.service.PipelineBatch.BatchEntry;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Log4j2
@Service
@RequiredArgsConstructor
public class BatchManager {

    private final BatchConfig batchConfig;
    private final DuckDbService duckDbService;
    private final HdfsWriterService hdfsWriterService;

    private final ConcurrentHashMap<String, PipelineBatch> batches = new ConcurrentHashMap<>();

    public void add(String pipelineName, Path parquetFile, Acknowledgment acknowledgment) throws Exception {
        long size = Files.size(parquetFile);
        PipelineBatch batch = batches.computeIfAbsent(pipelineName, PipelineBatch::new);

        boolean shouldFlush;
        synchronized (batch) {
            batch.add(parquetFile, size, acknowledgment);
            shouldFlush = batch.getTotalBytes() >= batchConfig.getMaxParquetBytes();
        }

        if (shouldFlush) {
            log.info("Size threshold reached for pipeline '{}' ({} MB), flushing",
                    pipelineName, batchConfig.getMaxParquetBytes() / (1024 * 1024));
            flushPipeline(pipelineName);
        }
    }

    @Scheduled(fixedDelay = 10_000)
    public void flushAll() {
        long now = System.currentTimeMillis();
        List<String> due = batches.entrySet().stream()
                .filter(e -> !e.getValue().isEmpty())
                .filter(e -> now - e.getValue().getLastFlushedAt() >= batchConfig.getFlushIntervalMs())
                .map(java.util.Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toList());

        if (due.isEmpty()) return;
        log.info("Scheduled flush: {} pipeline(s) due", due.size());
        due.forEach(this::flushPipeline);
    }

    private void flushPipeline(String pipelineName) {
        PipelineBatch batch = batches.get(pipelineName);
        if (batch == null) return;

        List<BatchEntry> entries;
        synchronized (batch) {
            if (batch.isEmpty()) return;
            entries = batch.drain();
        }

        log.info("Flushing pipeline '{}': {} Parquet file(s), ~{} MB total",
                pipelineName, entries.size(),
                entries.stream().mapToLong(BatchEntry::getBytes).sum() / (1024 * 1024));

        Path mergedParquet = null;
        try {
            List<Path> files = entries.stream()
                    .map(BatchEntry::getPath)
                    .collect(java.util.stream.Collectors.toList());
            mergedParquet = duckDbService.mergeParquets(files);
            hdfsWriterService.writeToHdfs(mergedParquet, pipelineName);
            entries.stream()
                    .map(BatchEntry::getAck)
                    .forEach(Acknowledgment::acknowledge);
        } catch (Exception e) {
            log.error("Failed to flush pipeline '{}' — {} message(s) will be redelivered on restart",
                    pipelineName, entries.size(), e);
        } finally {
            entries.stream().map(BatchEntry::getPath).forEach(this::deleteSilently);
            if (mergedParquet != null) deleteSilently(mergedParquet);
        }
    }

    private void deleteSilently(Path path) {
        try { Files.deleteIfExists(path); } catch (Exception e) {
            log.warn("Failed to delete {}: {}", path, e.getMessage());
        }
    }
}
