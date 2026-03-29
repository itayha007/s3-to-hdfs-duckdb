package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.example.config.BatchConfig;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Buffers small Parquet files per pipeline and flushes them as one merged
 * Parquet to HDFS when either threshold is reached:
 *
 *   - Size:  accumulated Parquet bytes >= batch.max-parquet-bytes (default 128 MB)
 *   - Time:  scheduled flush every batch.flush-interval-ms (default 5 minutes)
 *
 * DuckDB merges the buffered files via read_parquet([...]) in one pass.
 */
@Log4j2
@Service
@RequiredArgsConstructor
public class BatchManager {

    private final BatchConfig batchConfig;
    private final DuckDbService duckDbService;
    private final HdfsWriterService hdfsWriterService;

    private final ConcurrentHashMap<String, PipelineBatch> batches = new ConcurrentHashMap<>();

    /**
     * Adds a small Parquet temp file to the pipeline's batch.
     * BatchManager owns the file after this call — do not delete it externally.
     * Flushes immediately if the Parquet size threshold is exceeded.
     */
    public void add(String pipelineName, Path parquetFile) throws Exception {
        long size = Files.size(parquetFile);
        PipelineBatch batch = batches.computeIfAbsent(pipelineName, PipelineBatch::new);

        boolean shouldFlush;
        synchronized (batch) {
            batch.add(parquetFile, size);
            shouldFlush = batch.getTotalBytes() >= batchConfig.getMaxParquetBytes();
        }

        if (shouldFlush) {
            log.info("Size threshold reached for pipeline '{}' ({} MB), flushing",
                    pipelineName, batchConfig.getMaxParquetBytes() / (1024 * 1024));
            flushPipeline(pipelineName);
        }
    }

    @Scheduled(fixedDelayString = "${batch.flush-interval-ms:300000}")
    public void flushAll() {
        long nonEmpty = batches.values().stream().filter(b -> !b.isEmpty()).count();
        if (nonEmpty == 0) return;
        log.info("Scheduled flush: {} pipeline(s) with pending data", nonEmpty);
        batches.keySet().forEach(this::flushPipeline);
    }

    private void flushPipeline(String pipelineName) {
        PipelineBatch batch = batches.get(pipelineName);
        if (batch == null) return;

        List<Path> files;
        synchronized (batch) {
            if (batch.isEmpty()) return;
            files = batch.drainFiles();
        }

        long totalMb = files.stream().mapToLong(f -> {
            try { return Files.size(f); } catch (Exception e) { return 0L; }
        }).sum() / (1024 * 1024);
        log.info("Flushing pipeline '{}': {} Parquet file(s), ~{} MB total",
                pipelineName, files.size(), totalMb);

        Path mergedParquet = null;
        try {
            mergedParquet = duckDbService.mergeParquets(files);
            hdfsWriterService.writeToHdfs(mergedParquet, pipelineName);
        } catch (Exception e) {
            log.error("Failed to flush pipeline '{}'", pipelineName, e);
        } finally {
            files.forEach(this::deleteSilently);
            if (mergedParquet != null) deleteSilently(mergedParquet);
        }
    }

    private void deleteSilently(Path path) {
        try { Files.deleteIfExists(path); } catch (Exception e) {
            log.warn("Failed to delete {}: {}", path, e.getMessage());
        }
    }
}
