package org.example.sink;

import org.apache.parquet.hadoop.ParquetWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.*;

public class AsyncAvroParquetWriter<T> implements Closeable {
    private final ParquetWriter<T> writer;
    private final BlockingQueue<T> queue = new LinkedBlockingQueue<>(30_000);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Future<?> writeTask;
    private volatile boolean running = true;

    public AsyncAvroParquetWriter(ParquetWriter<T> writer) {
        this.writer = writer;
        this.writeTask = executor.submit(() -> {
            try {
                while (running || !queue.isEmpty()) {
                    T record = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (record != null) {
                        writer.write(record);
                    }
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        });
    }

    public void write(T record) throws InterruptedException {
        queue.put(record);
    }

    @Override
    public void close() throws IOException {
        running = false;
        try {
            writeTask.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            executor.shutdown();
            writer.close();
        }
    }
}
