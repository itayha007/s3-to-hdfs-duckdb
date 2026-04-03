package org.example.sink;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;

@Data
@RequiredArgsConstructor
class ParquetBufferedWriter implements OutputFile, Closeable {

    private final BufferedOutputStream out;

    @Override
    public void close() throws IOException {
        this.out.close();
    }

    @Override
    public PositionOutputStream create(long l) throws IOException {
        return createPositionOutputStream();
    }

    private PositionOutputStream createPositionOutputStream() {
        return new PositionOutputStream() {
            private long position = 0;

            @Override
            public void write(int b) throws IOException {
                out.write(b);
                position++;
            }

            @Override
            public long getPos() throws IOException {
                return this.position;
            }
        };
    }

    @Override
    public PositionOutputStream createOrOverwrite(long l) throws IOException {
        return createPositionOutputStream();
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }
}
