package org.example.sink;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;

public class ParquetFileInputFormat extends FileInputFormat<GenericRecord> {

    private ParquetReader<GenericRecord> parquetReader;
    private GenericRecord readRecord;

    @Override
    public void open(FileInputSplit split) throws IOException {
        super.open(split);
        org.apache.hadoop.fs.Path hadoopPath =
                new org.apache.hadoop.fs.Path(split.getPath().toString());
        this.parquetReader = AvroParquetReader.<GenericRecord>builder(hadoopPath).build();
        this.readRecord = this.parquetReader.read();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return this.readRecord == null;
    }

    @Override
    public GenericRecord nextRecord(GenericRecord genericRecord) throws IOException {
        GenericRecord record = this.readRecord;
        this.readRecord = this.parquetReader.read();
        return record;
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (this.parquetReader != null) {
            this.parquetReader.close();
            this.parquetReader = null;
        }
    }
}
