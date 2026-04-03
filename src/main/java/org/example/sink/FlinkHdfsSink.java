package org.example.sink;

import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.InputFormatBasedReader;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.function.SerializableSupplierWithException;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.example.config.HdfsConfig;
import org.example.service.SchemaService;
import org.springframework.stereotype.Component;
import org.springframework.util.unit.DataSize;

import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;

import static org.example.utils.DtUtils.extractPipelineName;

@Component
@RequiredArgsConstructor
public class FlinkHdfsSink {

    private static final Clock CLOCK = Clock.system(ZoneId.of("Asia/Jerusalem"));

    private final HdfsConfig hdfsConfig;

    public FileSink<GenericRecord> build(SchemaService schemaService) {
        return FileSink
                .forBulkFormat(new Path(hdfsConfig.getFlinkOutputPath()),
                        createParquetWriterFactory(schemaService))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new DtBucketAssigner())
                .enableCompact(createCompactStrategy(), createCompactor(schemaService))
                .build();
    }

    private FileCompactStrategy createCompactStrategy() {
        return FileCompactStrategy.Builder.newBuilder()
                .setSizeThreshold(DataSize.ofMegabytes(hdfsConfig.getSizeThresholdMb()).toBytes())
                .enableCompactionOnCheckpoint(hdfsConfig.getCompactOnCheckpoint())
                .build();
    }

    private RecordWiseFileCompactor<GenericRecord> createCompactor(SchemaService schemaService) {
        return new CustomFileCompactor<>(
                new InputFormatBasedReader.Factory<>(
                        (SerializableSupplierWithException<FileInputFormat<GenericRecord>, IOException>)
                                ParquetFileInputFormat::new
                ),
                hdfsConfig.getFlinkOutputPath(),
                CLOCK,
                schemaService
        );
    }

    private static ParquetWriterFactory<GenericRecord> createParquetWriterFactory(SchemaService schemaService) {
        return new ParquetWriterFactory<>((out) -> createParquetWriter(schemaService, GenericData.get(), out));
    }

    private static ParquetWriter<GenericRecord> createParquetWriter(SchemaService schemaService,
                                                                     GenericData genericData,
                                                                     OutputFile outputFile) {
        try {
            String pipelineName = extractPipelineName(outputFile);
            return AvroParquetWriter.<GenericRecord>builder(outputFile)
                    .withSchema(schemaService.getFinalSchemaByPipelineName(pipelineName))
                    .withDataModel(genericData)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
