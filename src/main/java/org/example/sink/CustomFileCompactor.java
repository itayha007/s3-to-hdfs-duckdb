package org.example.sink;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.example.service.SchemaService;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.example.utils.DtUtils.extractPipelineFromPath;
import static org.example.utils.DtUtils.getDtFromEpoch;

public class CustomFileCompactor<IN> extends RecordWiseFileCompactor<IN> implements Serializable {

    private final Reader.Factory<IN> readerFactory;
    private final String baseOutputPath;
    private final Clock clock;
    private final SchemaService schemaService;

    public CustomFileCompactor(Reader.Factory<IN> readerFactory, String baseOutputPath,
                               Clock clock, SchemaService schemaService) {
        super(readerFactory);
        this.readerFactory = readerFactory;
        this.baseOutputPath = baseOutputPath;
        this.clock = clock;
        this.schemaService = schemaService;
    }

    @Override
    public void compact(List<Path> inputFiles, Writer<IN> ignoredWriter) throws Exception {
        Configuration hadoopConf = new Configuration();
        FileSystem fs = FileSystem.get(hadoopConf);
        Map<String, List<InputFile>> inputFilesByPipeline = this.getInputFilesByPipeline(inputFiles);

        try {
            for (String pipelineName : inputFilesByPipeline.keySet()) {
                String dt = getDtFromEpoch(Instant.now().toEpochMilli() + 300_000);
                FSDataOutputStream outputStream = fs.create(
                        new org.apache.hadoop.fs.Path(getOutputPath(dt, pipelineName, this.clock).toUri()));
                try (outputStream;
                     BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
                     AsyncAvroParquetWriter<IN> writer = new AsyncAvroParquetWriter<>(
                             AvroParquetWriter.<IN>builder(new ParquetBufferedWriter(bufferedOutputStream))
                                     .withSchema(this.schemaService.getFinalSchemaByPipelineName(pipelineName))
                                     .withCompressionCodec(CompressionCodecName.SNAPPY)
                                     .withConf(hadoopConf)
                                     .build())) {
                    for (InputFile inputFile : inputFilesByPipeline.get(pipelineName)) {
                        try (Reader<IN> reader = this.readerFactory.createFor(inputFile.getPath())) {
                            IN record;
                            while ((record = reader.read()) != null) {
                                writer.write(record);
                            }
                        }
                        inputFile.setProcessed(true);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            this.deleteTempFiles(inputFilesByPipeline);
        }
    }

    private void deleteTempFiles(Map<String, List<InputFile>> inputFilesByPipeline) throws Exception {
        for (String pipelineName : inputFilesByPipeline.keySet()) {
            for (InputFile inputFile : inputFilesByPipeline.get(pipelineName)) {
                if (inputFile.isProcessed()) {
                    try {
                        boolean deleted = inputFile.getPath().getFileSystem()
                                .delete(new Path(inputFile.getPath().toUri()), false);
                        if (!deleted) {
                            System.out.println("failed to delete " + inputFile.getPath());
                        }
                    } catch (IOException e) {
                        System.out.println("failed to delete " + inputFile.getPath());
                    }
                }
            }
        }
    }

    private Path getOutputPath(String dt, String pipelineName, Clock clock) {
        Path dtPath = new Path(new Path(this.baseOutputPath, pipelineName), "dt=".concat(dt));
        return new Path(dtPath, String.format("%s_%s_%s.parq.snappy",
                "flink",
                clock.instant().atZone(ZoneId.of("Asia/Jerusalem"))
                        .format(DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")),
                UUID.randomUUID()));
    }

    private Map<String, List<InputFile>> getInputFilesByPipeline(List<Path> inputFiles) throws IOException {
        return inputFiles.stream()
                .map(InputFile::new)
                .collect(Collectors.groupingBy(
                        inputFile -> extractPipelineFromPath(inputFile.getPath().getPath())
                ));
    }

    @Data
    @RequiredArgsConstructor
    public static class InputFile implements Serializable {
        private final Path path;
        private boolean isProcessed;
    }
}
