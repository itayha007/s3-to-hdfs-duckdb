package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.example.config.HdfsConfig;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Log4j2
@Service
@RequiredArgsConstructor
public class HdfsWriterService {

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd");

    private final FileSystem hdfsFileSystem;
    private final HdfsConfig hdfsConfig;

    /**
     * Copies a local Parquet file to HDFS.
     * Target path: {outputBasePath}/{pipelineName}/{yyyyMMdd}/{uuid}.parquet
     *
     * @return the HDFS path the file was written to
     */
    public String writeToHdfs(java.nio.file.Path localParquetFile,
                               String pipelineName) throws IOException {
        String date = LocalDate.now().format(DATE_FORMATTER);
        String fileName = UUID.randomUUID() + ".parquet";

        String hdfsPathStr = String.format("%s/%s/%s/%s",
                hdfsConfig.getOutputBasePath(), pipelineName, date, fileName);

        Path hdfsPath = new Path(hdfsPathStr);
        hdfsFileSystem.mkdirs(hdfsPath.getParent());

        log.info("Writing {} ({} bytes) to HDFS {}",
                localParquetFile, Files.size(localParquetFile), hdfsPath);

        hdfsFileSystem.copyFromLocalFile(
                new Path(localParquetFile.toAbsolutePath().toString()),
                hdfsPath);

        log.info("HDFS write complete: {}", hdfsPath);
        return hdfsPathStr;
    }
}
