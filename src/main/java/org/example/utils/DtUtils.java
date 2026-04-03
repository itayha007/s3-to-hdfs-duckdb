package org.example.utils;

import org.apache.parquet.io.OutputFile;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DtUtils {

    public static String getDtFromEpoch(long epoch) {
        Instant time = Instant.ofEpochMilli(epoch);
        ZonedDateTime zdt = time.atZone(ZoneId.of("Asia/Jerusalem"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHH");
        return zdt.format(formatter);
    }

    public static Long getCurrentEpoch() {
        return ZonedDateTime.now(ZoneId.of("Asia/Jerusalem"))
                .toInstant()
                .toEpochMilli();
    }

    public static String extractPipelineName(OutputFile outputFile) {
        String path = extractTargetFilePath(outputFile);
        return extractPipelineFromPath(path);
    }

    public static String extractPipelineFromPath(String path) {
        path = path.replace('\\', '/');
        String[] parts = path.split("/");
        if (parts.length < 4) {
            throw new IllegalArgumentException("Path too short: " + path);
        }
        return parts[parts.length - 3];
    }

    public static String extractTargetFilePath(OutputFile of) {
        try {
            Field streamField = of.getClass().getDeclaredField("stream");
            streamField.setAccessible(true);
            Object privateStream = streamField.get(of);
            Field targetFileField = privateStream.getClass().getDeclaredField("targetFile");
            targetFileField.setAccessible(true);
            Object targetFile = targetFileField.get(privateStream);
            if (targetFile == null) return null;
            return targetFile.toString();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            return null;
        }
    }
}
