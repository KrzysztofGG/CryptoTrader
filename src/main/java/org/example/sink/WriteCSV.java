package org.example.sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;

public class WriteCSV {

    private final Path file;

    WriteCSV(String filename, List<String> headers) throws IOException {
        this.file = Path.of(filename);
        writeHeaderIfNotExists(headers);
    }

    private void writeHeaderIfNotExists(List<String> headers) throws IOException {
        Files.createDirectories(file.getParent());

        if (Files.notExists(file)) {
            String headerLine = String.join(",", headers) + System.lineSeparator();
            Files.writeString(
                    file,
                    headerLine,
                    StandardOpenOption.CREATE
            );
        }
    }

    public void writeRow(List<?> row) throws IOException {
        String line = row.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(","))
                 + System.lineSeparator();

        Files.writeString(file, line, StandardOpenOption.APPEND);
    }

}

