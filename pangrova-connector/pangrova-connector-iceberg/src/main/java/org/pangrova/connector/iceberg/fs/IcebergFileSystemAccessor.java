package org.pangrova.connector.iceberg.fs;

import org.pangrova.connector.api.index.FileSystemAccessor;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Adapts Iceberg {@link FileIO} to the Pangrova {@link FileSystemAccessor} SPI.
 *
 * <p>Provides byte-level I/O for index file storage using the same
 * file system backend as the Iceberg catalog (HDFS, S3, local FS).</p>
 */
public final class IcebergFileSystemAccessor implements FileSystemAccessor {

    private final FileIO fileIO;

    public IcebergFileSystemAccessor(FileIO fileIO) {
        if (fileIO == null) {
            throw new IllegalArgumentException("fileIO must not be null");
        }
        this.fileIO = fileIO;
    }

    @Override
    public InputStream openInputStream(String path) throws IOException {
        InputFile inputFile = fileIO.newInputFile(path);
        return inputFile.newStream();
    }

    @Override
    public OutputStream openOutputStream(String path) throws IOException {
        OutputFile outputFile = fileIO.newOutputFile(path);
        return outputFile.createOrOverwrite();
    }

    @Override
    public void deleteFile(String path) throws IOException {
        fileIO.deleteFile(path);
    }

    @Override
    public boolean exists(String path) throws IOException {
        InputFile inputFile = fileIO.newInputFile(path);
        return inputFile.exists();
    }

    @Override
    public long fileSize(String path) throws IOException {
        InputFile inputFile = fileIO.newInputFile(path);
        return inputFile.getLength();
    }

    @Override
    public List<String> listFiles(String directoryPath) throws IOException {
        // Iceberg FileIO does not provide directory listing natively.
        // This is a limitation — callers should use manifest-based file discovery.
        return List.of();
    }

    @Override
    public void close() {
        fileIO.close();
    }
}
