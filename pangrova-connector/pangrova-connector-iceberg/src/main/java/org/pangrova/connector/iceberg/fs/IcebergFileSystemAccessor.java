package org.pangrova.connector.iceberg.fs;

import org.pangrova.connector.api.index.FileSystemAccessor;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.ArrayList;

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
        Path directory = new Path(directoryPath);
        List<String> files = new ArrayList<>();
        if (fileIO instanceof HadoopFileIO hadoop) {
            try (var fs = org.apache.hadoop.fs.FileSystem.newInstance(directory.toUri(), hadoop.getConf())) {
                for (var status : fs.listStatus(directory)) {
                    if (status.isFile()) files.add(status.getPath().toString());
                }
            }
        } else if (fileIO instanceof SupportsPrefixOperations prefixIO) {
            String prefix = directoryPath.endsWith("/") ? directoryPath : directoryPath + "/";
            for (var entry : prefixIO.listPrefix(prefix)) {
                String location = entry.location();
                if (!location.endsWith("/") && directory.equals(new Path(location).getParent())) {
                    files.add(location);
                }
            }
        } else {
            throw new IOException("Configured Iceberg FileIO does not support directory listing");
        }
        return List.copyOf(files);
    }

    @Override
    public void close() {
        // This accessor borrows the connector's FileIO. Each caller owns its streams.
    }
}
