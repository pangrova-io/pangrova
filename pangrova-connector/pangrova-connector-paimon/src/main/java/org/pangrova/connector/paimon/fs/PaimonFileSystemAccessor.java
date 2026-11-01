package org.pangrova.connector.paimon.fs;

import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Adapts Paimon {@link FileIO} to the Pangrova {@link FileSystemAccessor} SPI.
 *
 * <p>Provides byte-level I/O for index file storage using the same
 * file system backend as the Paimon catalog.</p>
 */
public final class PaimonFileSystemAccessor implements FileSystemAccessor {

    private final FileIO fileIO;

    public PaimonFileSystemAccessor(FileIO fileIO) {
        if (fileIO == null) {
            throw new IllegalArgumentException("fileIO must not be null");
        }
        this.fileIO = fileIO;
    }

    @Override
    public InputStream openInputStream(String path) throws IOException {
        return fileIO.newInputStream(new Path(path));
    }

    @Override
    public OutputStream openOutputStream(String path) throws IOException {
        return fileIO.newOutputStream(new Path(path), true);
    }

    @Override
    public void deleteFile(String path) throws IOException {
        fileIO.deleteQuietly(new Path(path));
    }

    @Override
    public boolean exists(String path) throws IOException {
        return fileIO.exists(new Path(path));
    }

    @Override
    public long fileSize(String path) throws IOException {
        return fileIO.getFileSize(new Path(path));
    }

    @Override
    public List<String> listFiles(String directoryPath) throws IOException {
        org.apache.paimon.fs.FileStatus[] statuses = fileIO.listStatus(new Path(directoryPath));
        List<String> result = new ArrayList<>();
        if (statuses != null) {
            for (org.apache.paimon.fs.FileStatus status : statuses) {
                if (!status.isDir()) {
                    result.add(status.getPath().toString());
                }
            }
        }
        return result;
    }

    @Override
    public void close() {
        // FileIO lifecycle managed externally by Paimon catalog
    }
}
