package org.pangrova.indexing.storage;

import org.pangrova.connector.api.index.FileSystemAccessor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * {@link FileSystemAccessor} backed by the local file system ({@link java.nio.file.Files}).
 *
 * <p>Explicit local accessor for callers that own and authorize the supplied paths.
 * Catalog-backed indexing obtains accessors from the selected connector.</p>
 */
public final class LocalFileSystemAccessor implements FileSystemAccessor {

    @Override
    public InputStream openInputStream(String path) throws IOException {
        return Files.newInputStream(Path.of(path));
    }

    @Override
    public OutputStream openOutputStream(String path) throws IOException {
        Path target = Path.of(path);
        Path parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        return Files.newOutputStream(target);
    }

    @Override
    public void deleteFile(String path) throws IOException {
        Files.deleteIfExists(Path.of(path));
    }

    @Override
    public boolean exists(String path) throws IOException {
        return Files.exists(Path.of(path));
    }

    @Override
    public long fileSize(String path) throws IOException {
        return Files.size(Path.of(path));
    }

    @Override
    public List<String> listFiles(String directoryPath) throws IOException {
        try (Stream<Path> stream = Files.list(Path.of(directoryPath))) {
            return stream
                    .filter(Files::isRegularFile)
                    .map(Path::toString)
                    .toList();
        }
    }

    @Override
    public void close() {
        // no resources to release
    }
}
