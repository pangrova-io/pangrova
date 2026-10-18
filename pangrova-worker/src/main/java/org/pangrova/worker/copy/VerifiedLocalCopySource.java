package org.pangrova.worker.copy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/** Task-owned immutable scan file, released only after its Native reader exits. */
public final class VerifiedLocalCopySource implements AutoCloseable {
    private final Path path;
    private final Path directory;

    VerifiedLocalCopySource(Path path, Path stagingRoot) {
        Path candidate = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        Path root = Objects.requireNonNull(stagingRoot, "stagingRoot").toAbsolutePath().normalize();
        Path parent = candidate.getParent();
        Path directoryName = parent == null ? null : parent.getFileName();
        Path fileName = candidate.getFileName();
        if (parent == null || directoryName == null || fileName == null
                || !root.equals(parent.getParent())
                || !directoryName.toString().startsWith("copy-scan-")
                || !"source.parquet".equals(fileName.toString())) {
            throw new IllegalArgumentException("COPY scan file is outside its task-private staging directory");
        }
        this.path = candidate;
        this.directory = parent;
    }

    /** Local path produced from a verified managed object; never from a network split. */
    public Path path() {
        return path;
    }

    @Override public void close() throws IOException {
        Files.deleteIfExists(path);
        Files.deleteIfExists(directory);
    }
}
