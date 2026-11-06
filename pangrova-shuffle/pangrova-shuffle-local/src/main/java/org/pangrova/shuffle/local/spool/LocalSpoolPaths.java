package org.pangrova.shuffle.local.spool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Validated paths under one configured, process-private spool root. Directory
 * identity checks detect replacement between operations; they are not a substitute
 * for race-free directory-relative OS operations against an untrusted local writer.
 */
final class LocalSpoolPaths {
    private static final Pattern SAFE_QUERY_ID = Pattern.compile("[a-zA-Z0-9_\\-.]+");
    private final Path root;
    private final Object rootKey;

    LocalSpoolPaths(Path configuredRoot) throws IOException {
        Files.createDirectories(configuredRoot);
        root = configuredRoot.toRealPath();
        rootKey = directoryKey(root);
    }

    Path lockFile() throws IOException {
        requireRoot();
        return root.resolve(".owner.lock");
    }

    Segment segment(String query, int stage, int partition, int producer, long attempt,
                    boolean create) throws IOException {
        validateQueryId(query);
        if (stage < 0 || partition < 0 || producer < 0 || attempt < 0) {
            throw new IllegalArgumentException("Spool identities must be non-negative");
        }
        Path queryDirectory = root.resolve(query);
        Path stageDirectory = queryDirectory.resolve("s" + stage);
        Path partitionDirectory = stageDirectory.resolve("p" + partition);
        if (create) {
            requireRoot();
            ensureDirectory(queryDirectory);
            requireParents(queryDirectory);
            ensureDirectory(stageDirectory);
            requireParents(stageDirectory);
            ensureDirectory(partitionDirectory);
        }
        requireParents(partitionDirectory);
        return new Segment(partitionDirectory.resolve("m" + producer + "-a" + attempt + ".spool"),
                partitionDirectory, directoryKey(partitionDirectory));
    }

    Path temporary(Segment segment) throws IOException {
        requireCurrent(segment);
        // The temporary filename is private, not derived from a caller's identity.
        return Files.createTempFile(segment.directory(), "pangrova-batch-", ".tmp");
    }

    Path queryDirectory(String query) throws IOException {
        validateQueryId(query);
        requireRoot();
        return root.resolve(query);
    }

    void requireCurrent(Segment segment) throws IOException {
        requireParents(segment.directory());
        if (!segment.directoryKey().equals(directoryKey(segment.directory()))) {
            throw new IOException("Spool partition directory was replaced");
        }
    }

    void requireParents(Path directory) throws IOException {
        requireRoot();
        if (!directory.isAbsolute() || !directory.normalize().equals(directory) || !directory.startsWith(root)) {
            throw new IOException("Spool directory is outside its owned root");
        }
        Path current = root;
        for (Path component : root.relativize(directory)) {
            current = current.resolve(component);
            directoryKey(current);
        }
    }

    void requireRoot() throws IOException {
        if (!rootKey.equals(directoryKey(root))) {
            throw new IOException("Spool root directory was replaced");
        }
    }

    private static Object directoryKey(Path directory) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(
                directory, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isDirectory() || attributes.isSymbolicLink() || attributes.fileKey() == null) {
            throw new IOException("Spool requires a real directory with stable filesystem identity");
        }
        return attributes.fileKey();
    }

    private static void ensureDirectory(Path directory) throws IOException {
        if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) Files.createDirectory(directory);
        directoryKey(directory);
    }

    static void validateQueryId(String query) {
        if (query == null || !SAFE_QUERY_ID.matcher(query).matches()
                || query.contains("..") || query.equals(".")) {
            throw new IllegalArgumentException("Invalid spool query identity");
        }
    }

    record Segment(Path file, Path directory, Object directoryKey) {
        Segment {
            Objects.requireNonNull(file, "file");
            Objects.requireNonNull(directory, "directory");
            Objects.requireNonNull(directoryKey, "directoryKey");
            if (!directory.equals(file.getParent())) {
                throw new IllegalArgumentException("Spool segment must belong to its bound directory");
            }
        }
    }
}
