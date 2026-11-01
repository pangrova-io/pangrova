package org.pangrova.shuffle.storage.local;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.Pattern;

import org.pangrova.shuffle.api.capability.ShuffleDurability;
import org.pangrova.shuffle.api.spool.SpoolReader;
import org.pangrova.shuffle.api.spool.SpoolStore;
import org.pangrova.shuffle.api.spool.SpoolWriter;

/**
 * Local-filesystem implementation of {@link SpoolStore} (ADR-002 D6).
 *
 * <p>Stores committed shuffle segments under a configurable base directory with layout:
 * <pre>
 *   &lt;baseDir&gt;/&lt;queryId&gt;/s&lt;stageId&gt;/p&lt;partitionId&gt;/m&lt;mapId&gt;-a&lt;attemptId&gt;.arrow
 * </pre>
 *
 * <p>Durability is {@link ShuffleDurability#LOCAL_SPILL}: data survives task failure on the
 * same worker but is lost if the worker process crashes (no replication). This is the in-tree
 * FTE default that lets the scheduler retry failed downstream tasks by re-reading spooled
 * partitions rather than re-running upstream producers.
 */
public final class LocalFsSpoolStore implements SpoolStore {

    /**
     * Allowed characters in queryId: alphanumeric, underscore, hyphen, dot.
     * Rejects path separators and traversal sequences (../) to prevent
     * path traversal attacks where a rogue queryId could escape baseDir.
     */
    private static final Pattern SAFE_QUERY_ID = Pattern.compile("[a-zA-Z0-9_\\-.]+");

    private final Path baseDir;

    /**
     * Creates a local spool store rooted at the supplied directory.
     *
     * @param baseDir base directory for query spool data
     */
    public LocalFsSpoolStore(Path baseDir) {
        if (baseDir == null) {
            throw new IllegalArgumentException("baseDir must not be null");
        }
        this.baseDir = baseDir;
    }

    @Override
    public SpoolWriter open(String queryId, int stageId, int partitionId, int mapId, long attemptId) {
        validateQueryId(queryId);
        Path segmentPath = segmentPath(queryId, stageId, partitionId, mapId, attemptId);
        return new LocalFsSpoolWriter(segmentPath);
    }

    @Override
    public SpoolReader read(String queryId, int stageId, int partitionId,
                            int startMapIndex, int endMapIndex) {
        validateQueryId(queryId);
        Path partitionDir = partitionDir(queryId, stageId, partitionId);
        return new LocalFsSpoolReader(partitionDir, startMapIndex, endMapIndex);
    }

    @Override
    public void delete(String queryId) {
        validateQueryId(queryId);
        Path queryDir = baseDir.resolve(queryId);
        if (!Files.exists(queryDir)) {
            return;
        }
        try {
            Files.walkFileTree(queryDir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                        throws IOException {
                    Files.deleteIfExists(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to delete spool data for query " + queryId, e);
        }
    }

    @Override
    public ShuffleDurability durability() {
        return ShuffleDurability.LOCAL_SPILL;
    }

    @Override
    public void close() {
        // No long-lived resources to release; cleanup is per-query via delete()
    }

    private Path segmentPath(String queryId, int stageId, int partitionId,
                             int mapId, long attemptId) {
        return partitionDir(queryId, stageId, partitionId)
                .resolve("m" + mapId + "-a" + attemptId + ".arrow");
    }

    private Path partitionDir(String queryId, int stageId, int partitionId) {
        return baseDir.resolve(queryId)
                .resolve("s" + stageId)
                .resolve("p" + partitionId);
    }

    private static void validateQueryId(String queryId) {
        if (queryId == null
                || ".".equals(queryId)
                || "..".equals(queryId)
                || !SAFE_QUERY_ID.matcher(queryId).matches()) {
            throw new IllegalArgumentException(
                    "Invalid queryId (must match [a-zA-Z0-9_\\-.]+, not '.' or '..'): " + queryId);
        }
    }
}
