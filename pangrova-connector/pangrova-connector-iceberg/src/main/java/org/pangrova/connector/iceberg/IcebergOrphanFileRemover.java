package org.pangrova.connector.iceberg;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Sweeps an Iceberg table location for orphan files — files that are not
 * reachable from any snapshot or table metadata and are older than a cutoff —
 * and deletes them via a prefix-capable {@link FileIO}.
 */
final class IcebergOrphanFileRemover {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergOrphanFileRemover.class);

    private IcebergOrphanFileRemover() {}

    /** Aggregated outcome of an orphan-file sweep. */
    record SweepResult(long scannedCount, long deletedCount, long deletedBytes,
                       List<String> deletedOrphanPaths) {}

    /**
     * Scans the table location and deletes unreachable files older than the cutoff.
     *
     * @param table        the table whose location is swept
     * @param prefixIO     prefix-capable FileIO used for listing and deletion
     * @param olderThanMs  files created at/after this timestamp are kept
     * @param tableName    table name used for failure logging
     * @return the sweep outcome
     */
    static SweepResult sweep(Table table, SupportsPrefixOperations prefixIO,
                             long olderThanMs, String tableName) {
        Set<String> reachable = collectReachableFiles(table);
        long deletedCount = 0;
        long deletedBytes = 0;
        long scannedCount = 0;
        List<String> deletedOrphanPaths = new ArrayList<>();
        for (FileInfo fileInfo : prefixIO.listPrefix(table.location())) {
            scannedCount++;
            if (fileInfo.createdAtMillis() >= olderThanMs) {
                continue;
            }
            String location = fileInfo.location();
            if (reachable.contains(location) || reachable.contains(canonicalize(location))) {
                continue;
            }
            try {
                prefixIO.deleteFile(location);
                deletedCount++;
                deletedBytes += fileInfo.size();
                deletedOrphanPaths.add(location);
            } catch (RuntimeException e) {
                LOG.warn("event=remove_orphan_file_failed table={} path={} error={}",
                        tableName, location, e.getMessage());
            }
        }
        return new SweepResult(scannedCount, deletedCount, deletedBytes, deletedOrphanPaths);
    }

    /**
     * Collects all files that are still reachable from any snapshot or table metadata.
     * Anything outside this set under the table location and older than the cutoff
     * is considered orphaned.
     */
    private static Set<String> collectReachableFiles(Table table) {
        Set<String> reachable = new HashSet<>();

        // Metadata JSON files + version-hint
        reachable.addAll(ReachableFileUtil.metadataFileLocations(table, true));
        String versionHint = ReachableFileUtil.versionHintLocation(table);
        if (versionHint != null) {
            reachable.add(versionHint);
        }

        // Manifest list files for all snapshots
        reachable.addAll(ReachableFileUtil.manifestListLocations(table));

        // Statistics files (Puffin, etc.)
        reachable.addAll(ReachableFileUtil.statisticsFilesLocations(table));

        // Manifest files + every data/delete file path referenced by them.
        // ManifestFiles.readPaths works for both DATA and DELETES manifests.
        FileIO io = table.io();
        for (Snapshot snapshot : table.snapshots()) {
            for (ManifestFile manifest : snapshot.allManifests(io)) {
                reachable.add(manifest.path());
                addManifestEntryPaths(table, manifest, reachable);
            }
        }

        // Also normalize forms (with/without scheme) for matching
        Set<String> normalized = new HashSet<>();
        for (String path : reachable) {
            normalized.add(canonicalize(path));
        }
        reachable.addAll(normalized);
        return reachable;
    }

    private static void addManifestEntryPaths(
            Table table, ManifestFile manifest, Set<String> reachable) {
        try (org.apache.iceberg.io.CloseableIterable<String> paths =
                org.apache.iceberg.ManifestFiles.readPaths(
                        manifest, table.io(), table.specs())) {
            for (String path : paths) {
                reachable.add(path);
            }
        } catch (Exception e) {
            LOG.warn("event=walk_manifest_failed manifest={} error={}",
                    manifest.path(), e.getMessage());
        }
    }

    /**
     * Strips a URI scheme prefix (e.g. {@code file:}, {@code s3:}, {@code hdfs:}) so
     * paths reported by {@code listPrefix} (which may omit the scheme) compare equal
     * to paths recorded in manifests (which usually include it).
     */
    private static String canonicalize(String path) {
        if (path == null) {
            return null;
        }
        int schemeIdx = path.indexOf("://");
        if (schemeIdx > 0) {
            return path.substring(schemeIdx + 2); // keep //...
        }
        if (path.startsWith("file:")) {
            return path.substring("file:".length());
        }
        return path;
    }
}
