package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.handle.PaimonTableIdentity;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.pangrova.spi.connector.index.SnapshotExpiredException;
import org.pangrova.spi.connector.index.SnapshotPinProvider;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Persists each index snapshot hold as a Paimon tag without an expiry time. */
final class PaimonSnapshotPinProvider implements SnapshotPinProvider {
    private static final String TAG_PREFIX = "pangrova_index_";
    private final Catalog catalog;

    PaimonSnapshotPinProvider(Catalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    @Override
    public void pinSnapshot(String catalogName, String schemaName, String tableName,
                            long snapshotId, String pinnedByIndexId) {
        FileStoreTable table = load(schemaName, tableName);
        String tag = tagName(pinnedByIndexId, snapshotId);
        Long existing = taggedSnapshot(table, tag);
        if (existing != null) {
            requireExact(existing, snapshotId, tag);
            return;
        }
        if (!table.snapshotManager().snapshotExists(snapshotId)) {
            throw new SnapshotExpiredException(Long.toString(snapshotId),
                    "Cannot pin expired Paimon snapshot " + snapshotId);
        }
        table.createTag(tag, snapshotId, null);
        requireExact(taggedSnapshot(load(schemaName, tableName), tag), snapshotId, tag);
    }

    @Override
    public void unpinSnapshot(String catalogName, String schemaName, String tableName,
                              long snapshotId, String pinnedByIndexId) {
        FileStoreTable table = load(schemaName, tableName);
        String tag = tagName(pinnedByIndexId, snapshotId);
        Long existing = taggedSnapshot(table, tag);
        if (existing != null) {
            requireExact(existing, snapshotId, tag);
            table.deleteTag(tag);
        }
    }

    @Override
    public List<Long> getPinnedSnapshots(String catalogName, String schemaName, String tableName) {
        return load(schemaName, tableName).tagManager().tags().entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(name -> name.startsWith(TAG_PREFIX)))
                .map(entry -> entry.getKey().id()).distinct().toList();
    }

    @Override
    public List<Long> getPinnedSnapshotsForIndex(
            String catalogName, String schemaName, String tableName, String indexId) {
        String prefix = tagPrefix(indexId) + "_";
        return load(schemaName, tableName).tagManager().tags().entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(name -> {
                    if (!name.startsWith(prefix)) {
                        return false;
                    }
                    requireExact(entry.getKey().id(),
                            parseSnapshotSuffix(name.substring(prefix.length())), name);
                    return true;
                }))
                .map(entry -> entry.getKey().id()).distinct().toList();
    }

    private FileStoreTable load(String schemaName, String tableName) {
        try {
            var table = PaimonTableIdentity.loadFresh(catalog, Identifier.create(schemaName, tableName));
            if (table instanceof FileStoreTable fileStore) {
                return fileStore;
            }
            throw new IllegalStateException("Snapshot pins require a Paimon file-store table");
        } catch (Catalog.TableNotExistException missing) {
            throw new IllegalStateException("Paimon table no longer exists for snapshot pinning", missing);
        }
    }

    private static Long taggedSnapshot(FileStoreTable table, String tagName) {
        Long result = null;
        for (var entry : table.tagManager().tags().entrySet()) {
            if (entry.getValue().contains(tagName)) {
                if (result != null) {
                    throw new IllegalStateException("Paimon tag resolves to multiple snapshots: " + tagName);
                }
                result = entry.getKey().id();
            }
        }
        return result;
    }

    private static long parseSnapshotSuffix(String suffix) {
        try {
            return Long.parseLong(suffix);
        } catch (NumberFormatException invalid) {
            throw new IllegalStateException("Invalid Paimon index pin snapshot: " + suffix, invalid);
        }
    }

    private static void requireExact(Long actual, long expected, String tag) {
        if (actual == null || actual != expected) {
            throw new IllegalStateException("Paimon index pin " + tag + " differs from its snapshot");
        }
    }

    private static String tagName(String indexId, long snapshotId) {
        return tagPrefix(indexId) + "_" + snapshotId;
    }

    private static String tagPrefix(String indexId) {
        if (indexId == null || indexId.isBlank()) {
            throw new IllegalArgumentException("Index ID is required for snapshot pinning");
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(indexId.getBytes(StandardCharsets.UTF_8));
            return TAG_PREFIX + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 unavailable", failure);
        }
    }
}
