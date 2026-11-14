package org.pangrova.connector.iceberg;

import org.pangrova.common.connector.SnapshotPinTags;

import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.pangrova.connector.api.index.SnapshotExpiredException;
import org.pangrova.connector.spi.index.SnapshotPinProvider;

import java.util.List;
import java.util.Objects;

/** Persists one index's snapshot hold as an Iceberg tag in the source table metadata. */
final class IcebergSnapshotPinProvider implements SnapshotPinProvider {
    private final Catalog catalog;

    IcebergSnapshotPinProvider(Catalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    @Override
    public void pinSnapshot(String catalogName, String schemaName, String tableName,
                            long snapshotId, String pinnedByIndexId) {
        Table table = load(schemaName, tableName);
        String tag = SnapshotPinTags.forSnapshot(pinnedByIndexId, snapshotId);
        SnapshotRef existing = table.refs().get(tag);
        if (existing != null) {
            requireExactTag(existing, snapshotId, tag);
            return;
        }
        if (table.snapshot(snapshotId) == null) {
            throw new SnapshotExpiredException(Long.toString(snapshotId),
                    "Cannot pin expired Iceberg snapshot " + snapshotId);
        }
        table.manageSnapshots().createTag(tag, snapshotId)
                .setMaxRefAgeMs(tag, Long.MAX_VALUE).commit();
    }

    @Override
    public void unpinSnapshot(String catalogName, String schemaName, String tableName,
                              long snapshotId, String pinnedByIndexId) {
        Table table = load(schemaName, tableName);
        String tag = SnapshotPinTags.forSnapshot(pinnedByIndexId, snapshotId);
        SnapshotRef existing = table.refs().get(tag);
        if (existing != null) {
            requireExactTag(existing, snapshotId, tag);
            table.manageSnapshots().removeTag(tag).commit();
            table.refresh();
        }
        // Ref names created before per-snapshot pins are still released on DROP.
        String legacyTag = SnapshotPinTags.indexPrefix(pinnedByIndexId);
        SnapshotRef legacy = table.refs().get(legacyTag);
        if (legacy != null && legacy.snapshotId() == snapshotId) {
            requireExactTag(legacy, snapshotId, legacyTag);
            table.manageSnapshots().removeTag(legacyTag).commit();
        }
    }

    @Override
    public List<Long> getPinnedSnapshots(String catalogName, String schemaName, String tableName) {
        return load(schemaName, tableName).refs().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(SnapshotPinTags.PREFIX) && entry.getValue().isTag())
                .map(entry -> entry.getValue().snapshotId()).distinct().toList();
    }

    @Override
    public List<Long> getPinnedSnapshotsForIndex(
            String catalogName, String schemaName, String tableName, String indexId) {
        String prefix = SnapshotPinTags.indexPrefix(indexId);
        return load(schemaName, tableName).refs().entrySet().stream()
                .filter(entry -> entry.getKey().equals(prefix)
                        || entry.getKey().startsWith(prefix + "_"))
                .map(entry -> checkedSnapshot(entry.getKey(), entry.getValue(), prefix))
                .distinct().toList();
    }

    private static long checkedSnapshot(String tag, SnapshotRef ref, String prefix) {
        if (!ref.isTag()) {
            throw new IllegalStateException("Index snapshot ref is not a tag: " + tag);
        }
        if (!tag.equals(prefix)) {
            String suffix = tag.substring(prefix.length() + 1);
            if (!Long.toString(ref.snapshotId()).equals(suffix)) {
                throw new IllegalStateException("Index snapshot tag differs from its snapshot: " + tag);
            }
        }
        return ref.snapshotId();
    }

    private Table load(String schemaName, String tableName) {
        Table table = catalog.loadTable(TableIdentifier.of(schemaName, tableName));
        table.refresh();
        return table;
    }

    private static void requireExactTag(SnapshotRef ref, long snapshotId, String tag) {
        if (!ref.isTag() || ref.snapshotId() != snapshotId) {
            throw new IllegalStateException("Iceberg index pin " + tag + " differs from its snapshot");
        }
    }
}
