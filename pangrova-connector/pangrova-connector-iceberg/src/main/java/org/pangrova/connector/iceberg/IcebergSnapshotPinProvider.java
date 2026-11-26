package org.pangrova.connector.iceberg;

import org.pangrova.common.connector.SnapshotPinTags;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotPinReference;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.pangrova.connector.api.exception.SnapshotExpiredException;

import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Persists one index attempt's snapshot hold as an Iceberg tag in source table metadata. */
final class IcebergSnapshotPinProvider implements SnapshotPinProvider {
    private final Catalog catalog;

    IcebergSnapshotPinProvider(Catalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    @Override
    public void pinSnapshot(String catalogName, String schemaName, String tableName,
                            String snapshotToken, SnapshotPinOwner owner) {
        Table table = load(schemaName, tableName);
        long snapshotId = snapshotId(snapshotToken);
        String tag = tag(owner, snapshotToken);
        if (!exactTags(table, owner, snapshotToken, snapshotId).isEmpty()) {
            return;
        }
        if (table.snapshot(snapshotId) == null) {
            throw new SnapshotExpiredException(snapshotToken,
                    "Cannot pin expired Iceberg snapshot " + snapshotToken);
        }
        table.manageSnapshots().createTag(tag, snapshotId)
                .setMaxRefAgeMs(tag, Long.MAX_VALUE).commit();
    }

    @Override
    public void unpinSnapshot(String catalogName, String schemaName, String tableName,
                              String snapshotToken, SnapshotPinOwner owner) {
        Table table = load(schemaName, tableName);
        long snapshotId = snapshotId(snapshotToken);
        List<String> exact = exactTags(table, owner, snapshotToken, snapshotId);
        if (!exact.isEmpty()) {
            var update = table.manageSnapshots();
            exact.forEach(update::removeTag);
            update.commit();
            table.refresh();
        }
        if (!isLegacyOwner(owner)) {
            return;
        }
        String legacyTag = SnapshotPinTags.indexPrefix(owner.indexId());
        SnapshotRef legacy = table.refs().get(legacyTag);
        if (legacy != null && legacy.snapshotId() == snapshotId) {
            requireExactTag(legacy, snapshotId, legacyTag);
            table.manageSnapshots().removeTag(legacyTag).commit();
        }
        String legacySnapshotTag = SnapshotPinTags.indexPrefix(owner.indexId()) + "_" + snapshotToken;
        SnapshotRef legacySnapshot = table.refs().get(legacySnapshotTag);
        if (legacySnapshot != null && legacySnapshot.snapshotId() == snapshotId) {
            requireExactTag(legacySnapshot, snapshotId, legacySnapshotTag);
            table.manageSnapshots().removeTag(legacySnapshotTag).commit();
        }
    }

    @Override
    public void validateSnapshotPin(String catalogName, String schemaName, String tableName,
                                    String snapshotToken, SnapshotPinOwner owner) {
        Table table = load(schemaName, tableName);
        long snapshotId = snapshotId(snapshotToken);
        if (exactTags(table, owner, snapshotToken, snapshotId).isEmpty()) {
            throw new SnapshotExpiredException(snapshotToken,
                    "Iceberg index snapshot pin is absent for " + owner.id());
        }
        if (table.snapshot(snapshotId) == null) {
            throw new SnapshotExpiredException(snapshotToken,
                    "Pinned Iceberg snapshot is no longer readable: " + snapshotToken);
        }
    }

    @Override
    public List<String> getPinnedSnapshots(String catalogName, String schemaName, String tableName) {
        return snapshotTokens(schemaName, tableName).stream().distinct().toList();
    }

    @Override
    public List<SnapshotPinReference> getSnapshotPinsForIndex(
            String catalogName, String schemaName, String tableName, String indexId) {
        Table table = load(schemaName, tableName);
        List<SnapshotPinReference> result = new ArrayList<>();
        String prefix = SnapshotPinTags.indexPrefix(indexId);
        for (var entry : table.refs().entrySet()) {
            String tag = entry.getKey();
            if (!tag.startsWith(prefix) || !entry.getValue().isTag()) {
                continue;
            }
            SnapshotPinTags.DecodedSnapshotPin decoded = SnapshotPinTags.decode(tag, indexId);
            if (decoded != null) {
                requireExactTag(entry.getValue(), snapshotId(decoded.snapshotToken()), tag);
                result.add(new SnapshotPinReference(decoded.snapshotToken(),
                        new SnapshotPinOwner(indexId, decoded.generation(), decoded.attemptId())));
                continue;
            }
            String legacy = SnapshotPinTags.legacySnapshotToken(tag, indexId);
            if (legacy != null) {
                requireExactTag(entry.getValue(), snapshotId(legacy), tag);
                result.add(new SnapshotPinReference(legacy,
                        new SnapshotPinOwner(indexId, 0, "legacy")));
            } else if (tag.equals(prefix)) {
                result.add(new SnapshotPinReference(Long.toString(entry.getValue().snapshotId()),
                        new SnapshotPinOwner(indexId, 0, "legacy")));
            }
        }
        return result.stream().distinct().toList();
    }

    private List<String> snapshotTokens(String schemaName, String tableName) {
        Table table = load(schemaName, tableName);
        List<String> result = new ArrayList<>();
        for (var entry : table.refs().entrySet()) {
            String tag = entry.getKey();
            if (!tag.startsWith(SnapshotPinTags.PREFIX) || !entry.getValue().isTag()) {
                continue;
            }
            SnapshotPinTags.DecodedSnapshotPin decoded = SnapshotPinTags.decodeAny(tag);
            if (decoded != null) {
                requireExactTag(entry.getValue(), snapshotId(decoded.snapshotToken()), tag);
                result.add(decoded.snapshotToken());
                continue;
            }
            String suffix = tag.substring(SnapshotPinTags.PREFIX.length());
            int separator = suffix.indexOf('_');
            String token = separator < 0 ? Long.toString(entry.getValue().snapshotId())
                    : suffix.substring(separator + 1);
            if (token.matches("0|[1-9][0-9]*")) {
                result.add(token);
            }
        }
        return List.copyOf(result);
    }

    private Table load(String schemaName, String tableName) {
        Table table = catalog.loadTable(TableIdentifier.of(schemaName, tableName));
        table.refresh();
        return table;
    }

    private static List<String> exactTags(Table table, SnapshotPinOwner owner, String token, long snapshotId) {
        return SnapshotPinTags.matchingNames(owner.indexId(), owner.generation(), owner.attemptId(), token, tag -> {
            SnapshotRef ref = table.refs().get(tag);
            if (ref == null) {
                return false;
            }
            requireExactTag(ref, snapshotId, tag);
            return true;
        });
    }

    private static boolean isLegacyOwner(SnapshotPinOwner owner) {
        return owner.generation() == 0 && owner.attemptId().equals("legacy");
    }

    private static String tag(SnapshotPinOwner owner, String snapshotToken) {
        return SnapshotPinTags.forSnapshot(owner.indexId(), owner.generation(), owner.attemptId(), snapshotToken);
    }

    private static long snapshotId(String snapshotToken) {
        try {
            long id = Long.parseLong(snapshotToken);
            if (id <= 0 || !Long.toString(id).equals(snapshotToken)) {
                throw new NumberFormatException("non-canonical Iceberg snapshot");
            }
            return id;
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("Invalid Iceberg snapshot token", invalid);
        }
    }

    private static void requireExactTag(SnapshotRef ref, long snapshotId, String tag) {
        if (!ref.isTag() || ref.snapshotId() != snapshotId) {
            throw new IllegalStateException("Iceberg index pin " + tag + " differs from its snapshot");
        }
    }
}
