package org.pangrova.connector.paimon;

import org.pangrova.common.connector.SnapshotPinTags;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotPinReference;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.pangrova.connector.api.exception.SnapshotExpiredException;
import org.pangrova.connector.paimon.handle.PaimonTableIdentity;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotNotExistException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Persists each index attempt's snapshot hold as a Paimon tag without an expiry time. */
final class PaimonSnapshotPinProvider implements SnapshotPinProvider {
    private final Catalog catalog;

    PaimonSnapshotPinProvider(Catalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    @Override
    public void pinSnapshot(String catalogName, String schemaName, String tableName,
                            String snapshotToken, SnapshotPinOwner owner) {
        FileStoreTable table = load(schemaName, tableName);
        long snapshotId = snapshotId(snapshotToken);
        String tag = tag(owner, snapshotToken);
        if (!exactTags(table, owner, snapshotToken, snapshotId).isEmpty()) {
            return;
        }
        try {
            // Paimon also resolves snapshots retained by a tag after numbered snapshot expiration.
            table.createTag(tag, snapshotId, null);
        } catch (SnapshotNotExistException expired) {
            throw new SnapshotExpiredException(snapshotToken,
                    "Cannot pin expired Paimon snapshot " + snapshotToken);
        }
        requireExact(taggedSnapshot(load(schemaName, tableName), tag), snapshotId, tag);
    }

    @Override
    public void unpinSnapshot(String catalogName, String schemaName, String tableName,
                              String snapshotToken, SnapshotPinOwner owner) {
        FileStoreTable table = load(schemaName, tableName);
        long snapshotId = snapshotId(snapshotToken);
        for (String tag : exactTags(table, owner, snapshotToken, snapshotId)) {
            table.deleteTag(tag);
        }
        if (!isLegacyOwner(owner)) {
            return;
        }
        String legacyPrefix = SnapshotPinTags.indexPrefix(owner.indexId());
        for (String legacy : List.of(legacyPrefix, legacyPrefix + "_" + snapshotToken)) {
            Long legacySnapshot = taggedSnapshot(table, legacy);
            if (legacySnapshot != null && legacySnapshot == snapshotId) {
                requireExact(legacySnapshot, snapshotId, legacy);
                table.deleteTag(legacy);
            }
        }
    }

    @Override
    public void validateSnapshotPin(String catalogName, String schemaName, String tableName,
                                    String snapshotToken, SnapshotPinOwner owner) {
        FileStoreTable table = load(schemaName, tableName);
        long snapshotId = snapshotId(snapshotToken);
        if (exactTags(table, owner, snapshotToken, snapshotId).isEmpty()) {
            throw new SnapshotExpiredException(snapshotToken,
                    "Paimon index snapshot pin is absent for " + owner.id());
        }
    }

    @Override
    public List<String> getPinnedSnapshots(String catalogName, String schemaName, String tableName) {
        return snapshotTokens(schemaName, tableName).stream().distinct().toList();
    }

    @Override
    public List<SnapshotPinReference> getSnapshotPinsForIndex(
            String catalogName, String schemaName, String tableName, String indexId) {
        FileStoreTable table = load(schemaName, tableName);
        List<SnapshotPinReference> result = new ArrayList<>();
        String prefix = SnapshotPinTags.indexPrefix(indexId);
        for (var entry : table.tagManager().tags().entrySet()) {
            for (String tag : entry.getValue()) {
                if (!tag.startsWith(prefix)) {
                    continue;
                }
                SnapshotPinTags.DecodedSnapshotPin decoded = SnapshotPinTags.decode(tag, indexId);
                if (decoded != null) {
                    requireExact(entry.getKey().id(), snapshotId(decoded.snapshotToken()), tag);
                    result.add(new SnapshotPinReference(decoded.snapshotToken(),
                            new SnapshotPinOwner(indexId, decoded.generation(), decoded.attemptId())));
                    continue;
                }
                String legacy = SnapshotPinTags.legacySnapshotToken(tag, indexId);
                if (legacy != null) {
                    requireExact(entry.getKey().id(), snapshotId(legacy), tag);
                    result.add(new SnapshotPinReference(legacy,
                            new SnapshotPinOwner(indexId, 0, "legacy")));
                } else if (tag.equals(prefix)) {
                    result.add(new SnapshotPinReference(Long.toString(entry.getKey().id()),
                            new SnapshotPinOwner(indexId, 0, "legacy")));
                }
            }
        }
        return result.stream().distinct().toList();
    }

    private List<String> snapshotTokens(String schemaName, String tableName) {
        FileStoreTable table = load(schemaName, tableName);
        List<String> result = new ArrayList<>();
        for (var entry : table.tagManager().tags().entrySet()) {
            for (String tag : entry.getValue()) {
                if (!tag.startsWith(SnapshotPinTags.PREFIX)) {
                    continue;
                }
                SnapshotPinTags.DecodedSnapshotPin decoded = SnapshotPinTags.decodeAny(tag);
                if (decoded != null) {
                    requireExact(entry.getKey().id(), snapshotId(decoded.snapshotToken()), tag);
                    result.add(decoded.snapshotToken());
                    continue;
                }
                int separator = tag.indexOf('_', SnapshotPinTags.PREFIX.length() + 64);
                String token = separator < 0 ? Long.toString(entry.getKey().id())
                        : tag.substring(separator + 1);
                if (token.matches("0|[1-9][0-9]*")) {
                    result.add(token);
                }
            }
        }
        return List.copyOf(result);
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
        return taggedSnapshot(table.tagManager().tags(), tagName);
    }

    private static Long taggedSnapshot(Map<Snapshot, List<String>> tags, String tagName) {
        Long result = null;
        for (var entry : tags.entrySet()) {
            if (entry.getValue().contains(tagName)) {
                if (result != null) {
                    throw new IllegalStateException("Paimon tag resolves to multiple snapshots: " + tagName);
                }
                result = entry.getKey().id();
            }
        }
        return result;
    }

    private static long snapshotId(String snapshotToken) {
        try {
            long id = Long.parseLong(snapshotToken);
            if (id < 0 || !Long.toString(id).equals(snapshotToken)) {
                throw new NumberFormatException("non-canonical Paimon snapshot");
            }
            return id;
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("Invalid Paimon snapshot token", invalid);
        }
    }

    private static List<String> exactTags(FileStoreTable table, SnapshotPinOwner owner, String token, long snapshotId) {
        var tags = table.tagManager().tags();
        return SnapshotPinTags.matchingNames(owner.indexId(), owner.generation(), owner.attemptId(), token, tag -> {
            Long actual = taggedSnapshot(tags, tag);
            if (actual == null) {
                return false;
            }
            requireExact(actual, snapshotId, tag);
            return true;
        });
    }

    private static boolean isLegacyOwner(SnapshotPinOwner owner) {
        return owner.generation() == 0 && owner.attemptId().equals("legacy");
    }

    private static String tag(SnapshotPinOwner owner, String snapshotToken) {
        return SnapshotPinTags.forSnapshot(owner.indexId(), owner.generation(), owner.attemptId(), snapshotToken);
    }

    private static void requireExact(Long actual, long expected, String tag) {
        if (actual == null || actual != expected) {
            throw new IllegalStateException("Paimon index pin " + tag + " differs from its snapshot");
        }
    }
}
