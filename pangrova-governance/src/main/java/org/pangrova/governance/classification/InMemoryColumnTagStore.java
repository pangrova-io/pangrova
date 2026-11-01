package org.pangrova.governance.classification;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory column tag store for development and testing.
 * Tags stored here are transient — they do not survive coordinator restart.
 *
 * <p>For persistent tag storage, the governance bootstrap must wire a durable
 * {@link ColumnTagStore} implementation; this class is for development/testing only.</p>
 */
public final class InMemoryColumnTagStore implements ColumnTagStore {

    /** Key: "catalog|schema|table|column", Value: tagKey → tagValue. */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, String>> store =
            new ConcurrentHashMap<>();

    @Override
    public void saveTag(String catalog, String schema, String table,
                        String columnName, String tagKey, String tagValue) {
        String key = storeKey(catalog, schema, table, columnName);
        store.computeIfAbsent(key, k -> new ConcurrentHashMap<>())
                .put(tagKey, tagValue);
    }

    @Override
    public void deleteTag(String catalog, String schema, String table,
                          String columnName, String tagKey) {
        String key = storeKey(catalog, schema, table, columnName);
        ConcurrentHashMap<String, String> tags = store.get(key);
        if (tags != null) {
            tags.remove(tagKey);
        }
    }

    @Override
    public Map<String, String> getTags(String catalog, String schema, String table, String columnName) {
        String key = storeKey(catalog, schema, table, columnName);
        ConcurrentHashMap<String, String> tags = store.get(key);
        return tags == null ? Map.of() : Map.copyOf(tags);
    }

    @Override
    public List<ColumnTagEntry> getAllTags() {
        return store.entrySet().stream()
                .flatMap(entry -> {
                    String[] parts = entry.getKey().split("\\|", 4);
                    String catalog = parts[0];
                    String schema = parts[1];
                    String table = parts[2];
                    String columnName = parts[3];
                    return entry.getValue().entrySet().stream()
                            .map(tag -> new ColumnTagEntry(
                                    catalog, schema, table, columnName,
                                    tag.getKey(), tag.getValue()));
                })
                .toList();
    }

    private static String storeKey(String catalog, String schema, String table, String columnName) {
        return catalog + "|" + schema + "|" + table + "|" + columnName;
    }
}
