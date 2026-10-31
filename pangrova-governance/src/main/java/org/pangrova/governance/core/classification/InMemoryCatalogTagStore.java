package org.pangrova.governance.core.classification;

import org.pangrova.governance.api.catalog.CatalogTagStore;
import org.pangrova.governance.store.jdbc.JdbcColumnTagStore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory catalog tag store for development and testing.
 * Tags stored here are transient and do not survive coordinator restart.
 *
 * <p>For persistent storage, a JDBC-backed implementation can be substituted
 * (analogous to {@link JdbcColumnTagStore} for column tags).</p>
 */
public final class InMemoryCatalogTagStore implements CatalogTagStore {

    /** Key: catalogName, Value: tagKey to tagValue. */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, String>> store =
            new ConcurrentHashMap<>();

    @Override
    public void saveTag(String catalogName, String tagKey, String tagValue) {
        store.computeIfAbsent(catalogName, k -> new ConcurrentHashMap<>())
                .put(tagKey, tagValue);
    }

    @Override
    public void deleteTag(String catalogName, String tagKey) {
        ConcurrentHashMap<String, String> tags = store.get(catalogName);
        if (tags != null) {
            tags.remove(tagKey);
        }
    }

    @Override
    public Map<String, String> getTags(String catalogName) {
        ConcurrentHashMap<String, String> tags = store.get(catalogName);
        return tags == null ? Map.of() : Map.copyOf(tags);
    }

    @Override
    public List<CatalogTagEntry> getAllTags() {
        return store.entrySet().stream()
                .flatMap(entry -> entry.getValue().entrySet().stream()
                        .map(tag -> new CatalogTagEntry(
                                entry.getKey(), tag.getKey(), tag.getValue())))
                .toList();
    }
}
