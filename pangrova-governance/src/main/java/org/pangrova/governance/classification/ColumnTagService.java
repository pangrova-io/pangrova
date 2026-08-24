package org.pangrova.governance.classification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Produces {@link TagChangedEvent} when column tags are assigned or removed.
 * Listeners (e.g. {@link TagBasedSecurityPolicy}) react to tag changes
 * to activate auto-masking or audit policies.
 */
public class ColumnTagService {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnTagService.class);

    private final List<TagChangedEventListener> listeners = new CopyOnWriteArrayList<>();
    private final Map<String, Map<String, String>> tagStore = new ConcurrentHashMap<>();

    private static String tagStoreKey(String catalog, String schema, String table, String column) {
        return catalog + "." + schema + "." + table + "." + column;
    }

    public Map<String, String> getTags(String catalog, String schema, String table, String columnName) {
        Map<String, String> tags = tagStore.get(tagStoreKey(catalog, schema, table, columnName));
        return tags != null ? Collections.unmodifiableMap(tags) : Map.of();
    }

    /**
     * Registers a listener that will be notified on tag changes.
     */
    public void addListener(TagChangedEventListener listener) {
        checkNotNull(listener, "listener must not be null");
        listeners.add(listener);
    }

    /**
     * Removes a previously registered listener.
     */
    public void removeListener(TagChangedEventListener listener) {
        listeners.remove(listener);
    }

    /**
     * Assigns a tag to a column. Emits a {@link TagChangedEvent} to all registered listeners.
     *
     * @param catalog    catalog name
     * @param schema     schema name
     * @param table      table name
     * @param columnName column name
     * @param tagKey     tag key being assigned
     * @param oldValue   previous tag value (null if new assignment)
     * @param newValue   new tag value (must not be null for assignment)
     */
    public void assignTag(String catalog, String schema, String table,
                          String columnName, String tagKey, String oldValue, String newValue) {
        checkNotNull(catalog, "catalog must not be null");
        checkNotNull(schema, "schema must not be null");
        checkNotNull(table, "table must not be null");
        checkNotNull(columnName, "columnName must not be null");
        checkNotNull(tagKey, "tagKey must not be null");
        checkNotNull(newValue, "newValue must not be null for assignment");

        String key = tagStoreKey(catalog, schema, table, columnName);
        tagStore.computeIfAbsent(key, k -> new ConcurrentHashMap<>()).put(tagKey, newValue);
        TagChangedEvent event = new TagChangedEvent(catalog, schema, table, columnName, tagKey, oldValue, newValue);
        LOG.info("event=tag_assigned catalog={} schema={} table={} column={} tagKey={} newValue={}",
                catalog, schema, table, columnName, tagKey, newValue);
        dispatchEvent(event);
    }

    /**
     * Removes a tag from a column. Emits a {@link TagChangedEvent} to all registered listeners.
     *
     * @param catalog    catalog name
     * @param schema     schema name
     * @param table      table name
     * @param columnName column name
     * @param tagKey     tag key being removed
     * @param oldValue   previous tag value being removed
     */
    public void removeTag(String catalog, String schema, String table,
                          String columnName, String tagKey, String oldValue) {
        checkNotNull(catalog, "catalog must not be null");
        checkNotNull(schema, "schema must not be null");
        checkNotNull(table, "table must not be null");
        checkNotNull(columnName, "columnName must not be null");
        checkNotNull(tagKey, "tagKey must not be null");
        checkNotNull(oldValue, "oldValue must not be null for removal");

        String key = tagStoreKey(catalog, schema, table, columnName);
        Map<String, String> tags = tagStore.get(key);
        if (tags != null) {
            tags.remove(tagKey);
            if (tags.isEmpty()) {
                tagStore.remove(key);
            }
        }
        TagChangedEvent event = new TagChangedEvent(catalog, schema, table, columnName, tagKey, oldValue, null);
        LOG.info("event=tag_removed catalog={} schema={} table={} column={} tagKey={} oldValue={}",
                catalog, schema, table, columnName, tagKey, oldValue);
        dispatchEvent(event);
    }

    private void dispatchEvent(TagChangedEvent event) {
        for (TagChangedEventListener listener : listeners) {
            try {
                listener.onTagChanged(event);
            } catch (Exception e) {
                LOG.error("event=tag_listener_failed listenerClass={} tagKey={} column={}",
                        listener.getClass().getSimpleName(), event.tagKey(), event.columnName(), e);
            }
        }
    }
}
