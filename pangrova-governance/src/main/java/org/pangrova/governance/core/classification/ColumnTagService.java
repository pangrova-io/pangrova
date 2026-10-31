package org.pangrova.governance.core.classification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.pangrova.common.util.Preconditions.checkNotNull;
import org.pangrova.governance.api.classification.TagChangedEvent;
import org.pangrova.governance.api.classification.TagChangedEventListener;

/**
 * Produces {@link TagChangedEvent} when column tags are assigned or removed.
 * Listeners (e.g. {@link TagBasedSecurityPolicy}) react to tag changes
 * to activate auto-masking or audit policies.
 */
public class ColumnTagService {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnTagService.class);

    private final java.util.function.Function<org.pangrova.connector.spi.metadata.table.QualifiedTableName,
            java.util.UUID> tableIdentities;

    /** Names are resolved through the authoritative control plane, never an event-built name cache. */
    public ColumnTagService(java.util.function.Function<
            org.pangrova.connector.spi.metadata.table.QualifiedTableName, java.util.UUID> tableIdentities) {
        this.tableIdentities = java.util.Objects.requireNonNull(tableIdentities, "tableIdentities");
    }

    private java.util.UUID identity(String catalog, String schema, String table) {
        return java.util.Objects.requireNonNull(tableIdentities.apply(
                new org.pangrova.connector.spi.metadata.table.QualifiedTableName(catalog, schema, table)), "tableId");
    }

    private final List<TagChangedEventListener> listeners = new CopyOnWriteArrayList<>();
    private final Map<String, Map<String, String>> tagStore = new ConcurrentHashMap<>();

    public Map<String, String> getTags(String catalog, String schema, String table, String columnName) {
        Map<String, String> tags = tagStore.get(identity(catalog, schema, table) + ":" + columnName);
        return tags != null ? Map.copyOf(tags) : Map.of();
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
        assignTag(identity(catalog, schema, table), catalog, schema, table, columnName, tagKey, oldValue, newValue);
    }

    /** Projects an already captured stable identity; retirement events never resolve a replacement name. */
    public void assignTag(java.util.UUID tableId, String catalog, String schema, String table,
                          String columnName, String tagKey, String oldValue, String newValue) {
        checkNotNull(catalog, "catalog must not be null");
        checkNotNull(schema, "schema must not be null");
        checkNotNull(table, "table must not be null");
        checkNotNull(columnName, "columnName must not be null");
        checkNotNull(tagKey, "tagKey must not be null");
        checkNotNull(newValue, "newValue must not be null for assignment");

        String key = java.util.Objects.requireNonNull(tableId, "tableId") + ":" + columnName;
        tagStore.computeIfAbsent(key, k -> new ConcurrentHashMap<>()).put(tagKey, newValue);
        TagChangedEvent event = new TagChangedEvent(catalog, schema, table, columnName,
                tagKey, oldValue, newValue, tableId);
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
        removeTag(identity(catalog, schema, table), catalog, schema, table, columnName, tagKey, oldValue);
    }

    /** Withdraws only the captured identity, including after it has been retired. */
    public void removeTag(java.util.UUID tableId, String catalog, String schema, String table,
                          String columnName, String tagKey, String oldValue) {
        checkNotNull(catalog, "catalog must not be null");
        checkNotNull(schema, "schema must not be null");
        checkNotNull(table, "table must not be null");
        checkNotNull(columnName, "columnName must not be null");
        checkNotNull(tagKey, "tagKey must not be null");
        checkNotNull(oldValue, "oldValue must not be null for removal");

        String key = java.util.Objects.requireNonNull(tableId, "tableId") + ":" + columnName;
        Map<String, String> tags = tagStore.get(key);
        if (tags != null) {
            tags.remove(tagKey);
            if (tags.isEmpty()) {
                tagStore.remove(key);
            }
        }
        TagChangedEvent event = new TagChangedEvent(catalog, schema, table, columnName,
                tagKey, oldValue, null, tableId);
        LOG.info("event=tag_removed catalog={} schema={} table={} column={} tagKey={} oldValue={}",
                catalog, schema, table, columnName, tagKey, oldValue);
        dispatchEvent(event);
    }

    private void dispatchEvent(TagChangedEvent event) {
        RuntimeException failure = null;
        for (TagChangedEventListener listener : listeners) {
            try {
                listener.onTagChanged(event);
            } catch (RuntimeException rejected) {
                if (failure == null) failure = rejected;
                else failure.addSuppressed(rejected);
            }
        }
        if (failure != null) throw failure;
    }
}
