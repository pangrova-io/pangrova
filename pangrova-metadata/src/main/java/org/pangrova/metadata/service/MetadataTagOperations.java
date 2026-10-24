package org.pangrova.metadata.service;

import org.pangrova.governance.classification.ColumnTagService;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.tag.ColumnTagStore;
import org.pangrova.metadata.tag.InMemoryColumnTagStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/** Owns column-tag persistence and governance notifications for metadata service. */
final class MetadataTagOperations {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataTagOperations.class);

    private volatile ColumnTagService columnTagService;
    private volatile ColumnTagStore tagStore = new InMemoryColumnTagStore();

    void setColumnTagService(ColumnTagService service) {
        columnTagService = service;
    }

    void setColumnTagStore(ColumnTagStore store) {
        tagStore = Objects.requireNonNull(store, "store");
        LOG.info("event=column_tag_store_installed type={}", store.getClass().getSimpleName());
    }

    void setTag(QualifiedTableRef tableRef, String columnName, String tagKey, String value) {
        Objects.requireNonNull(value, "value");
        String oldValue = tagStore.put(key(tableRef, columnName),
                Objects.requireNonNull(tagKey, "tagKey"), value);
        ColumnTagService service = columnTagService;
        if (service != null) {
            service.assignTag(tableRef.catalog(), tableRef.schema(), tableRef.table(),
                    columnName, tagKey, oldValue, value);
        }
        LOG.info("event=column_tag_set catalog={} schema={} table={} column={} tagKey={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table(), columnName, tagKey);
    }

    void unsetTag(QualifiedTableRef tableRef, String columnName, String tagKey) {
        Objects.requireNonNull(tagKey, "tagKey");
        String removed = tagStore.remove(key(tableRef, columnName), tagKey);
        if (removed == null) {
            return;
        }
        ColumnTagService service = columnTagService;
        if (service != null) {
            service.removeTag(tableRef.catalog(), tableRef.schema(), tableRef.table(),
                    columnName, tagKey, removed);
        }
        LOG.info("event=column_tag_unset catalog={} schema={} table={} column={} tagKey={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table(), columnName, tagKey);
    }

    Map<String, String> listTags(QualifiedTableRef tableRef, String columnName) {
        return tagStore.list(key(tableRef, columnName));
    }

    /**
     * Replays persisted tags into the shared {@link ColumnTagService} so
     * listeners can regenerate derived policy (for example auto-masks) after a
     * coordinator restart. Persistence is not touched.
     */
    int replayAll() {
        ColumnTagService service = columnTagService;
        if (service == null) {
            return 0;
        }
        int replayed = 0;
        for (Map.Entry<String, Map<String, String>> entry : tagStore.listAll().entrySet()) {
            String[] parts = entry.getKey().split("\\|", 4);
            if (parts.length != 4) {
                LOG.warn("event=column_tag_replay_skipped reason=unparseable_key key={}",
                        entry.getKey());
                continue;
            }
            for (Map.Entry<String, String> tag : entry.getValue().entrySet()) {
                service.assignTag(parts[0], parts[1], parts[2], parts[3],
                        tag.getKey(), null, tag.getValue());
                replayed++;
            }
        }
        LOG.info("event=column_tags_replayed tags={}", replayed);
        return replayed;
    }

    /**
     * Removes every tag on a retired table. Each removal is dispatched so
     * derived policy (auto-masks) is withdrawn with the tag.
     */
    int clearTable(QualifiedTableRef tableRef) {
        String prefix = tableRef.catalog() + "|" + tableRef.schema() + "|"
                + tableRef.table() + "|";
        int removed = 0;
        for (Map.Entry<String, Map<String, String>> entry : tagStore.listAll().entrySet()) {
            if (!entry.getKey().startsWith(prefix)) {
                continue;
            }
            String columnName = entry.getKey().substring(prefix.length());
            for (String tagKey : entry.getValue().keySet()) {
                unsetTag(tableRef, columnName, tagKey);
                removed++;
            }
        }
        return removed;
    }

    private static String key(QualifiedTableRef tableRef, String columnName) {
        Objects.requireNonNull(tableRef, "tableRef");
        Objects.requireNonNull(columnName, "columnName");
        return tableRef.catalog() + "|" + tableRef.schema() + "|"
                + tableRef.table() + "|" + columnName;
    }

    /**
     * Moves every tag entry to the renamed table. The new name is written and
     * announced before the old entry is withdrawn, so a failure part-way leaves
     * policy over-applied rather than silently dropped.
     */
    int renameTable(QualifiedTableRef from, QualifiedTableRef to) {
        if (from.equals(to)) {
            return 0;
        }
        String prefix = from.catalog() + "|" + from.schema() + "|" + from.table() + "|";
        int moved = 0;
        for (Map.Entry<String, Map<String, String>> entry : tagStore.listAll().entrySet()) {
            if (!entry.getKey().startsWith(prefix)) {
                continue;
            }
            String columnName = entry.getKey().substring(prefix.length());
            for (Map.Entry<String, String> tag : entry.getValue().entrySet()) {
                setTag(to, columnName, tag.getKey(), tag.getValue());
                unsetTag(from, columnName, tag.getKey());
                moved++;
            }
        }
        if (moved > 0) {
            LOG.info("event=column_tags_renamed from={}.{}.{} to={}.{}.{} tags={}",
                    from.catalog(), from.schema(), from.table(),
                    to.catalog(), to.schema(), to.table(), moved);
        }
        return moved;
    }
}
