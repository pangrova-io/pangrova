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

    private static String key(QualifiedTableRef tableRef, String columnName) {
        Objects.requireNonNull(tableRef, "tableRef");
        Objects.requireNonNull(columnName, "columnName");
        return tableRef.catalog() + "|" + tableRef.schema() + "|"
                + tableRef.table() + "|" + columnName;
    }
}
