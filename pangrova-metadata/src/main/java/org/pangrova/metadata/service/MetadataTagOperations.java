package org.pangrova.metadata.service;

import org.pangrova.governance.core.classification.ColumnTagService;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.table.api.governancebinding.GovernanceBinding;
import org.pangrova.table.api.governancebinding.GovernanceBindingType;
import org.pangrova.table.api.control.TableControlRecord;
import org.pangrova.table.api.control.TableControlState;
import org.pangrova.metadata.api.object.QualifiedTableRef;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Column tags have one durable authority: governance bindings keyed by stable table identity. */
final class MetadataTagOperations {
    private final CatalogRegistry catalogs;
    private volatile ColumnTagService columnTagService;

    MetadataTagOperations(CatalogRegistry catalogs) {
        this.catalogs = Objects.requireNonNull(catalogs, "catalogs");
    }

    void setColumnTagService(ColumnTagService service) {
        columnTagService = service;
    }

    void setTag(TableControlRecord admitted, String column, String tagKey, String value, String actor) {
        Objects.requireNonNull(value, "value");
        if ("pii_type".equals(tagKey) && org.pangrova.governance.core.classification.TagBasedSecurityPolicy
                .piiTypeToMaskExpression(value) == null) {
            throw new org.pangrova.security.api.AccessDeniedException("Unknown PII tag value");
        }
        String reference = reference(column, tagKey);
        TableControlRecord record = current(admitted);
        var binder = catalogs.tableGovernance();
        String previous = binder.find(record.tableId(), GovernanceBindingType.COLUMN_TAG, reference)
                .map(binding -> binding.config().get("value")).orElse(null);
        binder.bind(record, GovernanceBindingType.COLUMN_TAG,
                reference, Map.of("column", column, "tagKey", tagKey, "value", value), actor);
        ColumnTagService service = columnTagService;
        if (service != null) {
            service.assignTag(record.tableId(), record.catalog(), record.schema(), record.table(),
                    column, tagKey, previous, value);
        }
    }

    void unsetTag(TableControlRecord admitted, String column, String tagKey) {
        String reference = reference(column, tagKey);
        TableControlRecord record = current(admitted);
        var binder = catalogs.tableGovernance();
        var existing = binder.find(record.tableId(), GovernanceBindingType.COLUMN_TAG, reference);
        if (existing.isEmpty()) return;
        binder.unbind(record.tableId(), GovernanceBindingType.COLUMN_TAG, reference);
        emitRemoval(record, existing.orElseThrow());
    }

    Map<String, String> listTags(TableControlRecord admitted, String column) {
        Objects.requireNonNull(column, "column");
        TableControlRecord record = current(admitted);
        Map<String, String> result = new LinkedHashMap<>();
        for (GovernanceBinding binding : catalogs.tableGovernance().bindingsFor(record.tableId())) {
            if (binding.bindingType() == GovernanceBindingType.COLUMN_TAG
                    && column.equals(binding.config().get("column"))) {
                result.put(binding.config().get("tagKey"), binding.config().get("value"));
            }
        }
        return Map.copyOf(result);
    }

    int replayAll() {
        ColumnTagService service = columnTagService;
        if (service == null) return 0;
        int replayed = 0;
        for (TableControlRecord record : catalogs.tableControl().listLive()) {
            if (record.state() != TableControlState.ACTIVE) continue;
            catalogs.tableControl().requireCurrentCatalog(record, "Governance replay");
            for (GovernanceBinding binding : catalogs.tableGovernance().bindingsFor(record.tableId())) {
                if (binding.bindingType() != GovernanceBindingType.COLUMN_TAG) continue;
                Map<String, String> config = binding.config();
                service.assignTag(record.tableId(), record.catalog(), record.schema(), record.table(),
                        Objects.requireNonNull(config.get("column"), "Persisted tag column"),
                        Objects.requireNonNull(config.get("tagKey"), "Persisted tag key"),
                        null, Objects.requireNonNull(config.get("value"), "Persisted tag value"));
                replayed++;
            }
        }
        return replayed;
    }

    int clearTable(UUID tableId) {
        TableControlRecord record = catalogs.tableControl().findById(tableId).orElseThrow();
        int removed = 0;
        for (GovernanceBinding binding : catalogs.tableGovernance().bindingsFor(tableId)) {
            if (binding.bindingType() != GovernanceBindingType.COLUMN_TAG) continue;
            catalogs.tableGovernance().unbind(tableId, GovernanceBindingType.COLUMN_TAG, binding.bindingRef());
            emitRemoval(record, binding);
            removed++;
        }
        return removed;
    }

    int renameTable(QualifiedTableRef from, QualifiedTableRef to) {
        TableControlRecord record = catalogs.tableControl().find(to.catalog(), to.schema(), to.table())
                .orElseThrow(() -> new IllegalStateException("Renamed table identity is missing"));
        if (!from.catalog().equals(to.catalog()) || record.state() != TableControlState.RENAMING) {
            throw new IllegalStateException("Tag projection requires the reserved rename identity");
        }
        int moved = 0;
        for (GovernanceBinding binding : catalogs.tableGovernance().bindingsFor(record.tableId())) {
            if (binding.bindingType() != GovernanceBindingType.COLUMN_TAG) continue;
            ColumnTagService service = columnTagService;
            if (service != null) {
                Map<String, String> config = binding.config();
                service.assignTag(record.tableId(), to.catalog(), to.schema(), to.table(),
                        config.get("column"), config.get("tagKey"), null, config.get("value"));
            }
            moved++;
        }
        return moved;
    }

    private TableControlRecord current(TableControlRecord admitted) {
        Objects.requireNonNull(admitted, "admitted");
        catalogs.tableControl().requireReadOwner(admitted, "Column tags");
        var current = catalogs.tableGovernance().requireBindable(
                admitted.catalog(), admitted.schema(), admitted.table());
        if (!current.tableId().equals(admitted.tableId())) {
            throw new org.pangrova.security.api.AccessDeniedException("Column tag target was replaced after admission");
        }
        return admitted;
    }

    private void emitRemoval(TableControlRecord record, GovernanceBinding binding) {
        ColumnTagService service = columnTagService;
        if (service != null) {
            Map<String, String> config = binding.config();
            service.removeTag(record.tableId(), record.catalog(), record.schema(), record.table(),
                    config.get("column"), config.get("tagKey"), config.get("value"));
        }
    }

    private static String reference(String column, String tagKey) {
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(tagKey, "tagKey");
        // Length framing prevents (column=a:b, tag=c) colliding with (column=a, tag=b:c).
        return column.codePointCount(0, column.length()) + ":" + column + tagKey;
    }
}
