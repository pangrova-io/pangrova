package org.pangrova.governance.runtime.classification;

import org.pangrova.table.api.policy.TablePolicyBindings;
import org.pangrova.table.api.policy.GovernanceBinding;
import org.pangrova.table.api.enums.GovernanceBindingType;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.identity.TableRef;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Column tags have one durable authority: governance bindings keyed by stable table identity. */
public final class ColumnTagAuthority {
    private final TablePolicyBindings authority;
    private final ColumnTagService events;

    public ColumnTagAuthority(TablePolicyBindings authority) {
        this.authority = Objects.requireNonNull(authority, "authority");
        this.events = new ColumnTagService(table -> authority.requireBindable(
                table.catalog(), table.schema(), table.table()).tableId());
    }

    /** The event projection attached to this binding authority for its entire lifetime. */
    public ColumnTagService events() {
        return events;
    }

    /** Binds a column tag on the current table record and publishes the assignment event. */
    public void setTag(TableControlRecord admitted, String column, String tagKey, String value, String actor) {
        Objects.requireNonNull(value, "value");
        if ("pii_type".equals(tagKey) && TagBasedSecurityPolicy
                .piiTypeToMaskExpression(value) == null) {
            throw new org.pangrova.security.api.exception.AccessDeniedException("Unknown PII tag value");
        }
        String reference = reference(column, tagKey);
        TableControlRecord record = current(admitted);
        String previous = authority.find(record.tableId(), GovernanceBindingType.COLUMN_TAG, reference)
                .map(binding -> binding.config().get("value")).orElse(null);
        authority.bind(record, GovernanceBindingType.COLUMN_TAG,
                reference, Map.of("column", column, "tagKey", tagKey, "value", value), actor);
        events.assignTag(record.tableId(), record.catalog(), record.schema(), record.table(),
                column, tagKey, previous, value);
    }

    /** Removes a column tag binding, if present, and publishes the removal event. */
    public void unsetTag(TableControlRecord admitted, String column, String tagKey) {
        String reference = reference(column, tagKey);
        TableControlRecord record = current(admitted);
        var existing = authority.find(record.tableId(), GovernanceBindingType.COLUMN_TAG, reference);
        if (existing.isEmpty()) return;
        authority.unbind(record.tableId(), GovernanceBindingType.COLUMN_TAG, reference);
        emitRemoval(record, existing.orElseThrow());
    }

    /** Returns the tag keys and values bound to one column of the current table record. */
    public Map<String, String> listTags(TableControlRecord admitted, String column) {
        Objects.requireNonNull(column, "column");
        TableControlRecord record = current(admitted);
        Map<String, String> result = new LinkedHashMap<>();
        for (GovernanceBinding binding : authority.bindingsFor(record.tableId())) {
            if (binding.bindingType() == GovernanceBindingType.COLUMN_TAG
                    && column.equals(binding.config().get("column"))) {
                result.put(binding.config().get("tagKey"), binding.config().get("value"));
            }
        }
        return Map.copyOf(result);
    }

    /** Republishes every persisted column tag of active tables and returns the number replayed. */
    public int replayAll() {
        int replayed = 0;
        for (TableControlRecord record : authority.listLive()) {
            if (record.state() != TableControlState.ACTIVE) continue;
            authority.requireCurrentCatalog(record, "Governance replay");
            for (GovernanceBinding binding : authority.bindingsFor(record.tableId())) {
                if (binding.bindingType() != GovernanceBindingType.COLUMN_TAG) continue;
                Map<String, String> config = binding.config();
                events.assignTag(record.tableId(), record.catalog(), record.schema(), record.table(),
                        Objects.requireNonNull(config.get("column"), "Persisted tag column"),
                        Objects.requireNonNull(config.get("tagKey"), "Persisted tag key"),
                        null, Objects.requireNonNull(config.get("value"), "Persisted tag value"));
                replayed++;
            }
        }
        return replayed;
    }

    /** Removes every column tag of a table and returns the number removed. */
    public int clearTable(UUID tableId) {
        TableControlRecord record = authority.findById(tableId).orElseThrow();
        int removed = 0;
        for (GovernanceBinding binding : authority.bindingsFor(tableId)) {
            if (binding.bindingType() != GovernanceBindingType.COLUMN_TAG) continue;
            authority.unbind(tableId, GovernanceBindingType.COLUMN_TAG, binding.bindingRef());
            emitRemoval(record, binding);
            removed++;
        }
        return removed;
    }

    /** Republishes column tags under the reserved rename identity and returns the number moved. */
    public int renameTable(TableRef from, TableRef to) {
        TableControlRecord record = authority.find(to.catalog(), to.schema(), to.table())
                .orElseThrow(() -> new IllegalStateException("Renamed table identity is missing"));
        if (!from.catalog().equals(to.catalog()) || record.state() != TableControlState.RENAMING) {
            throw new IllegalStateException("Tag projection requires the reserved rename identity");
        }
        int moved = 0;
        for (GovernanceBinding binding : authority.bindingsFor(record.tableId())) {
            if (binding.bindingType() != GovernanceBindingType.COLUMN_TAG) continue;
            Map<String, String> config = binding.config();
            events.assignTag(record.tableId(), to.catalog(), to.schema(), to.table(),
                    config.get("column"), config.get("tagKey"), null, config.get("value"));
            moved++;
        }
        return moved;
    }

    private TableControlRecord current(TableControlRecord admitted) {
        Objects.requireNonNull(admitted, "admitted");
        authority.requireReadOwner(admitted, "Column tags");
        var current = authority.requireBindable(
                admitted.catalog(), admitted.schema(), admitted.table());
        if (!current.tableId().equals(admitted.tableId())) {
            throw new org.pangrova.security.api.exception.AccessDeniedException(
                    "Column tag target was replaced after admission");
        }
        return admitted;
    }

    private void emitRemoval(TableControlRecord record, GovernanceBinding binding) {
        Map<String, String> config = binding.config();
        events.removeTag(record.tableId(), record.catalog(), record.schema(), record.table(),
                config.get("column"), config.get("tagKey"), config.get("value"));
    }

    private static String reference(String column, String tagKey) {
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(tagKey, "tagKey");
        // Length framing prevents (column=a:b, tag=c) colliding with (column=a, tag=b:c).
        return column.codePointCount(0, column.length()) + ":" + column + tagKey;
    }
}
