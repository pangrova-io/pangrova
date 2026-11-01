package org.pangrova.table.core.control;

import org.pangrova.table.api.control.TableControlRecord;
import org.pangrova.table.api.control.TableControlState;
import org.pangrova.table.api.governancebinding.GovernanceBinding;
import org.pangrova.table.api.governancebinding.GovernanceBindingType;
import org.pangrova.table.api.governancebinding.TableControlGovernanceBindingStore;
import org.pangrova.spi.security.AccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Binds tags, masks, row filters, quality rules, and lineage to a stable
 * {@code table_id}.
 *
 * <p>Governance can only be bound to a LINKED or MANAGED table that is ACTIVE.
 * OBSERVED tables have no stable identity, so binding to them would attach
 * policy to a name that a later drop/recreate could inherit. On the read path
 * {@link #requireCurrentBinding} re-resolves the live record and rejects a
 * binding whose {@code table_id} no longer matches the table under that name.</p>
 */
public final class TableControlPolicyBinder {

    private static final Logger LOG = LoggerFactory.getLogger(TableControlPolicyBinder.class);

    private final TableControlService control;
    private final TableControlGovernanceBindingStore store;
    private final Clock clock;

    public TableControlPolicyBinder(TableControlService control,
            TableControlGovernanceBindingStore store) {
        this(control, store, Clock.systemUTC());
    }

    public TableControlPolicyBinder(TableControlService control,
            TableControlGovernanceBindingStore store, Clock clock) {
        this.control = java.util.Objects.requireNonNull(control, "control");
        this.store = java.util.Objects.requireNonNull(store, "store");
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    /** Registers or updates a binding on a controlled, active table. */
    public GovernanceBinding bind(String catalog, String schema, String table,
            GovernanceBindingType type, String bindingRef, Map<String, String> config,
            String actor) {
        TableControlRecord record = requireControlled(catalog, schema, table);
        return bind(record, type, bindingRef, config, actor);
    }

    /** Writes only the captured identity, so name reuse between admission and persistence cannot retarget policy. */
    public GovernanceBinding bind(TableControlRecord record, GovernanceBindingType type,
            String bindingRef, Map<String, String> config, String actor) {
        TableControlRecord current = requireControlled(record.catalog(), record.schema(), record.table());
        if (!current.tableId().equals(record.tableId())) {
            throw new AccessDeniedException("Governance target was replaced after admission");
        }
        if (bindingRef == null || bindingRef.isBlank()) {
            throw new AccessDeniedException("Governance binding rejected: the reference "
                    + "for " + type + " on " + record.qualifiedName() + " must not be blank");
        }
        Instant now = clock.instant();
        GovernanceBinding binding = store.find(record.tableId(), type, bindingRef)
                .map(existing -> existing.withConfig(config, now))
                .orElseGet(() -> new GovernanceBinding(UUID.randomUUID(), record.tableId(),
                        type, bindingRef, config, actor, now, now));
        store.save(binding);
        LOG.info("event=table_governance_bound table={} tableId={} type={} ref={}",
                record.qualifiedName(), record.tableId(), type, bindingRef);
        return binding;
    }

    /**
     * Resolves a binding against the live table control record. A binding left
     * over from a dropped/recreated table is rejected rather than applied to the
     * new table, because the names match but the {@code table_id} does not.
     */
    public GovernanceBinding requireCurrentBinding(String catalog, String schema, String table,
            GovernanceBindingType type, String bindingRef) {
        TableControlRecord record = requireControlled(catalog, schema, table);
        return store.find(record.tableId(), type, bindingRef)
                .orElseThrow(() -> new AccessDeniedException("Governance binding "
                        + type + "/" + bindingRef + " does not belong to the current table "
                        + record.qualifiedName() + " (tableId=" + record.tableId()
                        + "); it may refer to a dropped/recreated table and must be re-created"));
    }

    /** Lists bindings for the live table, rejecting OBSERVED and stale states. */
    public List<GovernanceBinding> bindingsFor(String catalog, String schema, String table) {
        return store.list(requireControlled(catalog, schema, table).tableId());
    }

    /** Lists an already captured identity for replay and retirement; this does not authorize a read. */
    public List<GovernanceBinding> bindingsFor(UUID tableId) {
        return store.list(java.util.Objects.requireNonNull(tableId, "tableId"));
    }

    public Optional<GovernanceBinding> find(UUID tableId, GovernanceBindingType type,
            String bindingRef) {
        return store.find(tableId, type, bindingRef);
    }

    /** Removes one binding by its stable identity. */
    public void unbind(UUID tableId, GovernanceBindingType type, String bindingRef) {
        store.delete(tableId, type, bindingRef);
    }

    /** Validates that governance may be bound here without changing anything. */
    public TableControlRecord requireBindable(String catalog, String schema, String table) {
        return requireControlled(catalog, schema, table);
    }

    /** Removes one binding after resolving the table's current stable identity. */
    public void unbind(String catalog, String schema, String table,
            GovernanceBindingType type, String bindingRef) {
        TableControlRecord record = requireControlled(catalog, schema, table);
        store.delete(record.tableId(), type, bindingRef);
    }

    /** Drops every binding when a table is unlinked or dropped. */
    public void purge(UUID tableId) {
        if (tableId == null) {
            return;
        }
        store.deleteAll(tableId);
        LOG.info("event=table_governance_bindings_purged tableId={}", tableId);
    }

    private TableControlRecord requireControlled(String catalog, String schema, String table) {
        if (!control.enabled()) {
            throw new AccessDeniedException("Governance binding requires the table control "
                    + "plane; without control records a table cannot be identified stably");
        }
        TableControlRecord record = control.find(catalog, schema, table)
                .orElseThrow(() -> new AccessDeniedException("Governance binding rejected: "
                        + catalog + "." + schema + "." + table + " is OBSERVED; run ALTER TABLE "
                        + "... ADOPT (or CREATE TABLE) before binding governance"));
        if (record.state() != TableControlState.ACTIVE) {
            throw new AccessDeniedException("Governance binding rejected: the control record "
                    + "for " + record.qualifiedName() + " is " + record.state()
                    + "; governance requires ACTIVE");
        }
        control.requireCurrentCatalog(record, "Governance binding");
        return record;
    }
}
