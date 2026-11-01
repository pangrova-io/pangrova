package org.pangrova.metadata.core.control;

import org.pangrova.metadata.api.control.OwnershipMode;
import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.security.AccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Write and DDL admission for the table control plane.
 *
 * <p>Admission fences every mutating statement before it reaches a connector:
 * only {@link OwnershipMode#MANAGED} tables in {@link TableControlState#ACTIVE}
 * state may be written, and a recorded format identity must still describe the
 * table the connector just resolved. {@link #enforcing()} is false while the
 * control plane runs in shadow mode.</p>
 */
final class TableControlAdmission {

    private static final Logger LOG = LoggerFactory.getLogger(TableControlAdmission.class);

    private final TableControlStore store;
    private final Function<String, CatalogDeploymentIdentity> catalogs;
    private final TableControlSettings settings;
    private final Clock clock;

    TableControlAdmission(TableControlStore store, Function<String, CatalogDeploymentIdentity> catalogs,
            TableControlSettings settings, Clock clock) {
        this.store = store;
        this.catalogs = catalogs;
        this.settings = settings;
        this.clock = clock;
    }

    /** Rejects a reserved name before any physical CREATE or registration. */
    void requireCreatableName(String catalog, String schema, String table) {
        store.find(catalog, schema, table).ifPresent(TableControlSupport::requireNoPendingRename);
    }

    /** Rejects DML before it reaches the connector unless the table is MANAGED and ACTIVE. */
    void requireManagedForWrite(String catalog, String schema, String table, String operation) {
        requireManaged(catalog, schema, table, operation);
    }

    /**
     * DML admission with an identity fence. A MANAGED record that carries a format
     * UUID must still describe the table the connector just resolved; otherwise the
     * name was dropped and recreated behind our back and the write is quarantined.
     *
     * <p>Missing evidence is not the same as contradicting evidence. A connector
     * that cannot report an identity right now blocks the write but leaves the
     * record untouched, so a transient lookup failure cannot permanently disable a
     * healthy table. Only explicitly allowlisted legacy tables may bind their
     * first reported identity; a name alone never grants ownership.</p>
     */
    void requireManagedForWrite(String catalog, String schema, String table,
            String operation, Optional<TableIdentity> observedIdentity) {
        TableControlRecord record = requireManaged(catalog, schema, table, operation);
        requireMatchingIdentity(record, operation, observedIdentity);
    }

    private void requireMatchingIdentity(TableControlRecord record, String operation,
            Optional<TableIdentity> observedIdentity) {
        if (record == null) {
            return;
        }
        TableControlSupport.requireCurrentCatalog(catalogs, record, operation);
        Optional<TableIdentity> evidence = observedIdentity
                .filter(TableIdentity::hasFormatUuid);
        if (!TableControlSupport.hasFormatUuid(record)) {
            if (settings.isLegacyWriteAllowlisted(record.qualifiedName()) && evidence.isPresent()) {
                store.save(TableControlSupport.withIdentity(record, evidence, Optional.empty(), clock.instant())
                        .metadataRevision(record.metadataRevision() + 1)
                        .build());
                return;
            }
            throw new AccessDeniedException(operation + " rejected: no registered format identity for "
                    + record.qualifiedName() + "; ownership cannot be inferred from its current name");
        }
        if (evidence.isEmpty()) {
            throw new AccessDeniedException(operation + " rejected: the connector reported no "
                    + "format identity for " + record.qualifiedName()
                    + ", so the operation cannot be fenced against a drop/recreate");
        }
        if (TableControlSupport.sameIdentity(record, evidence.get())) {
            return;
        }
        store.save(record.toBuilder()
                .state(TableControlState.QUARANTINED)
                .metadataRevision(record.metadataRevision() + 1)
                .updatedAt(clock.instant())
                .build());
        throw new AccessDeniedException(operation + " rejected: the format identity of "
                + record.qualifiedName() + " no longer matches its control record; "
                + "re-adopt the table after confirming the drop/recreate");
    }

    /** True when admission decisions are enforced, not merely recorded in shadow mode. */
    boolean enforcing() {
        return settings.enabled() && settings.externalReadOnly();
    }

    /** Fences a controlled operation against retirement, concurrent changes and physical replacement. */
    void requireActiveIdentity(TableControlRecord expected, Optional<TableIdentity> identity,
            String operation) {
        TableControlRecord current = store.findById(expected.tableId()).orElse(null);
        if (current == null || current.state() != TableControlState.ACTIVE
                || current.metadataRevision() != expected.metadataRevision()) {
            throw new AccessDeniedException(operation + " rejected: control record changed for "
                    + expected.qualifiedName() + "; resolve the table again");
        }
        TableControlSupport.requireCurrentCatalog(catalogs, current, operation);
        if (!TableControlSupport.hasFormatUuid(current)
                || identity.filter(TableIdentity::hasFormatUuid).isEmpty()) {
            throw new AccessDeniedException(operation + " rejected: missing format identity for "
                    + expected.qualifiedName());
        }
        requireMatchingIdentity(current, operation, identity);
    }

    /** Fences read admission while allowing a selected immutable snapshot to predate a data-version observation. */
    void requireReadIdentity(TableControlRecord expected, Optional<TableIdentity> identity, String operation) {
        TableControlRecord current = currentReadOwner(expected, operation);
        if (!TableControlSupport.hasFormatUuid(current)
                || identity.filter(TableIdentity::hasFormatUuid).isEmpty()) {
            throw new AccessDeniedException(operation + " rejected: missing format identity for "
                    + expected.qualifiedName());
        }
        requireMatchingIdentity(current, operation, identity);
    }

    /** Revalidates a cached binding's captured owner without treating old evidence as a new observation. */
    void requireReadOwner(TableControlRecord expected, String operation) {
        currentReadOwner(expected, operation);
    }

    private TableControlRecord currentReadOwner(TableControlRecord expected, String operation) {
        TableControlRecord current = store.findById(expected.tableId()).orElse(null);
        if (current == null || current.state() != TableControlState.ACTIVE
                || !current.catalog().equals(expected.catalog())
                || !current.schema().equals(expected.schema())
                || !current.table().equals(expected.table())
                || !java.util.Objects.equals(current.catalogIncarnationId(), expected.catalogIncarnationId())
                || !java.util.Objects.equals(current.catalogRevision(), expected.catalogRevision())
                || !current.format().equals(expected.format())
                || !java.util.Objects.equals(current.formatTableUuid(), expected.formatTableUuid())
                || !java.util.Objects.equals(current.formatLocation(), expected.formatLocation())) {
            throw new AccessDeniedException(operation + " rejected: table ownership or namespace changed for "
                    + expected.qualifiedName() + "; resolve the table again");
        }
        TableControlSupport.requireCurrentCatalog(catalogs, current, operation);
        return current;
    }

    /** Rejects destructive DDL (DROP/ALTER) unless the table is MANAGED and ACTIVE. */
    void requireManagedForDdl(String catalog, String schema, String table, String operation) {
        requireManaged(catalog, schema, table, operation, false);
    }

    /** Checks ownership and the physical identity resolved for destructive DDL. */
    void requireManagedForDdl(String catalog, String schema, String table, String operation,
            Optional<TableIdentity> observedIdentity) {
        requireMatchingIdentity(requireManaged(catalog, schema, table, operation, false),
                operation, observedIdentity);
    }

    /**
     * Admits DROP TABLE. An ORPHANED_MANAGED record is droppable because the
     * physical table was created but the control record could not be completed:
     * refusing DROP would leave an unusable table that cannot be cleaned up.
     */
    void requireDroppable(String catalog, String schema, String table, String operation) {
        requireManaged(catalog, schema, table, operation, true);
    }

    /** Allows orphan cleanup only after checking any recorded physical identity. */
    void requireDroppable(String catalog, String schema, String table, String operation,
            Optional<TableIdentity> observedIdentity) {
        requireMatchingIdentity(requireManaged(catalog, schema, table, operation, true),
                operation, observedIdentity);
    }

    private TableControlRecord requireManaged(String catalog, String schema, String table,
            String operation) {
        return requireManaged(catalog, schema, table, operation, false);
    }

    private TableControlRecord requireManaged(String catalog, String schema, String table,
            String operation, boolean allowOrphaned) {
        if (!settings.enabled() || !settings.externalReadOnly()) {
            return null;
        }
        String qualified = catalog + "." + schema + "." + table;
        TableControlRecord record = store.find(catalog, schema, table).orElse(null);
        if (record == null) {
            if (settings.isLegacyWriteAllowlisted(qualified)) {
                return registerLegacyManaged(catalog, schema, table);
            }
            throw new AccessDeniedException(operation + " requires a MANAGED table; "
                    + qualified + " is OBSERVED (visible in the catalog but not owned by Pangrova)");
        }
        if (record.ownershipMode() != OwnershipMode.MANAGED) {
            throw new AccessDeniedException(operation + " is not allowed on LINKED table "
                    + qualified + "; only governance and explicitly authorized maintenance are supported");
        }
        boolean live = record.state() == TableControlState.ACTIVE
                || (allowOrphaned && record.state() == TableControlState.ORPHANED_MANAGED);
        if (!live) {
            throw new AccessDeniedException(operation + " requires an ACTIVE control record; "
                    + qualified + " is " + record.state());
        }
        TableControlSupport.requireCurrentCatalog(catalogs, record, operation);
        return record;
    }

    private TableControlRecord registerLegacyManaged(String catalog, String schema, String table) {
        Instant now = clock.instant();
        TableControlRecord legacy = TableControlSupport.newRecord(
                catalogs, catalog, schema, table, "", TableControlProvenance.CREATED, now)
                .format(TableControlSupport.LEGACY_FORMAT)
                .properties(Map.of("legacy-write-allowlist", "true"))
                .build();
        store.save(legacy);
        LOG.info("event=table_control_legacy_allowlisted table={}.{}.{} tableId={}",
                catalog, schema, table, legacy.tableId());
        return legacy;
    }
}
