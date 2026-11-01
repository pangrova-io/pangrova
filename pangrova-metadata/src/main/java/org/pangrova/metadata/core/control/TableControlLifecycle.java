package org.pangrova.metadata.core.control;

import org.pangrova.metadata.api.control.OwnershipMode;
import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
import org.pangrova.spi.security.AccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;

/**
 * Registration, retirement, and rename lifecycle for controlled tables.
 *
 * <p>Every control record is created here from connector evidence, so duplicate
 * DDL entry points cannot register incompletely. Destructive transitions
 * (DROP, RENAME) retire or move only the exact registration captured before
 * the physical change started, never a newer one.</p>
 */
final class TableControlLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(TableControlLifecycle.class);

    private final TableControlStore store;
    private final Function<String, CatalogDeploymentIdentity> catalogs;
    private final TableControlSettings settings;
    private final Clock clock;

    TableControlLifecycle(TableControlStore store, Function<String, CatalogDeploymentIdentity> catalogs,
            TableControlSettings settings, Clock clock) {
        this.store = store;
        this.catalogs = catalogs;
        this.settings = settings;
        this.clock = clock;
    }

    /**
     * Records a table created by Pangrova as MANAGED.
     *
     * <p>Identity evidence is optional because connectors are not required to
     * publish it. When evidence is present and disagrees with a live record, the
     * old record is retired and a new identity is registered: drop/recreate under
     * the same name is a different table.</p>
     */
    Optional<TableControlRecord> registerCreated(
            String catalog, String schema, String table, String ownerPrincipal,
            Optional<TableIdentity> identity, Optional<TableVersionToken> version) {
        if (!settings.enabled()) {
            return Optional.empty();
        }
        Instant now = clock.instant();
        Optional<TableControlRecord> existing = store.find(catalog, schema, table);
        existing.ifPresent(TableControlSupport::requireNoPendingRename);
        if (existing.isPresent() && TableControlSupport.matchesCatalog(
                existing.get(), catalogs.apply(catalog))
                && TableControlSupport.matchesIdentity(existing.get(), identity)) {
            TableControlRecord updated = TableControlSupport.withIdentity(
                    existing.get(), identity, version, now)
                    .state(TableControlState.ACTIVE)
                    .metadataRevision(existing.get().metadataRevision() + 1)
                    .build();
            store.save(updated);
            return Optional.of(updated);
        }
        existing.ifPresent(record -> retire(record, now));

        TableControlRecord created = TableControlSupport.withIdentity(TableControlSupport.newRecord(
                catalogs, catalog, schema, table, ownerPrincipal, TableControlProvenance.CREATED, now),
                identity, version, now)
                .build();
        store.save(created);
        LOG.info("event=table_control_registered table={}.{}.{} tableId={} mode={}",
                catalog, schema, table, created.tableId(), created.ownershipMode());
        return Optional.of(created);
    }

    /**
     * Registers a table created through a connector, capturing whatever identity
     * and version evidence the connector can provide. This is the single place
     * that converts connector evidence into a control record, so duplicate DDL
     * entry points cannot register incompletely.
     */
    Optional<TableControlRecord> registerCreatedFromConnector(
            org.pangrova.spi.connector.metadata.ConnectorMetadata metadata,
            org.pangrova.spi.connector.ConnectorSession session,
            String catalog, String schema, String table, String ownerPrincipal) {
        if (!settings.enabled()) {
            return Optional.empty();
        }
        var name = new org.pangrova.spi.connector.metadata.table.SchemaTableName(schema, table);
        org.pangrova.spi.connector.metadata.table.ConnectorTableHandle handle =
                metadata.getTableHandle(session, name);
        if (handle == null) {
            throw new org.pangrova.metadata.api.error.MetastoreException(
                    "Created table cannot be resolved for control registration: "
                            + catalog + "." + schema + "." + table);
        }
        // Unsupported evidence is Optional.empty(). Lookup failures must reach
        // the DDL recovery path instead of registering an unfenced ACTIVE table.
        Optional<TableIdentity> identity = metadata.getTableIdentity(session, handle);
        Optional<TableVersionToken> version = metadata.observeVersion(session, handle);
        return registerCreated(catalog, schema, table, ownerPrincipal, identity, version);
    }

    /**
     * Adopts an externally written table as {@link OwnershipMode#LINKED}.
     *
     * <p>Identity evidence is mandatory: adopting a name without a format UUID
     * would let a later drop/recreate silently inherit governance and restricted
     * maintenance rights. Re-adopting the same live identity is idempotent, while
     * a changed identity is rejected and must be confirmed by an operator.</p>
     */
    TableControlRecord adopt(String catalog, String schema, String table,
            String ownerPrincipal, Optional<TableIdentity> identity,
            Optional<TableVersionToken> version) {
        if (!settings.enabled()) {
            throw new AccessDeniedException("ADOPT is disabled: the table control plane is off");
        }
        String qualified = catalog + "." + schema + "." + table;
        if (identity.isEmpty() || !identity.get().hasFormatUuid()) {
            throw new AccessDeniedException("ADOPT rejected: the connector cannot report a stable "
                    + "format identity for " + qualified + "; refusing to adopt by name only");
        }
        Instant now = clock.instant();
        Optional<TableControlRecord> existing = store.find(catalog, schema, table);
        if (existing.isPresent()) {
            TableControlRecord record = existing.get();
            TableControlSupport.requireCurrentCatalog(catalogs, record, "ADOPT");
            if (record.state() == TableControlState.RENAMING) {
                throw new AccessDeniedException("ADOPT rejected: name is reserved by a pending rename");
            }
            if (record.ownershipMode() == OwnershipMode.MANAGED) {
                throw new AccessDeniedException("ADOPT rejected: " + qualified
                        + " is already MANAGED by Pangrova");
            }
            if (!TableControlSupport.matchesIdentity(record, identity)) {
                throw new AccessDeniedException("ADOPT rejected: the format identity of "
                        + qualified + " changed since it was adopted (dropped and recreated?); "
                        + "confirm the new table before re-adopting");
            }
            if (record.ownershipMode() == OwnershipMode.LINKED
                    && record.state() == TableControlState.ACTIVE) {
                return record;
            }
            TableControlRecord readopted = TableControlSupport.withIdentity(record, identity, version, now)
                    .ownershipMode(OwnershipMode.LINKED)
                    .provenance(TableControlProvenance.ADOPTED)
                    .state(TableControlState.ACTIVE)
                    .ownerPrincipal(ownerPrincipal)
                    .metadataRevision(record.metadataRevision() + 1)
                    .build();
            store.save(readopted);
            LOG.info("event=table_control_readopted table={}.{}.{} tableId={} previousState={}",
                    catalog, schema, table, readopted.tableId(), record.state());
            return readopted;
        }

        TableControlRecord adopted = TableControlSupport.withIdentity(TableControlSupport.newRecord(
                catalogs, catalog, schema, table, ownerPrincipal, TableControlProvenance.ADOPTED, now)
                        .ownershipMode(OwnershipMode.LINKED),
                identity, version, now)
                .build();
        store.save(adopted);
        LOG.info("event=table_control_adopted table={}.{}.{} tableId={} formatUuid={}",
                catalog, schema, table, adopted.tableId(), adopted.formatTableUuid());
        return adopted;
    }

    /**
     * Removes control state for a LINKED table. The physical table is untouched;
     * the table returns to OBSERVED and becomes read-only for Pangrova again.
     */
    TableControlRecord unlink(String catalog, String schema, String table) {
        if (!settings.enabled()) {
            throw new AccessDeniedException("UNLINK is disabled: the table control plane is off");
        }
        String qualified = catalog + "." + schema + "." + table;
        TableControlRecord record = store.find(catalog, schema, table)
                .orElseThrow(() -> new AccessDeniedException(
                        "UNLINK rejected: " + qualified + " has no control record (OBSERVED)"));
        if (record.ownershipMode() != OwnershipMode.LINKED) {
            throw new AccessDeniedException("UNLINK rejected: " + qualified
                    + " is MANAGED; use DROP TABLE to remove a Pangrova-owned table");
        }
        TableControlRecord retired = record.toBuilder()
                .state(TableControlState.DROPPED)
                .metadataRevision(record.metadataRevision() + 1)
                .updatedAt(clock.instant())
                .build();
        store.save(retired);
        LOG.info("event=table_control_unlinked table={}.{}.{} tableId={}",
                catalog, schema, table, record.tableId());
        return retired;
    }

    /** Marks a physically created table whose control record could not be completed. */
    void registerOrphanedManaged(String catalog, String schema, String table,
            String ownerPrincipal, Optional<TableIdentity> identity) {
        if (!settings.enabled()) {
            return;
        }
        // A concurrent CREATE may fail registration after another operation reserved this name.
        // Its recovery must never overwrite the rename intent or release the protected source.
        Instant now = clock.instant();
        TableControlRecord orphaned = store.find(catalog, schema, table)
                .map(record -> {
                    TableControlSupport.requireNoPendingRename(record);
                    TableControlSupport.requireCurrentCatalog(catalogs, record, "Orphan registration");
                    return TableControlSupport.withIdentity(record, identity, Optional.empty(), now)
                        .state(TableControlState.ORPHANED_MANAGED)
                        .metadataRevision(record.metadataRevision() + 1)
                        .build();
                })
                .orElseGet(() -> TableControlSupport.withIdentity(TableControlSupport.newRecord(
                        catalogs, catalog, schema, table, ownerPrincipal,
                        TableControlProvenance.CREATED, now), identity, Optional.empty(), now)
                        .state(TableControlState.ORPHANED_MANAGED)
                        .build());
        store.save(orphaned);
    }

    /** Retires only the control record captured before the physical DROP started. */
    void markDropped(TableControlRecord expected) {
        if (!settings.enabled()) {
            return;
        }
        TableControlRecord current = store.findById(expected.tableId()).orElse(null);
        if (current == null || current.metadataRevision() != expected.metadataRevision()
                || current.state() == TableControlState.DROPPED) {
            throw new AccessDeniedException("DROP TABLE completed with a changed control record for "
                    + expected.qualifiedName() + "; refusing to retire a newer table registration");
        }
        TableControlSupport.requireCurrentCatalog(catalogs, current, "DROP TABLE completion");
        retire(current, clock.instant());
    }

    /**
     * Moves the control record with an in-place rename so ownership survives the
     * move. A live record already registered under the target name blocks the
     * move instead of being overwritten: two tables cannot share a name, and
     * silently replacing the target's ownership would strand its governance.
     */
    void rename(String catalog, String schema, String table,
            String newSchema, String newTable) {
        if (!settings.enabled()) {
            return;
        }
        Optional<TableControlRecord> source = store.find(catalog, schema, table);
        if (source.isEmpty()) {
            return;
        }
        rename(source.get(), newSchema, newTable);
    }

    /** Moves only the exact captured registration, including when rolling back a failed rename. */
    TableControlRecord rename(TableControlRecord expected, String newSchema, String newTable) {
        if (!settings.enabled()) {
            return expected;
        }
        TableControlRecord source = store.findById(expected.tableId()).orElse(null);
        if (source == null || source.metadataRevision() != expected.metadataRevision()
                || source.state() == TableControlState.DROPPED) {
            throw new AccessDeniedException("RENAME rejected: control record changed for "
                    + expected.qualifiedName() + "; refusing to move a newer table registration");
        }
        TableControlSupport.requireCurrentCatalog(catalogs, source, "RENAME");
        Optional<TableControlRecord> target = store.find(source.catalog(), newSchema, newTable);
        if (target.isPresent() && !target.get().tableId().equals(source.tableId())) {
            throw new AccessDeniedException("RENAME rejected: " + source.catalog() + "." + newSchema
                    + "." + newTable + " already has a live control record (tableId="
                    + target.get().tableId() + ")");
        }
        TableControlRecord moved = source.toBuilder()
                .state(TableControlState.RENAMING)
                .properties(TableControlRenameIntent.begin(source))
                .name(source.catalog(), newSchema, newTable)
                .metadataRevision(source.metadataRevision() + 1)
                .updatedAt(clock.instant())
                .build();
        store.save(moved);
        return moved;
    }

    /** Records physical identity changes caused by a successfully completed, admitted rename. */
    void completeRename(TableControlRecord expected, Optional<TableIdentity> identity,
            Optional<TableVersionToken> version) {
        if (!settings.enabled()) {
            return;
        }
        TableControlRecord current = store.findById(expected.tableId()).orElse(null);
        if (current == null || current.state() != TableControlState.RENAMING
                || current.metadataRevision() != expected.metadataRevision()) {
            throw new AccessDeniedException("RENAME completed with a changed control record for "
                    + expected.qualifiedName() + "; reconciliation is required");
        }
        TableIdentity physical = identity.filter(TableIdentity::hasFormatUuid)
                .filter(value -> value.format().equals(current.format()))
                .orElseThrow(() -> new AccessDeniedException(
                        "RENAME completed without verifiable format identity for "
                                + expected.qualifiedName() + "; reconciliation is required"));
        TableControlSupport.requireCurrentCatalog(catalogs, current, "RENAME completion");
        if ("ICEBERG".equalsIgnoreCase(current.format())
                && !physical.formatTableUuid().equals(current.formatTableUuid())) {
            throw new AccessDeniedException("RENAME completed with a different Iceberg table UUID for "
                    + expected.qualifiedName() + "; reconciliation is required");
        }
        Instant now = clock.instant();
        String token = version.map(TableVersionToken::token).orElse(null);
        TableControlRecord completed = TableControlSupport.withIdentity(
                current, Optional.of(physical), version, now)
                .state(TableControlState.ACTIVE)
                .properties(TableControlRenameIntent.complete(current))
                .version(token, token == null ? null : now)
                .metadataRevision(current.metadataRevision() + 1)
                .build();
        if (token == null) {
            store.save(completed);
        } else {
            store.saveWithObservation(completed, new TableVersionObservation(current.tableId(),
                    token, current.versionToken(), "DDL_RENAME", now));
        }
    }

    private void retire(TableControlRecord record, Instant now) {
        TableControlRecord dropped = record.toBuilder()
                .state(TableControlState.DROPPED)
                .metadataRevision(record.metadataRevision() + 1)
                .updatedAt(now)
                .build();
        store.save(dropped);
    }
}
