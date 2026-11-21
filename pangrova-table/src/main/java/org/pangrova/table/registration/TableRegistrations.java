package org.pangrova.table.registration;

import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableVersionToken;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.table.api.exception.TableControlException;
import org.pangrova.table.api.enums.TableControlProvenance;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.ownership.TableControlStore;
import org.pangrova.table.config.TableControlSettings;
import org.pangrova.table.identity.TableCatalogAuthority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.pangrova.table.identity.TablePhysicalIdentity.matchesIdentity;
import static org.pangrova.table.identity.TablePhysicalIdentity.withIdentity;

/** Owns table identity registration, adoption, retirement and namespace reservation. */
public final class TableRegistrations {
    private static final Logger LOG = LoggerFactory.getLogger(TableRegistrations.class);
    private static final String UNKNOWN_FORMAT = "UNKNOWN";
    private final TableControlStore store;
    private final TableCatalogAuthority catalogAuthority;
    private final TableControlSettings settings;
    private final Clock clock;

    /** Creates table registration operations over the control store. */
    public TableRegistrations(TableControlStore store, TableCatalogAuthority catalogAuthority,
            TableControlSettings settings, Clock clock) {
        this.store = store;
        this.catalogAuthority = catalogAuthority;
        this.settings = settings;
        this.clock = clock;
    }

    /** Rejects a reserved name before any physical CREATE or registration. */
    public void requireCreatableName(String catalog, String schema, String table) {
        store.find(catalog, schema, table).ifPresent(TableRegistrations::requireNoPendingRename);
    }

    private static void requireNoPendingRename(TableControlRecord record) {
        if (record.state() == TableControlState.RENAMING) {
            throw new AccessDeniedException("CREATE rejected: name is reserved by pending rename " + record.tableId());
        }
    }

    /**
     * Records a table created by Pangrova as MANAGED.
     *
     * <p>Identity evidence is optional because connectors are not required to
     * publish it. When evidence is present and disagrees with a live record, the
     * old record is retired and a new identity is registered: drop/recreate under
     * the same name is a different table.</p>
     */
    public Optional<TableControlRecord> registerCreated(
            String catalog, String schema, String table, String ownerPrincipal,
            Optional<TableIdentity> identity, Optional<TableVersionToken> version) {
        if (!settings.enabled()) {
            return Optional.empty();
        }
        return registerCreated(catalog, schema, table, ownerPrincipal, identity, version, catalogAuthority.current(
                catalog));
    }

    private Optional<TableControlRecord> registerCreated(String catalog, String schema, String table,
            String ownerPrincipal, Optional<TableIdentity> identity, Optional<TableVersionToken> version,
            CatalogDeploymentIdentity deployment) {
        Instant now = clock.instant();
        Optional<TableControlRecord> existing = store.find(catalog, schema, table);
        requireDeployment(catalog, deployment);
        existing.ifPresent(TableRegistrations::requireNoPendingRename);
        if (existing.isPresent() && TableCatalogAuthority.matches(existing.get(), deployment)
                && matchesIdentity(existing.get(), identity)) {
            TableControlRecord updated = withIdentity(existing.get(), identity, version, now)
                    .state(TableControlState.ACTIVE)
                    .metadataRevision(existing.get().metadataRevision() + 1)
                    .build();
            store.save(updated);
            return Optional.of(updated);
        }

        TableControlRecord created = withIdentity(newRecord(
                catalog, schema, table, ownerPrincipal, TableControlProvenance.CREATED, now, deployment),
                identity, version, now)
                .build();
        if (existing.isPresent()) {
            store.replaceRegistration(existing.get(), created, now);
        } else {
            store.save(created);
        }
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
    public Optional<TableControlRecord> registerCreatedFromConnector(
            ConnectorMetadata metadata, ConnectorSession session, String catalog, String schema, String table,
            String ownerPrincipal, CatalogDeploymentIdentity deployment) {
        if (!settings.enabled()) {
            return Optional.empty();
        }
        Optional<TableIdentity> identity = Optional.empty();
        try {
            var handle = metadata.getTableHandle(session, new SchemaTableName(schema, table));
            if (handle == null) {
                throw new TableControlException("Created table cannot be resolved for control registration: "
                        + catalog + "." + schema + "." + table);
            }
            identity = metadata.getTableIdentity(session, handle);
            var version = metadata.observeVersion(session, handle);
            return registerCreated(catalog, schema, table, ownerPrincipal, identity, version, deployment);
        } catch (RuntimeException failure) {
            try {
                registerOrphanedManaged(catalog, schema, table, ownerPrincipal, identity, deployment);
            } catch (RuntimeException recoveryFailure) {
                if (failure != recoveryFailure) failure.addSuppressed(recoveryFailure);
            }
            throw failure;
        }
    }

    private void requireDeployment(String catalog, CatalogDeploymentIdentity expected) {
        if (!expected.equals(catalogAuthority.current(catalog))) {
            throw new AccessDeniedException("CREATE rejected: catalog deployment changed for " + catalog);
        }
    }

    /**
     * Adopts an externally written table as {@link OwnershipMode#LINKED}.
     *
     * <p>Identity evidence is mandatory: adopting a name without a format UUID
     * would let a later drop/recreate silently inherit governance and restricted
     * maintenance rights. Re-adopting the same live identity is idempotent, while
     * a changed identity is rejected and must be confirmed by an operator.</p>
     */
    public TableControlRecord adopt(String catalog, String schema, String table,
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
            catalogAuthority.requireCurrent(record, "ADOPT");
            if (record.state() == TableControlState.RENAMING) {
                throw new AccessDeniedException("ADOPT rejected: name is reserved by a pending rename");
            }
            if (record.ownershipMode() == OwnershipMode.MANAGED) {
                throw new AccessDeniedException("ADOPT rejected: " + qualified
                        + " is already MANAGED by Pangrova");
            }
            if (!matchesIdentity(record, identity)) {
                throw new AccessDeniedException("ADOPT rejected: the format identity of "
                        + qualified + " changed since it was adopted (dropped and recreated?); "
                        + "confirm the new table before re-adopting");
            }
            if (record.ownershipMode() == OwnershipMode.LINKED
                    && record.state() == TableControlState.ACTIVE) {
                return record;
            }
            TableControlRecord readopted = withIdentity(record, identity, version, now)
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

        TableControlRecord adopted = withIdentity(newRecord(
                catalog, schema, table, ownerPrincipal, TableControlProvenance.ADOPTED, now,
                        catalogAuthority.current(catalog))
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
    public TableControlRecord unlink(String catalog, String schema, String table) {
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
    public void registerOrphanedManaged(String catalog, String schema, String table,
            String ownerPrincipal, Optional<TableIdentity> identity) {
        if (!settings.enabled()) {
            return;
        }
        registerOrphanedManaged(catalog, schema, table, ownerPrincipal, identity, catalogAuthority.current(catalog));
    }

    private void registerOrphanedManaged(String catalog, String schema, String table, String ownerPrincipal,
            Optional<TableIdentity> identity, CatalogDeploymentIdentity deployment) {
        // A concurrent CREATE may fail registration after another operation reserved this name.
        // Its recovery must never overwrite the rename intent or release the protected source.
        Instant now = clock.instant();
        var existing = store.find(catalog, schema, table);
        requireDeployment(catalog, deployment);
        if (existing.isPresent()) {
            var record = existing.get();
            requireNoPendingRename(record);
            catalogAuthority.requireCurrent(record, "Orphan registration");
            if (record.state() != TableControlState.ORPHANED_MANAGED
                    || record.ownershipMode() != OwnershipMode.MANAGED
                    || !java.util.Objects.equals(record.ownerPrincipal(), ownerPrincipal)
                    || !matchesIdentity(record, identity)) {
                throw new AccessDeniedException("Orphan registration cannot overwrite the existing owner of "
                        + record.qualifiedName());
            }
            return;
        }
        var orphaned = withIdentity(newRecord(catalog, schema, table, ownerPrincipal,
                TableControlProvenance.CREATED, now, deployment), identity, Optional.empty(), now)
                .state(TableControlState.ORPHANED_MANAGED)
                .build();
        store.save(orphaned);
    }

    /** Retires only the control record captured before the physical DROP started. */
    public void markDropped(TableControlRecord expected) {
        if (!settings.enabled()) {
            return;
        }
        TableControlRecord current = store.findById(expected.tableId()).orElse(null);
        if (current == null || current.metadataRevision() != expected.metadataRevision()
                || current.state() == TableControlState.DROPPED) {
            throw new AccessDeniedException("DROP TABLE completed with a changed control record for "
                    + expected.qualifiedName() + "; refusing to retire a newer table registration");
        }
        catalogAuthority.requireCurrent(current, "DROP TABLE completion");
        store.save(current.retiredAt(clock.instant()));
    }

    /** Registers write ownership for an explicitly allowlisted legacy table. */
    public TableControlRecord registerAllowlisted(String catalog, String schema, String table) {
        if (!settings.enabled() || !settings.isLegacyWriteAllowlisted(catalog + "." + schema + "." + table)) {
            throw new AccessDeniedException("Explicit legacy write ownership is required");
        }
        Instant now = clock.instant();
        TableControlRecord legacy = newRecord(catalog, schema, table, "", TableControlProvenance.CREATED, now,
                catalogAuthority.current(catalog))
                .format(UNKNOWN_FORMAT)
                .properties(Map.of("legacy-write-allowlist", "true"))
                .build();
        store.save(legacy);
        LOG.info("event=table_control_legacy_allowlisted table={}.{}.{} tableId={}",
                catalog, schema, table, legacy.tableId());
        return legacy;
    }

    private static TableControlRecord.Builder newRecord(String catalog, String schema, String table,
            String ownerPrincipal, TableControlProvenance provenance, Instant now,
                    CatalogDeploymentIdentity deployment) {
        return TableControlRecord.builder()
                .tableId(UUID.randomUUID())
                .name(catalog, schema, table)
                .catalogIncarnationId(deployment.incarnationId())
                .catalogRevision(deployment.revision())
                .format(UNKNOWN_FORMAT)
                .ownershipMode(OwnershipMode.MANAGED)
                .state(TableControlState.ACTIVE)
                .provenance(provenance)
                .metadataRevision(1)
                .ownerPrincipal(ownerPrincipal)
                .createdAt(now)
                .updatedAt(now);
    }

}
