package org.pangrova.table.ownership;

import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.ownership.TableControlStore;
import org.pangrova.table.api.ownership.TableControlView;
import org.pangrova.table.api.ownership.TableReadAuthority;
import org.pangrova.table.config.TableControlSettings;
import org.pangrova.table.identity.TableCatalogAuthority;
import org.pangrova.table.observation.TableVersionObservations;
import org.pangrova.table.rename.TableRenames;
import org.pangrova.table.registration.TableRegistrations;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static org.pangrova.table.identity.TablePhysicalIdentity.hasFormatUuid;
import static org.pangrova.table.identity.TablePhysicalIdentity.sameIdentity;
import static org.pangrova.table.identity.TablePhysicalIdentity.withIdentity;

/**
 * Ownership, identity, and admission decisions for the table control plane.
 *
 * <p>Catalog visibility is not ownership. A table with no control record is
 * {@link OwnershipMode#OBSERVED}: it can be queried but never written, dropped,
 * or maintained. {@link OwnershipMode#LINKED} and {@link OwnershipMode#MANAGED}
 * are the only persisted states, and only MANAGED permits DML and destructive
 * DDL.</p>
 */
public final class TableControlService implements TableReadAuthority {

    private final TableControlStore store;
    private final TableCatalogAuthority catalogAuthority;
    private final TableRegistrations registrations;
    private final TableRenames renames;
    private final TableVersionObservations observations;
    private final TableControlSettings settings;
    private final Clock clock;

    public TableControlService(TableControlStore store, Function<String, CatalogDeploymentIdentity> catalogs) {
        this(store, catalogs, TableControlSettings.defaults(), Clock.systemUTC());
    }

    public TableControlService(TableControlStore store, Function<String, CatalogDeploymentIdentity> catalogs,
            TableControlSettings settings) {
        this(store, catalogs, settings, Clock.systemUTC());
    }

    /** Creates a control authority using live catalog identities and the supplied observation clock. */
    public TableControlService(TableControlStore store, Function<String, CatalogDeploymentIdentity> catalogs,
            TableControlSettings settings, Clock clock) {
        this.store = store;
        this.catalogAuthority = new TableCatalogAuthority(catalogs);
        this.observations = new TableVersionObservations(store, catalogAuthority, settings);
        this.registrations = new TableRegistrations(store, catalogAuthority, settings, clock);
        this.renames = new TableRenames(store, catalogAuthority, settings, clock);
        this.settings = settings;
        this.clock = clock;
    }

    public TableVersionObservations observations() {
        return observations;
    }

    public TableRegistrations registrations() {
        return registrations;
    }

    public TableRenames renames() {
        return renames;
    }

    public TableControlSettings settings() {
        return settings;
    }

    @Override
    public boolean enabled() {
        return settings.enabled();
    }

    @Override
    public Optional<TableControlRecord> find(String catalog, String schema, String table) {
        return store.find(catalog, schema, table);
    }

    /** Resolves the original identity even after rename or retirement, for scoped cleanup. */
    public Optional<TableControlRecord> findById(UUID tableId) {
        return store.findById(tableId);
    }

    /** Ownership view for table resolution. Never persists anything. */
    public TableControlView describe(String catalog, String schema, String table) {
        return store.find(catalog, schema, table)
                .map(record -> {
                    requireCurrentCatalog(record, "Table resolution");
                    return record;
                })
                .map(TableControlView::of)
                .orElseGet(TableControlView::observed);
    }

    /** Live (non-retired) control records for observation and maintenance. */
    public java.util.List<TableControlRecord> listLive(String catalog) {
        return store.listByCatalog(catalog).stream()
                .filter(record -> record.state() != TableControlState.DROPPED)
                .toList();
    }

    /** Live (non-retired) control records across all catalogs. */
    public java.util.List<TableControlRecord> listLive() {
        return store.listAll().stream()
                .filter(record -> record.state() != TableControlState.DROPPED)
                .toList();
    }

    /** Rejects DML before it reaches the connector unless the table is MANAGED and ACTIVE. */
    public void requireManagedForWrite(String catalog, String schema, String table, String operation) {
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
    public void requireManagedForWrite(String catalog, String schema, String table,
            String operation, Optional<TableIdentity> observedIdentity) {
        TableControlRecord record = requireManaged(catalog, schema, table, operation);
        requireMatchingIdentity(record, operation, observedIdentity);
    }

    private void requireMatchingIdentity(TableControlRecord record, String operation,
            Optional<TableIdentity> observedIdentity) {
        if (record == null) {
            return;
        }
        requireCurrentCatalog(record, operation);
        Optional<TableIdentity> evidence = observedIdentity
                .filter(TableIdentity::hasFormatUuid);
        if (!hasFormatUuid(record)) {
            if (settings.isLegacyWriteAllowlisted(record.qualifiedName()) && evidence.isPresent()) {
                store.save(withIdentity(record, evidence, Optional.empty(), clock.instant())
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
        if (sameIdentity(record, evidence.get())) {
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
    public boolean enforcing() {
        return settings.enabled() && settings.externalReadOnly();
    }

    /** Fences a controlled operation against retirement, concurrent changes and physical replacement. */
    public void requireActiveIdentity(TableControlRecord expected, Optional<TableIdentity> identity,
            String operation) {
        TableControlRecord current = store.findById(expected.tableId()).orElse(null);
        if (current == null || current.state() != TableControlState.ACTIVE
                || current.metadataRevision() != expected.metadataRevision()) {
            throw new AccessDeniedException(operation + " rejected: control record changed for "
                    + expected.qualifiedName() + "; resolve the table again");
        }
        requireCurrentCatalog(current, operation);
        if (!hasFormatUuid(current) || identity.filter(TableIdentity::hasFormatUuid).isEmpty()) {
            throw new AccessDeniedException(operation + " rejected: missing format identity for "
                    + expected.qualifiedName());
        }
        requireMatchingIdentity(current, operation, identity);
    }

    /** Fences read admission while allowing a selected immutable snapshot to predate a data-version observation. */
    @Override
    public void requireReadIdentity(TableControlRecord expected, Optional<TableIdentity> identity, String operation) {
        TableControlRecord current = currentReadOwner(expected, operation);
        if (!hasFormatUuid(current) || identity.filter(TableIdentity::hasFormatUuid).isEmpty()) {
            throw new AccessDeniedException(operation + " rejected: missing format identity for "
                    + expected.qualifiedName());
        }
        requireMatchingIdentity(current, operation, identity);
    }

    /** Revalidates a cached binding's captured owner without treating old evidence as a new observation. */
    @Override
    public void requireReadOwner(TableControlRecord expected, String operation) {
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
        requireCurrentCatalog(current, operation);
        return current;
    }

    /** Rejects destructive DDL (DROP/ALTER) unless the table is MANAGED and ACTIVE. */
    public void requireManagedForDdl(String catalog, String schema, String table, String operation) {
        requireManaged(catalog, schema, table, operation, false);
    }

    /** Checks ownership and the physical identity resolved for destructive DDL. */
    public void requireManagedForDdl(String catalog, String schema, String table, String operation,
            Optional<TableIdentity> observedIdentity) {
        requireMatchingIdentity(requireManaged(catalog, schema, table, operation, false),
                operation, observedIdentity);
    }

    /**
     * Admits DROP TABLE. An ORPHANED_MANAGED record is droppable because the
     * physical table was created but the control record could not be completed:
     * refusing DROP would leave an unusable table that cannot be cleaned up.
     */
    public void requireDroppable(String catalog, String schema, String table, String operation) {
        requireManaged(catalog, schema, table, operation, true);
    }

    /** Allows orphan cleanup only after checking any recorded physical identity. */
    public void requireDroppable(String catalog, String schema, String table, String operation,
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
                return registrations.registerAllowlisted(catalog, schema, table);
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
        requireCurrentCatalog(record, operation);
        return record;
    }

    /** Rejects authority captured for another deployment, even if the format UUID is unchanged. */
    @Override
    public void requireCurrentCatalog(TableControlRecord record, String operation) {
        catalogAuthority.requireCurrent(record, operation);
    }

}
