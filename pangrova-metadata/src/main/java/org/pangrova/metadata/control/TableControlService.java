package org.pangrova.metadata.control;

import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
import org.pangrova.spi.security.AccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

/**
 * Ownership, identity, and admission decisions for the table control plane.
 *
 * <p>Catalog visibility is not ownership. A table with no control record is
 * {@link OwnershipMode#OBSERVED}: it can be queried but never written, dropped,
 * or maintained. {@link OwnershipMode#LINKED} and {@link OwnershipMode#MANAGED}
 * are the only persisted states, and only MANAGED permits DML and destructive
 * DDL.</p>
 */
public final class TableControlService {

    private static final Logger LOG = LoggerFactory.getLogger(TableControlService.class);
    private static final String LEGACY_FORMAT = "UNKNOWN";

    private final TableControlStore store;
    private final Function<String, CatalogDeploymentIdentity> catalogs;
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
        this.catalogs = java.util.Objects.requireNonNull(catalogs, "catalogs");
        this.settings = settings;
        this.clock = clock;
    }

    public TableControlSettings settings() {
        return settings;
    }

    public boolean enabled() {
        return settings.enabled();
    }

    public Optional<TableControlRecord> find(String catalog, String schema, String table) {
        return store.find(catalog, schema, table);
    }

    /** Resolves the original identity even after rename or retirement, for scoped cleanup. */
    public Optional<TableControlRecord> findById(java.util.UUID tableId) {
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

    /**
     * Reconciles one controlled table against the connector's live state.
     *
     * <p>A changed format identity quarantines the record: the name now points at
     * a different physical table, so writes and maintenance must stop until an
     * operator confirms the replacement. A changed version advances the control
     * revision and records the transition; peers discard cached resolutions
     * because the registry revision and version token no longer match.</p>
     *
     * <p>OBSERVED tables never reach this method. They have no persisted identity
     * and are validated at each resolution instead.</p>
     */
    public ObservationOutcome observeVersion(TableControlRecord record,
            Optional<TableIdentity> identity, Optional<TableVersionToken> version,
            String source, Instant observedAt) {
        if (!settings.enabled() || record == null) {
            return ObservationOutcome.IGNORED;
        }
        TableControlRecord current = store.findById(record.tableId()).orElse(null);
        if (current == null || current.state() == TableControlState.DROPPED
                || current.state() == TableControlState.RENAMING
                || current.state() == TableControlState.QUARANTINED
                || current.metadataRevision() != record.metadataRevision()) {
            return ObservationOutcome.IGNORED;
        }
        current = backfillCatalogDeployment(current, identity, observedAt);
        requireCurrentCatalog(current, "Version observation");
        boolean recovering = current.state() == TableControlState.STALE
                || current.state() == TableControlState.ORPHANED_MANAGED;
        if (recovering && identity.filter(TableIdentity::hasFormatUuid).isEmpty()) {
            // Existence and a version token do not prove that this is still the
            // physical table whose ownership was registered.
            return ObservationOutcome.UNSUPPORTED;
        }
        if (identity.isPresent() && identity.get().hasFormatUuid()
                && !hasFormatUuid(current)) {
            if (!settings.isLegacyWriteAllowlisted(current.qualifiedName())) {
                // A current identity cannot prove ownership of a table whose
                // original identity was never recorded.
                return ObservationOutcome.UNSUPPORTED;
            }
            current = withIdentity(current, identity, Optional.empty(), observedAt)
                    .metadataRevision(current.metadataRevision() + 1)
                    .build();
            store.save(current);
            LOG.info("event=table_control_identity_repaired table={} tableId={} formatUuid={}",
                    current.qualifiedName(), current.tableId(),
                    current.formatTableUuid());
        }
        if (identity.isPresent() && identity.get().hasFormatUuid()
                && !matchesIdentity(current, identity)) {
            store.save(current.toBuilder()
                    .state(TableControlState.QUARANTINED)
                    .metadataRevision(current.metadataRevision() + 1)
                    .updatedAt(observedAt)
                    .build());
            LOG.error("event=table_control_identity_changed table={} tableId={} "
                            + "registeredUuid={} observedUuid={}",
                    current.qualifiedName(), current.tableId(), current.formatTableUuid(),
                    identity.get().formatTableUuid());
            return ObservationOutcome.IDENTITY_CHANGED;
        }
        if (version.isEmpty()) {
            return recovering
                    ? recover(current, identity, null, observedAt)
                    : ObservationOutcome.UNSUPPORTED;
        }
        String observedToken = version.get().token();
        if (observedToken.equals(current.versionToken())) {
            return recovering
                    ? recover(current, identity, observedToken, observedAt)
                    : ObservationOutcome.UNCHANGED;
        }
        TableControlRecord updated = current.toBuilder()
                .state(TableControlState.ACTIVE)
                .version(observedToken, observedAt)
                .metadataRevision(current.metadataRevision() + 1)
                .updatedAt(observedAt)
                .lastSyncedAt(observedAt)
                .build();
        store.saveWithObservation(updated, new TableVersionObservation(
                current.tableId(), observedToken, current.versionToken(), source, observedAt));
        LOG.info("event=table_control_version_observed table={} tableId={} "
                        + "previousVersion={} observedVersion={} revision={}",
                current.qualifiedName(), current.tableId(), current.versionToken(),
                observedToken, updated.metadataRevision());
        return ObservationOutcome.VERSION_CHANGED;
    }

    /**
     * Returns a STALE or ORPHANED_MANAGED record to ACTIVE once the connector
     * proves the table still exists with a matching identity. Recovery keeps the
     * recorded version when the connector reports the same one (or none) and
     * refreshes identity evidence that was missing at registration time.
     */
    private ObservationOutcome recover(TableControlRecord current,
            Optional<TableIdentity> identity, String versionToken, Instant observedAt) {
        TableControlRecord.Builder updated = withIdentity(current.toBuilder()
                .state(TableControlState.ACTIVE)
                .metadataRevision(current.metadataRevision() + 1)
                .updatedAt(observedAt)
                .lastSyncedAt(observedAt), identity, Optional.empty(), observedAt);
        if (versionToken != null) {
            updated.version(versionToken, observedAt);
        }
        store.save(updated.build());
        LOG.info("event=table_control_recovered table={} tableId={} previousState={}",
                current.qualifiedName(), current.tableId(), current.state());
        return versionToken == null
                ? ObservationOutcome.UNSUPPORTED
                : ObservationOutcome.UNCHANGED;
    }

    /**
     * Marks a controlled table the catalog can no longer resolve as STALE.
     * Discovery failures are handled by the caller and must not reach this method:
     * an unreachable catalog is not evidence that a table was dropped.
     */
    public ObservationOutcome markUnavailable(TableControlRecord observedRecord, Instant observedAt) {
        if (!settings.enabled()) {
            return ObservationOutcome.IGNORED;
        }
        TableControlRecord current = store.findById(observedRecord.tableId()).orElse(null);
        if (current == null || current.state() == TableControlState.DROPPED
                || current.state() == TableControlState.RENAMING
                || current.metadataRevision() != observedRecord.metadataRevision()) {
            return ObservationOutcome.IGNORED;
        }
        if (current.state() == TableControlState.STALE) {
            return ObservationOutcome.UNCHANGED;
        }
        requireCurrentCatalog(current, "Missing-table observation");
        if (current.state() == TableControlState.QUARANTINED
                || current.state() == TableControlState.ORPHANED_MANAGED) {
            return ObservationOutcome.IGNORED;
        }
        store.save(current.toBuilder()
                .state(TableControlState.STALE)
                .metadataRevision(current.metadataRevision() + 1)
                .updatedAt(observedAt)
                .build());
        LOG.warn("event=table_control_stale table={} tableId={}",
                current.qualifiedName(), current.tableId());
        return ObservationOutcome.TABLE_MISSING;
    }

    /** Rejects a reserved name before any physical CREATE or registration. */
    public void requireCreatableName(String catalog, String schema, String table) {
        store.find(catalog, schema, table).filter(record -> record.state() == TableControlState.RENAMING)
                .ifPresent(record -> {
                    throw new AccessDeniedException("CREATE rejected: name is reserved by pending rename "
                            + record.tableId());
                });
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
        requireCreatableName(catalog, schema, table);
        Instant now = clock.instant();
        Optional<TableControlRecord> existing = store.find(catalog, schema, table);
        if (existing.isPresent() && matchesCatalog(existing.get(), catalogs.apply(catalog))
                && matchesIdentity(existing.get(), identity)) {
            TableControlRecord updated = withIdentity(existing.get(), identity, version, now)
                    .state(TableControlState.ACTIVE)
                    .metadataRevision(existing.get().metadataRevision() + 1)
                    .build();
            store.save(updated);
            return Optional.of(updated);
        }
        existing.ifPresent(record -> retire(record, now));

        TableControlRecord created = withIdentity(newRecord(
                catalog, schema, table, ownerPrincipal, TableControlProvenance.CREATED, now),
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
    public Optional<TableControlRecord> registerCreatedFromConnector(
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
            throw new org.pangrova.metadata.exception.MetastoreException(
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
            requireCurrentCatalog(record, "ADOPT");
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
                catalog, schema, table, ownerPrincipal, TableControlProvenance.ADOPTED, now)
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
        Instant now = clock.instant();
        TableControlRecord orphaned = store.find(catalog, schema, table)
                .map(record -> {
                    requireCurrentCatalog(record, "Orphan registration");
                    return withIdentity(record, identity, Optional.empty(), now)
                        .state(TableControlState.ORPHANED_MANAGED)
                        .metadataRevision(record.metadataRevision() + 1)
                        .build();
                })
                .orElseGet(() -> withIdentity(newRecord(
                        catalog, schema, table, ownerPrincipal,
                        TableControlProvenance.CREATED, now), identity, Optional.empty(), now)
                        .state(TableControlState.ORPHANED_MANAGED)
                        .build());
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
        requireCurrentCatalog(current, "DROP TABLE completion");
        retire(current, clock.instant());
    }

    /**
     * Moves the control record with an in-place rename so ownership survives the
     * move. A live record already registered under the target name blocks the
     * move instead of being overwritten: two tables cannot share a name, and
     * silently replacing the target's ownership would strand its governance.
     */
    public void rename(String catalog, String schema, String table,
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
    public TableControlRecord rename(TableControlRecord expected, String newSchema, String newTable) {
        if (!settings.enabled()) {
            return expected;
        }
        TableControlRecord source = store.findById(expected.tableId()).orElse(null);
        if (source == null || source.metadataRevision() != expected.metadataRevision()
                || source.state() == TableControlState.DROPPED) {
            throw new AccessDeniedException("RENAME rejected: control record changed for "
                    + expected.qualifiedName() + "; refusing to move a newer table registration");
        }
        requireCurrentCatalog(source, "RENAME");
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
    public void completeRename(TableControlRecord expected, Optional<TableIdentity> identity,
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
        requireCurrentCatalog(current, "RENAME completion");
        Instant now = clock.instant();
        String token = version.map(TableVersionToken::token).orElse(null);
        TableControlRecord completed = withIdentity(current, Optional.of(physical), version, now)
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
        requireCurrentCatalog(record, operation);
        return record;
    }

    private TableControlRecord registerLegacyManaged(String catalog, String schema, String table) {
        Instant now = clock.instant();
        TableControlRecord legacy = newRecord(catalog, schema, table, "", TableControlProvenance.CREATED, now)
                .format(LEGACY_FORMAT)
                .properties(Map.of("legacy-write-allowlist", "true"))
                .build();
        store.save(legacy);
        LOG.info("event=table_control_legacy_allowlisted table={}.{}.{} tableId={}",
                catalog, schema, table, legacy.tableId());
        return legacy;
    }

    private static boolean matchesIdentity(TableControlRecord record,
            Optional<TableIdentity> identity) {
        if (identity.isEmpty() || !identity.get().hasFormatUuid()) {
            return true;
        }
        if (!hasFormatUuid(record)) {
            return true;
        }
        return sameIdentity(record, identity.get());
    }

    private static boolean sameIdentity(TableControlRecord record, TableIdentity identity) {
        return record.format().equals(identity.format())
                && record.formatTableUuid().equals(identity.formatTableUuid())
                && java.util.Objects.equals(record.formatLocation(), identity.location());
    }

    private static boolean hasFormatUuid(TableControlRecord record) {
        return record.formatTableUuid() != null && !record.formatTableUuid().isBlank();
    }

    private static TableControlRecord.Builder withIdentity(TableControlRecord.Builder builder,
            Optional<TableIdentity> identity, Optional<TableVersionToken> version, Instant now) {
        identity.ifPresent(value -> builder
                .format(value.format())
                .formatTableUuid(value.formatTableUuid())
                .formatLocation(value.location())
                .sourceSignature(sourceSignature(value)));
        if (version.isPresent()) {
            builder.version(version.get().token(), now);
        }
        return builder;
    }

    private static TableControlRecord.Builder withIdentity(TableControlRecord record,
            Optional<TableIdentity> identity, Optional<TableVersionToken> version, Instant now) {
        return withIdentity(record.toBuilder().updatedAt(now), identity, version, now);
    }

    private TableControlRecord.Builder newRecord(String catalog, String schema, String table,
            String ownerPrincipal, TableControlProvenance provenance, Instant now) {
        CatalogDeploymentIdentity deployment = catalogs.apply(catalog);
        return TableControlRecord.builder()
                .tableId(UUID.randomUUID())
                .name(catalog, schema, table)
                .catalogIncarnationId(deployment.incarnationId())
                .catalogRevision(deployment.revision())
                .format(LEGACY_FORMAT)
                .ownershipMode(OwnershipMode.MANAGED)
                .state(TableControlState.ACTIVE)
                .provenance(provenance)
                .metadataRevision(1)
                .ownerPrincipal(ownerPrincipal)
                .createdAt(now)
                .updatedAt(now);
    }

    /** Rejects authority captured for another deployment, even if the format UUID is unchanged. */
    public void requireCurrentCatalog(TableControlRecord record, String operation) {
        if (!matchesCatalog(record, catalogs.apply(record.catalog()))) {
            throw new AccessDeniedException(operation + " rejected: catalog deployment identity changed or "
                    + "was never registered for " + record.qualifiedName()
                    + "; explicitly reconcile the control record before using its authority");
        }
    }

    private static boolean matchesCatalog(TableControlRecord record, CatalogDeploymentIdentity deployment) {
        return deployment.incarnationId().equals(record.catalogIncarnationId())
                && java.util.Objects.equals(deployment.revision(), record.catalogRevision());
    }

    private TableControlRecord backfillCatalogDeployment(TableControlRecord record,
            Optional<TableIdentity> identity, Instant observedAt) {
        if (record.catalogIncarnationId() != null || record.catalogRevision() != null
                || !settings.isLegacyWriteAllowlisted(record.qualifiedName())
                || !hasFormatUuid(record)
                || identity.filter(value -> value.hasFormatUuid() && sameIdentity(record, value)).isEmpty()) {
            return record;
        }
        CatalogDeploymentIdentity deployment = catalogs.apply(record.catalog());
        TableControlRecord bound = record.toBuilder()
                .catalogIncarnationId(deployment.incarnationId())
                .catalogRevision(deployment.revision())
                .metadataRevision(record.metadataRevision() + 1)
                .updatedAt(observedAt)
                .build();
        store.save(bound);
        LOG.info("event=table_control_catalog_backfilled table={} tableId={} incarnation={} revision={}",
                bound.qualifiedName(), bound.tableId(), bound.catalogIncarnationId(), bound.catalogRevision());
        return bound;
    }

    private void retire(TableControlRecord record, Instant now) {
        TableControlRecord dropped = record.toBuilder()
                .state(TableControlState.DROPPED)
                .metadataRevision(record.metadataRevision() + 1)
                .updatedAt(now)
                .build();
        store.save(dropped);
    }

    private static String sourceSignature(TableIdentity identity) {
        return identity.format() + '|' + identity.formatTableUuid() + '|' + identity.location();
    }
}
