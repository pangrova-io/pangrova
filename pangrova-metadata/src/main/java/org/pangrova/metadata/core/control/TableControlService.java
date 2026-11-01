package org.pangrova.metadata.core.control;

import org.pangrova.metadata.api.control.OwnershipMode;

import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;

/**
 * Ownership, identity, and admission decisions for the table control plane.
 *
 * <p>Catalog visibility is not ownership. A table with no control record is
 * {@link OwnershipMode#OBSERVED}: it can be queried but never written, dropped,
 * or maintained. {@link OwnershipMode#LINKED} and {@link OwnershipMode#MANAGED}
 * are the only persisted states, and only MANAGED permits DML and destructive
 * DDL.</p>
 *
 * <p>The service owns version observation and catalog-deployment fencing and
 * composes two package-private collaborators: {@link TableControlLifecycle}
 * for registration/retirement/rename transitions and {@link TableControlAdmission}
 * for write and DDL admission.</p>
 */
public final class TableControlService {

    private static final Logger LOG = LoggerFactory.getLogger(TableControlService.class);

    private final TableControlStore store;
    private final Function<String, CatalogDeploymentIdentity> catalogs;
    private final TableControlSettings settings;
    private final Clock clock;
    private final TableControlLifecycle lifecycle;
    private final TableControlAdmission admission;

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
        this.lifecycle = new TableControlLifecycle(store, catalogs, settings, clock);
        this.admission = new TableControlAdmission(store, catalogs, settings, clock);
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
        TableControlSupport.requireCurrentCatalog(catalogs, current, "Version observation");
        boolean recovering = current.state() == TableControlState.STALE
                || current.state() == TableControlState.ORPHANED_MANAGED;
        if (recovering && identity.filter(TableIdentity::hasFormatUuid).isEmpty()) {
            // Existence and a version token do not prove that this is still the
            // physical table whose ownership was registered.
            return ObservationOutcome.UNSUPPORTED;
        }
        IdentityFence fenced = fenceIdentity(current, identity, observedAt);
        if (fenced.outcome() != null) {
            return fenced.outcome();
        }
        current = fenced.record();
        return observeVersionChange(current, identity, version, recovering, source, observedAt);
    }

    /**
     * Records the connector-reported version against the control record. A
     * changed token advances the control revision and records the transition;
     * peers discard cached resolutions because the registry revision and
     * version token no longer match.
     */
    private ObservationOutcome observeVersionChange(TableControlRecord current,
            Optional<TableIdentity> identity, Optional<TableVersionToken> version,
            boolean recovering, String source, Instant observedAt) {
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
     * Repairs a missing registered identity or quarantines a changed one.
     *
     * <p>A legacy-allowlisted record may bind its first reported identity when
     * the evidence matches. A recorded identity that disagrees with live
     * evidence quarantines the record so writes and maintenance stop until an
     * operator confirms the replacement.</p>
     *
     * @return the possibly repaired record and a terminal outcome, or {@code null}
     *         when observation should continue
     */
    private IdentityFence fenceIdentity(TableControlRecord current, Optional<TableIdentity> identity,
            Instant observedAt) {
        if (identity.isPresent() && identity.get().hasFormatUuid()
                && !TableControlSupport.hasFormatUuid(current)) {
            if (!settings.isLegacyWriteAllowlisted(current.qualifiedName())) {
                // A current identity cannot prove ownership of a table whose
                // original identity was never recorded.
                return new IdentityFence(current, ObservationOutcome.UNSUPPORTED);
            }
            TableControlRecord repaired = TableControlSupport.withIdentity(
                    current, identity, Optional.empty(), observedAt)
                    .metadataRevision(current.metadataRevision() + 1)
                    .build();
            store.save(repaired);
            LOG.info("event=table_control_identity_repaired table={} tableId={} formatUuid={}",
                    repaired.qualifiedName(), repaired.tableId(), repaired.formatTableUuid());
            current = repaired;
        }
        if (identity.isPresent() && identity.get().hasFormatUuid()
                && !TableControlSupport.matchesIdentity(current, identity)) {
            store.save(current.toBuilder()
                    .state(TableControlState.QUARANTINED)
                    .metadataRevision(current.metadataRevision() + 1)
                    .updatedAt(observedAt)
                    .build());
            LOG.error("event=table_control_identity_changed table={} tableId={} "
                            + "registeredUuid={} observedUuid={}",
                    current.qualifiedName(), current.tableId(), current.formatTableUuid(),
                    identity.get().formatTableUuid());
            return new IdentityFence(current, ObservationOutcome.IDENTITY_CHANGED);
        }
        return new IdentityFence(current, null);
    }

    /** Result of the identity fence: the record to observe and an optional terminal outcome. */
    private record IdentityFence(TableControlRecord record, ObservationOutcome outcome) {}

    /**
     * Returns a STALE or ORPHANED_MANAGED record to ACTIVE once the connector
     * proves the table still exists with a matching identity. Recovery keeps the
     * recorded version when the connector reports the same one (or none) and
     * refreshes identity evidence that was missing at registration time.
     */
    private ObservationOutcome recover(TableControlRecord current,
            Optional<TableIdentity> identity, String versionToken, Instant observedAt) {
        TableControlRecord.Builder updated = TableControlSupport.withIdentity(current.toBuilder()
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
        TableControlSupport.requireCurrentCatalog(catalogs, current, "Missing-table observation");
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
        admission.requireCreatableName(catalog, schema, table);
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
        return lifecycle.registerCreated(catalog, schema, table, ownerPrincipal, identity, version);
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
        return lifecycle.registerCreatedFromConnector(metadata, session, catalog, schema, table, ownerPrincipal);
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
        return lifecycle.adopt(catalog, schema, table, ownerPrincipal, identity, version);
    }

    /**
     * Removes control state for a LINKED table. The physical table is untouched;
     * the table returns to OBSERVED and becomes read-only for Pangrova again.
     */
    public TableControlRecord unlink(String catalog, String schema, String table) {
        return lifecycle.unlink(catalog, schema, table);
    }

    /** Marks a physically created table whose control record could not be completed. */
    public void registerOrphanedManaged(String catalog, String schema, String table,
            String ownerPrincipal, Optional<TableIdentity> identity) {
        lifecycle.registerOrphanedManaged(catalog, schema, table, ownerPrincipal, identity);
    }

    /** Retires only the control record captured before the physical DROP started. */
    public void markDropped(TableControlRecord expected) {
        lifecycle.markDropped(expected);
    }

    /**
     * Moves the control record with an in-place rename so ownership survives the
     * move. A live record already registered under the target name blocks the
     * move instead of being overwritten: two tables cannot share a name, and
     * silently replacing the target's ownership would strand its governance.
     */
    public void rename(String catalog, String schema, String table,
            String newSchema, String newTable) {
        lifecycle.rename(catalog, schema, table, newSchema, newTable);
    }

    /** Moves only the exact captured registration, including when rolling back a failed rename. */
    public TableControlRecord rename(TableControlRecord expected, String newSchema, String newTable) {
        return lifecycle.rename(expected, newSchema, newTable);
    }

    /** Records physical identity changes caused by a successfully completed, admitted rename. */
    public void completeRename(TableControlRecord expected, Optional<TableIdentity> identity,
            Optional<TableVersionToken> version) {
        lifecycle.completeRename(expected, identity, version);
    }

    /** Rejects DML before it reaches the connector unless the table is MANAGED and ACTIVE. */
    public void requireManagedForWrite(String catalog, String schema, String table, String operation) {
        admission.requireManagedForWrite(catalog, schema, table, operation);
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
        admission.requireManagedForWrite(catalog, schema, table, operation, observedIdentity);
    }

    /** True when admission decisions are enforced, not merely recorded in shadow mode. */
    public boolean enforcing() {
        return admission.enforcing();
    }

    /** Fences a controlled operation against retirement, concurrent changes and physical replacement. */
    public void requireActiveIdentity(TableControlRecord expected, Optional<TableIdentity> identity,
            String operation) {
        admission.requireActiveIdentity(expected, identity, operation);
    }

    /** Fences read admission while allowing a selected immutable snapshot to predate a data-version observation. */
    public void requireReadIdentity(TableControlRecord expected, Optional<TableIdentity> identity, String operation) {
        admission.requireReadIdentity(expected, identity, operation);
    }

    /** Revalidates a cached binding's captured owner without treating old evidence as a new observation. */
    public void requireReadOwner(TableControlRecord expected, String operation) {
        admission.requireReadOwner(expected, operation);
    }

    /** Rejects destructive DDL (DROP/ALTER) unless the table is MANAGED and ACTIVE. */
    public void requireManagedForDdl(String catalog, String schema, String table, String operation) {
        admission.requireManagedForDdl(catalog, schema, table, operation);
    }

    /** Checks ownership and the physical identity resolved for destructive DDL. */
    public void requireManagedForDdl(String catalog, String schema, String table, String operation,
            Optional<TableIdentity> observedIdentity) {
        admission.requireManagedForDdl(catalog, schema, table, operation, observedIdentity);
    }

    /**
     * Admits DROP TABLE. An ORPHANED_MANAGED record is droppable because the
     * physical table was created but the control record could not be completed:
     * refusing DROP would leave an unusable table that cannot be cleaned up.
     */
    public void requireDroppable(String catalog, String schema, String table, String operation) {
        admission.requireDroppable(catalog, schema, table, operation);
    }

    /** Allows orphan cleanup only after checking any recorded physical identity. */
    public void requireDroppable(String catalog, String schema, String table, String operation,
            Optional<TableIdentity> observedIdentity) {
        admission.requireDroppable(catalog, schema, table, operation, observedIdentity);
    }

    /** Rejects authority captured for another deployment, even if the format UUID is unchanged. */
    public void requireCurrentCatalog(TableControlRecord record, String operation) {
        TableControlSupport.requireCurrentCatalog(catalogs, record, operation);
    }

    private TableControlRecord backfillCatalogDeployment(TableControlRecord record,
            Optional<TableIdentity> identity, Instant observedAt) {
        if (record.catalogIncarnationId() != null || record.catalogRevision() != null
                || !settings.isLegacyWriteAllowlisted(record.qualifiedName())
                || !TableControlSupport.hasFormatUuid(record)
                || identity.filter(value -> value.hasFormatUuid()
                        && TableControlSupport.sameIdentity(record, value)).isEmpty()) {
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
}
