package org.pangrova.table.observation;

import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableVersionToken;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.api.enums.ObservationOutcome;
import org.pangrova.table.api.observation.TableVersionObservation;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.ownership.TableControlStore;
import org.pangrova.table.config.TableControlSettings;
import org.pangrova.table.identity.TableCatalogAuthority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;

import static org.pangrova.table.identity.TablePhysicalIdentity.hasFormatUuid;
import static org.pangrova.table.identity.TablePhysicalIdentity.matchesIdentity;
import static org.pangrova.table.identity.TablePhysicalIdentity.sameIdentity;
import static org.pangrova.table.identity.TablePhysicalIdentity.withIdentity;

/** Applies connector evidence to the captured control revision and persists its version transition. */
public final class TableVersionObservations {
    private static final Logger LOG = LoggerFactory.getLogger(TableVersionObservations.class);

    private final TableControlStore store;
    private final TableCatalogAuthority catalogAuthority;
    private final TableControlSettings settings;

    public TableVersionObservations(TableControlStore store, TableCatalogAuthority catalogAuthority,
            TableControlSettings settings) {
        this.store = store;
        this.catalogAuthority = catalogAuthority;
        this.settings = settings;
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
        catalogAuthority.requireCurrent(current, "Version observation");
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
        catalogAuthority.requireCurrent(current, "Missing-table observation");
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

    private TableControlRecord backfillCatalogDeployment(TableControlRecord record,
            Optional<TableIdentity> identity, Instant observedAt) {
        if (record.catalogIncarnationId() != null || record.catalogRevision() != null
                || !settings.isLegacyWriteAllowlisted(record.qualifiedName())
                || !hasFormatUuid(record)
                || identity.filter(value -> value.hasFormatUuid() && sameIdentity(record, value)).isEmpty()) {
            return record;
        }
        CatalogDeploymentIdentity deployment = catalogAuthority.current(record.catalog());
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
