package org.pangrova.connector.lance;

import org.pangrova.connector.api.metadata.ManagedSourceCatalog;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.materialization.ChangeProof;
import org.pangrova.connector.api.enums.ChangeSetMode;
import org.pangrova.connector.api.materialization.ChangeSetPlan;
import org.pangrova.connector.api.materialization.MaterializationCapabilities;
import org.pangrova.connector.api.exception.MaterializationSourceException;
import org.pangrova.connector.api.materialization.PinnedSource;
import org.pangrova.connector.api.materialization.SourceProvider;
import org.pangrova.connector.api.materialization.SourceRef;
import org.pangrova.connector.api.materialization.SourceSnapshotRef;

import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;

import static org.pangrova.common.util.Preconditions.requireText;

/**
 * Lance materialization source contract.
 *
 * <p>An accepted Lance version is immutable authority state: publications and
 * their manifests are retained after the head advances, so a pin needs no
 * physical tag. Version {@code 0} is the empty incarnation and has no accepted
 * publication, therefore it cannot be planned as a source snapshot.
 *
 * <p>Lance does not expose an append-only proof over the write journal, so this
 * provider declares no change-set capability and fails closed instead of
 * inferring one from accepted versions.
 */
public final class LanceMaterializationSourceProvider implements SourceProvider {

    private static final String CONNECTOR_ID = "lance";

    private final String catalogName;
    private final LanceCatalogStore store;

    /** Uses the catalog's durable accepted-publication authority for source snapshots. */
    public LanceMaterializationSourceProvider(String catalogName, ManagedSourceCatalog authority) {
        if (catalogName == null || catalogName.isBlank()) {
            throw new IllegalArgumentException("catalogName must not be blank");
        }
        this.catalogName = catalogName;
        this.store = new LanceCatalogStore(Objects.requireNonNull(authority, "authority"));
    }

    @Override
    public String connectorId() {
        return CONNECTOR_ID;
    }

    @Override
    public MaterializationCapabilities capabilities(SourceRef source) {
        requireSource(source);
        return new MaterializationCapabilities(
                MaterializationCapabilities.CURRENT_VERSION,
                true,
                false,
                false,
                false,
                false,
                false,
                true,
                true);
    }

    @Override
    public SourceSnapshotRef describe(SourceRef source, String tenant, String snapshotToken,
            String policyFingerprint) {
        requireSource(source);
        requireText(tenant, "tenant");
        requireText(policyFingerprint, "policyFingerprint");
        long version = parseVersion(snapshotToken, source);
        LanceCatalogStore.Snapshot snapshot = loadExact(source, tenant, version);
        return new SourceSnapshotRef(CONNECTOR_ID, Long.toString(version),
                schemaFingerprint(snapshot.table()), policyFingerprint, Instant.now(), true);
    }

    @Override
    public PinnedSource pin(SourceRef source, String tenant, SourceSnapshotRef expected,
            String pinOwner) {
        requireSource(source);
        requireText(tenant, "tenant");
        requireText(pinOwner, "pinOwner");
        requireExpected(source, expected);
        long version = parseVersion(expected.snapshotToken(), source);
        LanceCatalogStore.Snapshot snapshot = loadExact(source, tenant, version);
        String actualSchema = schemaFingerprint(snapshot.table());
        if (!actualSchema.equals(expected.schemaFingerprint())) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SHAPE_MISMATCH,
                    "Lance source " + source.canonicalName()
                            + " changed schema since planning");
        }
        return new PinnedSource(tenant, source, expected, pinOwner);
    }

    @Override
    public ChangeSetPlan changeSet(PinnedSource from, PinnedSource to, ChangeSetMode mode) {
        SourceRef source = from == null ? null : from.source();
        requireSource(source);
        throw unsupported(source, mode);
    }

    @Override
    public ChangeProof verify(ChangeSetPlan plan) {
        SourceRef source = plan == null ? null : plan.source();
        ChangeSetMode mode = plan == null ? null : plan.mode();
        requireSource(source);
        throw unsupported(source, mode);
    }

    @Override
    public void unpin(PinnedSource pinned) {
        if (pinned == null) {
            throw new IllegalArgumentException("pinned must not be null");
        }
        requireSource(pinned.source());
        // Accepted versions are retained by the source authority for the life
        // of the table incarnation; there is no separate physical hold.
    }

    private LanceCatalogStore.Snapshot loadExact(SourceRef source, String tenant, long version) {
        var table = new SchemaTableName(source.schema(), source.table());
        LanceTableHandle current;
        try {
            current = store.load(catalogName, tenant, table);
        } catch (RuntimeException failure) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SOURCE_UNAVAILABLE,
                    "Lance source " + source.canonicalName() + " is not readable", failure);
        }
        if (current == null) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SOURCE_UNAVAILABLE,
                    "Lance source " + source.canonicalName() + " does not exist");
        }
        if (version > current.version()) {
            throw unavailable(source, version);
        }
        try {
            return store.loadSnapshot((LanceTableHandle) current.withTimeTravelSnapshot(version));
        } catch (IllegalStateException failure) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE,
                    "Lance snapshot " + version + " of " + source.canonicalName()
                            + " is no longer available", failure);
        }
    }

    private static long parseVersion(String token, SourceRef source) {
        requireText(token, "snapshotToken");
        long version;
        try {
            version = Long.parseLong(token);
        } catch (NumberFormatException invalid) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE,
                    "Invalid Lance snapshot token for " + source.canonicalName()
                            + ": " + token, invalid);
        }
        if (version <= 0) {
            throw unavailable(source, version);
        }
        return version;
    }

    private static MaterializationSourceException unavailable(SourceRef source, long version) {
        return new MaterializationSourceException(
                MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE,
                "Lance source " + source.canonicalName() + " has no accepted snapshot at version "
                        + version);
    }

    private static MaterializationSourceException unsupported(SourceRef source, ChangeSetMode mode) {
        return new MaterializationSourceException(
                MaterializationSourceException.Reason.CHANGESET_UNSUPPORTED,
                "Lance source " + (source == null ? "<unknown>" : source.canonicalName())
                        + " does not support change-set mode " + mode);
    }

    private void requireSource(SourceRef source) {
        if (source == null) {
            throw new IllegalArgumentException("source must not be null");
        }
        if (!CONNECTOR_ID.equals(source.connectorId())) {
            throw new IllegalArgumentException(
                    "Source connector " + source.connectorId() + " is not Lance");
        }
        if (!catalogName.equals(source.catalog())) {
            throw new IllegalArgumentException(
                    "Source catalog " + source.catalog() + " is not " + catalogName);
        }
    }

    private static void requireExpected(SourceRef source, SourceSnapshotRef expected) {
        if (expected == null) {
            throw new IllegalArgumentException("expected snapshot must not be null");
        }
        if (!CONNECTOR_ID.equals(expected.connectorId())) {
            throw new IllegalArgumentException(
                    "Expected snapshot connector " + expected.connectorId() + " is not Lance");
        }
    }

    private static String schemaFingerprint(LanceTableHandle table) {
        return "lance-schema:" + HexFormat.of().formatHex(table.schemaFence());
    }
}
