package org.pangrova.connector.paimon;

import org.pangrova.connector.api.enums.MaterializationSourceFailureReason;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.connector.paimon.handle.PaimonTableIdentity;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.SnapshotManager;
import org.pangrova.connector.api.materialization.ChangeProof;
import org.pangrova.connector.api.enums.ChangeSetMode;
import org.pangrova.connector.api.materialization.ChangeSetPlan;
import org.pangrova.connector.api.materialization.MaterializationCapabilities;
import org.pangrova.connector.api.exception.MaterializationSourceException;
import org.pangrova.connector.api.materialization.PinnedSource;
import org.pangrova.connector.api.materialization.SourceProvider;
import org.pangrova.connector.api.materialization.SourceRef;
import org.pangrova.connector.api.materialization.SourceSnapshotRef;

import static org.pangrova.common.validation.Preconditions.requireText;

/**
 * Paimon materialization source contract.
 *
 * <p>Snapshots are held by an attempt-scoped Paimon tag. Append-only intervals
 * are only provable for non-primary-key tables whose commits are all
 * {@link Snapshot.CommitKind#APPEND}; primary-key tables merge changelogs and
 * therefore fail closed instead of being treated as appends.
 */
public final class PaimonMaterializationSourceProvider implements SourceProvider {

    private static final String CONNECTOR_ID = "paimon";
    private static final String STRATEGY_VERSION = "paimon-append-v1";
    private static final String TAG_PREFIX = "pangrova_materialization_";

    private final Catalog catalog;
    private final String catalogName;

    /**
     * Creates the provider over a resolved Paimon catalog.
     *
     * @param catalogName logical catalog name reported in source references
     * @param catalog Paimon catalog used for snapshots and manifests
     */
    public PaimonMaterializationSourceProvider(String catalogName, Catalog catalog) {
        if (catalogName == null || catalogName.isBlank()) {
            throw new IllegalArgumentException("catalogName must not be blank");
        }
        this.catalogName = catalogName;
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    @Override
    public String connectorId() {
        return CONNECTOR_ID;
    }

    @Override
    public MaterializationCapabilities capabilities(SourceRef source) {
        requireSource(source);
        FileStoreTable table = load(source);
        boolean appendOnlyTable = table.primaryKeys().isEmpty();
        return new MaterializationCapabilities(
                MaterializationCapabilities.CURRENT_VERSION,
                true,
                appendOnlyTable,
                true,
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
        requireText(snapshotToken, "snapshotToken");
        requireText(policyFingerprint, "policyFingerprint");
        FileStoreTable table = load(source);
        long snapshotId = parseSnapshotToken(snapshotToken, source);
        if (!table.snapshotManager().snapshotExists(snapshotId)) {
            throw new MaterializationSourceException(
                    MaterializationSourceFailureReason.SNAPSHOT_UNAVAILABLE,
                    "Paimon snapshot " + snapshotId + " of " + source.canonicalName()
                            + " is not available");
        }
        return new SourceSnapshotRef(CONNECTOR_ID, Long.toString(snapshotId),
                schemaFingerprint(table.schema()), policyFingerprint, Instant.now(), true);
    }

    @Override
    public PinnedSource pin(SourceRef source, String tenant, SourceSnapshotRef expected,
            String pinOwner) {
        requireSource(source);
        requireText(tenant, "tenant");
        requireText(pinOwner, "pinOwner");
        requireExpected(expected);
        FileStoreTable table = load(source);
        long snapshotId = parseSnapshot(expected, source);
        if (!table.snapshotManager().snapshotExists(snapshotId)) {
            throw new MaterializationSourceException(
                    MaterializationSourceFailureReason.SNAPSHOT_UNAVAILABLE,
                    "Paimon snapshot " + snapshotId + " of " + source.canonicalName()
                            + " is no longer available");
        }
        String actualSchema = schemaFingerprint(table.schema());
        if (!actualSchema.equals(expected.schemaFingerprint())) {
            throw new MaterializationSourceException(
                    MaterializationSourceFailureReason.SHAPE_MISMATCH,
                    "Paimon source " + source.canonicalName()
                            + " changed schema since planning");
        }
        String tag = tagName(pinOwner, snapshotId);
        if (table.tagManager().tagExists(tag)) {
            requireExact(taggedSnapshot(table, tag), snapshotId, tag);
        } else {
            try {
                table.createTag(tag, snapshotId, null);
            } catch (RuntimeException failure) {
                throw new MaterializationSourceException(
                        MaterializationSourceFailureReason.PIN_UNSUPPORTED,
                        "Could not pin Paimon snapshot " + snapshotId + " of "
                                + source.canonicalName(), failure);
            }
            requireExact(taggedSnapshot(load(source), tag), snapshotId, tag);
        }
        return new PinnedSource(tenant, source, expected, pinOwner);
    }

    @Override
    public ChangeSetPlan changeSet(PinnedSource from, PinnedSource to, ChangeSetMode mode) {
        requireSameSource(from, to);
        SourceRef source = from.source();
        requireSource(source);
        if (!from.snapshot().hasSameShape(to.snapshot())) {
            throw new MaterializationSourceException(
                    MaterializationSourceFailureReason.SHAPE_MISMATCH,
                    "Paimon source " + source.canonicalName()
                            + " changed schema or policy shape inside the change interval");
        }
        MaterializationCapabilities capabilities = capabilities(source);
        if (!capabilities.supports(mode) || mode != ChangeSetMode.APPEND_ONLY) {
            throw unsupported(source, mode);
        }
        FileStoreTable table = load(source);
        long fromId = snapshotId(from, table);
        long toId = snapshotId(to, table);
        requireAppendOnly(table, fromId, toId, source);
        String fingerprint = Sha256Digests.sha256Hex("paimon-change-set-v1|" + source.canonicalName()
                + '|' + fromId + '|' + toId + '|' + mode.name());
        return new ChangeSetPlan(source, from.snapshot(), to.snapshot(), mode,
                fingerprint, Instant.now(), List.of());
    }

    @Override
    public ChangeProof verify(ChangeSetPlan plan) {
        SourceRef source = plan.source();
        requireSource(source);
        FileStoreTable table = load(source);
        long fromId = parseSnapshot(plan.fromSnapshot(), source);
        long toId = parseSnapshot(plan.toSnapshot(), source);
        boolean complete = appendOnly(table, fromId, toId, source);
        return new ChangeProof(source, Long.toString(fromId), Long.toString(toId),
                plan.mode(), Sha256Digests.sha256Hex(plan.planFingerprint() + "|verified|" + complete),
                STRATEGY_VERSION, Instant.now(), complete);
    }

    @Override
    public void unpin(PinnedSource pinned) {
        requireSource(pinned.source());
        long snapshotId = parseSnapshot(pinned.snapshot(), pinned.source());
        FileStoreTable table = load(pinned.source());
        String tag = tagName(pinned.pinOwner(), snapshotId);
        if (!table.tagManager().tagExists(tag)) {
            return;
        }
        requireExact(taggedSnapshot(table, tag), snapshotId, tag);
        try {
            table.deleteTag(tag);
        } catch (RuntimeException failure) {
            throw new MaterializationSourceException(
                    MaterializationSourceFailureReason.PIN_UNSUPPORTED,
                    "Could not release Paimon pin " + tag + " of "
                            + pinned.source().canonicalName(), failure);
        }
    }

    private long snapshotId(PinnedSource pinned, FileStoreTable table) {
        long snapshotId = parseSnapshot(pinned.snapshot(), pinned.source());
        if (!table.snapshotManager().snapshotExists(snapshotId)) {
            throw new MaterializationSourceException(
                    MaterializationSourceFailureReason.SNAPSHOT_UNAVAILABLE,
                    "Paimon snapshot " + snapshotId + " of "
                            + pinned.source().canonicalName() + " is no longer available");
        }
        return snapshotId;
    }

    private static void requireAppendOnly(
            FileStoreTable table, long fromId, long toId, SourceRef source) {
        if (!appendOnly(table, fromId, toId, source)) {
            throw new MaterializationSourceException(
                    MaterializationSourceFailureReason.PROOF_INCOMPLETE,
                    "Paimon change interval " + fromId + ".." + toId + " of "
                            + source.canonicalName() + " is not append-only");
        }
    }

    private static boolean appendOnly(
            FileStoreTable table, long fromId, long toId, SourceRef source) {
        if (!table.primaryKeys().isEmpty()) {
            return false;
        }
        if (fromId == toId) {
            return true;
        }
        SnapshotManager snapshots = table.snapshotManager();
        for (long id = fromId + 1; id <= toId; id++) {
            Snapshot snapshot = snapshots.snapshot(id);
            if (snapshot == null) {
                throw new MaterializationSourceException(
                        MaterializationSourceFailureReason.SNAPSHOT_UNAVAILABLE,
                        "Paimon snapshot " + id + " of " + source.canonicalName()
                                + " is no longer available");
            }
            if (snapshot.commitKind() != Snapshot.CommitKind.APPEND) {
                return false;
            }
        }
        return true;
    }

    private static long parseSnapshot(SourceSnapshotRef snapshot, SourceRef source) {
        return parseSnapshotToken(snapshot.snapshotToken(), source);
    }

    private static long parseSnapshotToken(String token, SourceRef source) {
        try {
            return Long.parseLong(token);
        } catch (NumberFormatException invalid) {
            throw new MaterializationSourceException(
                    MaterializationSourceFailureReason.SNAPSHOT_UNAVAILABLE,
                    "Invalid Paimon snapshot token for " + source.canonicalName()
                            + ": " + token, invalid);
        }
    }

    private static void requireExpected(SourceSnapshotRef expected) {
        if (expected == null) {
            throw new IllegalArgumentException("expected snapshot must not be null");
        }
        if (!CONNECTOR_ID.equals(expected.connectorId())) {
            throw new IllegalArgumentException(
                    "Expected snapshot connector " + expected.connectorId() + " is not Paimon");
        }
    }

    private FileStoreTable load(SourceRef source) {
        try {
            Table table = PaimonTableIdentity.loadFresh(catalog, Identifier.create(source.schema(), source.table()));
            if (!(table instanceof FileStoreTable fileStore)) {
                throw new MaterializationSourceException(
                        MaterializationSourceFailureReason.SOURCE_UNAVAILABLE,
                        "Paimon table " + source.canonicalName()
                                + " is not a file-store table");
            }
            return fileStore;
        } catch (Catalog.TableNotExistException missing) {
            throw new MaterializationSourceException(
                    MaterializationSourceFailureReason.SOURCE_UNAVAILABLE,
                    "Paimon table " + source.canonicalName() + " does not exist", missing);
        }
    }

    private void requireSource(SourceRef source) {
        if (source == null) {
            throw new IllegalArgumentException("source must not be null");
        }
        if (!CONNECTOR_ID.equals(source.connectorId())) {
            throw new IllegalArgumentException(
                    "Source connector " + source.connectorId() + " is not Paimon");
        }
        if (!catalogName.equals(source.catalog())) {
            throw new IllegalArgumentException(
                    "Source catalog " + source.catalog() + " is not " + catalogName);
        }
    }

    private static void requireSameSource(PinnedSource from, PinnedSource to) {
        if (from == null || to == null) {
            throw new IllegalArgumentException("from and to must not be null");
        }
        if (!from.source().equals(to.source())) {
            throw new IllegalArgumentException("change-set endpoints belong to different sources");
        }
    }

    static String schemaFingerprint(TableSchema schema) {
        return org.pangrova.connector.paimon.handle.PaimonTableIdentity.schemaFingerprint(schema);
    }

    private static Long taggedSnapshot(FileStoreTable table, String tag) {
        Long result = null;
        for (var entry : table.tagManager().tags().entrySet()) {
            if (entry.getValue().contains(tag)) {
                if (result != null) {
                    throw new MaterializationSourceException(
                            MaterializationSourceFailureReason.PIN_UNSUPPORTED,
                            "Paimon tag resolves to multiple snapshots: " + tag);
                }
                result = entry.getKey().id();
            }
        }
        return result;
    }

    private static String tagName(String pinOwner, long snapshotId) {
        return TAG_PREFIX + Sha256Digests.sha256Hex(pinOwner).substring(0, 32) + '_' + snapshotId;
    }

    private static void requireExact(Long actual, long snapshotId, String tag) {
        if (actual == null || actual != snapshotId) {
            throw new MaterializationSourceException(
                    MaterializationSourceFailureReason.PIN_UNSUPPORTED,
                    "Paimon materialization pin " + tag + " differs from snapshot " + snapshotId);
        }
    }

    private static MaterializationSourceException unsupported(
            SourceRef source, ChangeSetMode mode) {
        return new MaterializationSourceException(
                MaterializationSourceFailureReason.CHANGESET_UNSUPPORTED,
                "Paimon source " + source.canonicalName()
                        + " does not support change-set mode " + mode);
    }
}
