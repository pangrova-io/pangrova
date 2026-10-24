package org.pangrova.connector.iceberg;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.pangrova.spi.materialization.ChangeProof;
import org.pangrova.spi.materialization.ChangeSetMode;
import org.pangrova.spi.materialization.ChangeSetPlan;
import org.pangrova.spi.materialization.MaterializationCapabilities;
import org.pangrova.spi.materialization.MaterializationSourceException;
import org.pangrova.spi.materialization.PinnedSource;
import org.pangrova.spi.materialization.SourceProvider;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.materialization.SourceSnapshotRef;

/**
 * Iceberg materialization source contract.
 *
 * <p>Snapshots are held by an attempt-scoped Iceberg tag, so two attempts that
 * observe the same snapshot keep independent holds. The provider proves
 * append-only change intervals by walking the parent chain; any non-append
 * operation or delete manifest makes the interval unprovable, and the provider
 * fails closed instead of guessing.
 */
public final class IcebergMaterializationSourceProvider implements SourceProvider {

    private static final String CONNECTOR_ID = "iceberg";
    private static final String STRATEGY_VERSION = "iceberg-append-v1";
    private static final String TAG_PREFIX = "pangrova_materialization_";

    private final Catalog catalog;
    private final String catalogName;

    public IcebergMaterializationSourceProvider(String catalogName, Catalog catalog) {
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
        return new MaterializationCapabilities(
                MaterializationCapabilities.CURRENT_VERSION,
                true,
                true,
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
        Table table = load(source);
        long snapshotId = parseSnapshotToken(snapshotToken, source);
        if (table.snapshot(snapshotId) == null) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE,
                    "Iceberg snapshot " + snapshotId + " of " + source.canonicalName()
                            + " is not available");
        }
        return new SourceSnapshotRef(CONNECTOR_ID, Long.toString(snapshotId),
                schemaFingerprint(table), policyFingerprint, Instant.now(), true);
    }

    @Override
    public SourceSnapshotRef describeEmpty(SourceRef source, String tenant, String sourceIdentity,
            String policyFingerprint) {
        requireSource(source);
        requireText(tenant, "tenant");
        requireText(sourceIdentity, "sourceIdentity");
        requireText(policyFingerprint, "policyFingerprint");
        Table table = load(source);
        if (table.currentSnapshot() != null) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SHAPE_MISMATCH,
                    "Iceberg source " + source.canonicalName()
                            + " gained a data snapshot since planning");
        }
        String identity = emptyIdentity(table);
        if (!identity.equals(sourceIdentity)) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SHAPE_MISMATCH,
                    "Iceberg empty identity of " + source.canonicalName()
                            + " changed since planning");
        }
        return new SourceSnapshotRef(CONNECTOR_ID,
                SourceSnapshotRef.EMPTY_TOKEN_PREFIX + identity,
                schemaFingerprint(table), policyFingerprint, Instant.now(), true);
    }

    @Override
    public PinnedSource pin(SourceRef source, String tenant, SourceSnapshotRef expected,
            String pinOwner) {
        requireSource(source);
        requireText(tenant, "tenant");
        requireText(pinOwner, "pinOwner");
        requireExpected(source, expected);
        Table table = load(source);
        if (expected.isEmptySource()) {
            return pinEmpty(source, tenant, expected, pinOwner, table);
        }
        long snapshotId = parseSnapshot(expected, source);
        if (table.snapshot(snapshotId) == null) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE,
                    "Iceberg snapshot " + snapshotId + " of " + source.canonicalName()
                            + " is no longer available");
        }
        String actualSchema = schemaFingerprint(table);
        if (!actualSchema.equals(expected.schemaFingerprint())) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SHAPE_MISMATCH,
                    "Iceberg source " + source.canonicalName()
                            + " changed schema since planning");
        }
        String tag = tagName(pinOwner, snapshotId);
        SnapshotRef existing = table.refs().get(tag);
        if (existing != null) {
            requireExact(existing, snapshotId, tag);
        } else {
            try {
                table.manageSnapshots().createTag(tag, snapshotId)
                        .setMaxRefAgeMs(tag, Long.MAX_VALUE).commit();
            } catch (RuntimeException failure) {
                throw new MaterializationSourceException(
                        MaterializationSourceException.Reason.PIN_UNSUPPORTED,
                        "Could not pin Iceberg snapshot " + snapshotId
                                + " of " + source.canonicalName(), failure);
            }
            table.refresh();
            requireExact(table.refs().get(tag), snapshotId, tag);
        }
        return new PinnedSource(tenant, source, expected, pinOwner);
    }

    private static PinnedSource pinEmpty(SourceRef source, String tenant,
            SourceSnapshotRef expected, String pinOwner, Table table) {
        if (table.currentSnapshot() != null) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SHAPE_MISMATCH,
                    "Iceberg source " + source.canonicalName()
                            + " gained a data snapshot since planning");
        }
        String identity = emptyIdentity(table);
        if (!expected.snapshotToken().equals(SourceSnapshotRef.EMPTY_TOKEN_PREFIX + identity)) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SHAPE_MISMATCH,
                    "Iceberg empty identity of " + source.canonicalName()
                            + " changed since planning");
        }
        String actualSchema = schemaFingerprint(table);
        if (!actualSchema.equals(expected.schemaFingerprint())) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SHAPE_MISMATCH,
                    "Iceberg source " + source.canonicalName()
                            + " changed schema since planning");
        }
        return new PinnedSource(tenant, source, expected, pinOwner);
    }

    @Override
    public ChangeSetPlan changeSet(PinnedSource from, PinnedSource to, ChangeSetMode mode) {
        requireSameSource(from, to);
        SourceRef source = from.source();
        requireSource(source);
        if (from.snapshot().isEmptySource() || to.snapshot().isEmptySource()) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.CHANGESET_UNSUPPORTED,
                    "Iceberg source " + source.canonicalName()
                            + " has no change-set interval from an empty snapshot");
        }
        if (!from.snapshot().hasSameShape(to.snapshot())) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SHAPE_MISMATCH,
                    "Iceberg source " + source.canonicalName()
                            + " changed schema or policy shape inside the change interval");
        }
        MaterializationCapabilities capabilities = capabilities(source);
        if (!capabilities.supports(mode)) {
            throw unsupported(source, mode);
        }
        Table table = load(source);
        long fromId = snapshotId(from, table);
        long toId = snapshotId(to, table);
        if (mode != ChangeSetMode.APPEND_ONLY) {
            throw unsupported(source, mode);
        }
        requireAppendOnly(table, fromId, toId, source);
        String fingerprint = sha256("iceberg-change-set-v1|" + source.canonicalName()
                + '|' + fromId + '|' + toId + '|' + mode.name());
        return new ChangeSetPlan(source, from.snapshot(), to.snapshot(), mode,
                fingerprint, Instant.now());
    }

    @Override
    public ChangeProof verify(ChangeSetPlan plan) {
        SourceRef source = plan.source();
        requireSource(source);
        Table table = load(source);
        long fromId = parseSnapshot(plan.fromSnapshot(), source);
        long toId = parseSnapshot(plan.toSnapshot(), source);
        boolean complete = appendOnly(table, fromId, toId, source);
        return new ChangeProof(source, Long.toString(fromId), Long.toString(toId),
                plan.mode(), sha256(plan.planFingerprint() + "|verified|" + complete),
                STRATEGY_VERSION, Instant.now(), complete);
    }

    @Override
    public void unpin(PinnedSource pinned) {
        requireSource(pinned.source());
        if (pinned.snapshot().isEmptySource()) {
            return;
        }
        long snapshotId = parseSnapshot(pinned.snapshot(), pinned.source());
        Table table = load(pinned.source());
        String tag = tagName(pinned.pinOwner(), snapshotId);
        SnapshotRef existing = table.refs().get(tag);
        if (existing == null) {
            return;
        }
        requireExact(existing, snapshotId, tag);
        try {
            table.manageSnapshots().removeTag(tag).commit();
        } catch (RuntimeException failure) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.PIN_UNSUPPORTED,
                    "Could not release Iceberg pin " + tag + " of "
                            + pinned.source().canonicalName(), failure);
        }
    }

    private long snapshotId(PinnedSource pinned, Table table) {
        long snapshotId = parseSnapshot(pinned.snapshot(), pinned.source());
        if (table.snapshot(snapshotId) == null) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE,
                    "Iceberg snapshot " + snapshotId + " of "
                            + pinned.source().canonicalName() + " is no longer available");
        }
        return snapshotId;
    }

    private static long parseSnapshot(SourceSnapshotRef snapshot, SourceRef source) {
        return parseSnapshotToken(snapshot.snapshotToken(), source);
    }

    private static long parseSnapshotToken(String token, SourceRef source) {
        try {
            return Long.parseLong(token);
        } catch (NumberFormatException invalid) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE,
                    "Invalid Iceberg snapshot token for " + source.canonicalName()
                            + ": " + token, invalid);
        }
    }

    private static void requireExpected(SourceRef source, SourceSnapshotRef expected) {
        if (expected == null) {
            throw new IllegalArgumentException("expected snapshot must not be null");
        }
        if (!CONNECTOR_ID.equals(expected.connectorId())) {
            throw new IllegalArgumentException(
                    "Expected snapshot connector " + expected.connectorId() + " is not Iceberg");
        }
    }

    private static void requireAppendOnly(
            Table table, long fromId, long toId, SourceRef source) {
        if (!appendOnly(table, fromId, toId, source)) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.PROOF_INCOMPLETE,
                    "Iceberg change interval " + fromId + ".." + toId + " of "
                            + source.canonicalName() + " is not append-only");
        }
    }

    private static boolean appendOnly(Table table, long fromId, long toId, SourceRef source) {
        if (fromId == toId) {
            return true;
        }
        Snapshot current = table.snapshot(toId);
        if (current == null) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE,
                    "Iceberg snapshot " + toId + " of " + source.canonicalName()
                            + " is no longer available");
        }
        List<Snapshot> range = new ArrayList<>();
        while (current != null && current.snapshotId() != fromId) {
            range.add(current);
            Long parent = current.parentId();
            current = parent == null ? null : table.snapshot(parent);
        }
        if (current == null) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.CHANGESET_UNSUPPORTED,
                    "Iceberg snapshot " + fromId + " is not an ancestor of " + toId
                            + " for " + source.canonicalName());
        }
        for (Snapshot snapshot : range) {
            if (!"append".equals(snapshot.operation())) {
                return false;
            }
            if (!snapshot.deleteManifests(table.io()).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private Table load(SourceRef source) {
        try {
            Table table = catalog.loadTable(TableIdentifier.of(source.schema(), source.table()));
            table.refresh();
            return table;
        } catch (RuntimeException failure) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SOURCE_UNAVAILABLE,
                    "Iceberg table " + source.canonicalName() + " is not readable", failure);
        }
    }

    private void requireSource(SourceRef source) {
        if (source == null) {
            throw new IllegalArgumentException("source must not be null");
        }
        if (!CONNECTOR_ID.equals(source.connectorId())) {
            throw new IllegalArgumentException(
                    "Source connector " + source.connectorId() + " is not Iceberg");
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

    private static String schemaFingerprint(Table table) {
        StringBuilder canonical = new StringBuilder("iceberg-schema:")
                .append(table.schema().schemaId())
                .append('|').append(table.spec().specId());
        for (Types.NestedField field : table.schema().asStruct().fields()) {
            canonical.append('|').append(field.fieldId()).append(':')
                    .append(field.name()).append(':').append(field.type())
                    .append(':').append(field.isRequired() ? "R" : "O");
        }
        Set<Integer> identifiers = table.schema().identifierFieldIds();
        identifiers.stream().sorted().forEach(id -> canonical.append("|id:").append(id));
        for (PartitionField field : table.spec().fields()) {
            canonical.append("|p:").append(field.sourceId()).append(':')
                    .append(field.fieldId()).append(':').append(field.name())
                    .append(':').append(field.transform());
        }
        return sha256(canonical.toString());
    }

    private static String emptyIdentity(Table table) {
        if (!(table instanceof HasTableOperations operations)) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SOURCE_UNAVAILABLE,
                    "Iceberg table " + table.name() + " has no table operations");
        }
        String uuid = operations.operations().current().uuid();
        if (uuid == null || uuid.isBlank()) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.SOURCE_UNAVAILABLE,
                    "Iceberg table " + table.name() + " has no table UUID");
        }
        return sha256(uuid + "\0" + table.location() + "\0EMPTY");
    }

    private static String tagName(String pinOwner, long snapshotId) {
        return TAG_PREFIX + sha256(pinOwner).substring(0, 32) + '_' + snapshotId;
    }

    private static void requireExact(
            SnapshotRef reference, long snapshotId, String tag) {
        if (reference == null || !reference.isTag() || reference.snapshotId() != snapshotId) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.PIN_UNSUPPORTED,
                    "Iceberg materialization pin " + tag + " differs from snapshot " + snapshotId);
        }
    }

    private static MaterializationSourceException unsupported(
            SourceRef source, ChangeSetMode mode) {
        return new MaterializationSourceException(
                MaterializationSourceException.Reason.CHANGESET_UNSUPPORTED,
                "Iceberg source " + source.canonicalName()
                        + " does not support change-set mode " + mode);
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", failure);
        }
    }
}
