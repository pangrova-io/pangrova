package org.pangrova.connector.iceberg;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.pangrova.connector.spi.materialization.ChangeProof;
import org.pangrova.connector.spi.materialization.ChangeSetMode;
import org.pangrova.connector.spi.materialization.ChangeSetPlan;
import org.pangrova.connector.spi.materialization.MaterializationCapabilities;
import org.pangrova.connector.spi.materialization.MaterializationSourceException;
import org.pangrova.connector.spi.materialization.PartitionChange;
import org.pangrova.connector.spi.materialization.PartitionValue;
import org.pangrova.connector.spi.materialization.PinnedSource;
import org.pangrova.connector.spi.materialization.SourceProvider;
import org.pangrova.connector.spi.materialization.SourceRef;
import org.pangrova.connector.spi.materialization.SourceSnapshotRef;

/**
 * Iceberg materialization source contract.
 *
 * <p>Snapshots are held by an attempt-scoped Iceberg tag, so two attempts that
 * observe the same snapshot keep independent holds. The provider proves
 * append-only change intervals by walking the parent chain; any non-append
 * operation or delete manifest makes the interval unprovable, and the provider
 * fails closed instead of guessing. Partitioned tables additionally support
 * partition replacement when every partition field is an identity transform:
 * the provider names the partitions with added or removed data or delete files.
 */
public final class IcebergMaterializationSourceProvider implements SourceProvider {

    private static final String CONNECTOR_ID = "iceberg";
    private static final String STRATEGY_VERSION = "iceberg-append-v1";
    private static final String TAG_PREFIX = "pangrova_materialization_";

    private final Catalog catalog;
    private final String catalogName;

    /**
     * Creates the provider over a resolved Iceberg catalog.
     *
     * @param catalogName logical catalog name reported in source references
     * @param catalog Iceberg catalog used for snapshots and manifests
     */
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
                partitionReplaceSupported(load(source)),
                false,
                false,
                true,
                true);
    }

    @Override
    public java.util.List<String> partitionColumns(SourceRef source) {
        requireSource(source);
        Table table = load(source);
        if (!partitionReplaceSupported(table)) {
            return java.util.List.of();
        }
        java.util.List<String> columns = new ArrayList<>(table.spec().fields().size());
        for (PartitionField field : table.spec().fields()) {
            String column = table.schema().findColumnName(field.sourceId());
            if (column == null) {
                return java.util.List.of();
            }
            columns.add(column);
        }
        return java.util.List.copyOf(columns);
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
        List<PartitionChange> changedPartitions;
        if (mode == ChangeSetMode.APPEND_ONLY) {
            requireAppendOnly(table, fromId, toId, source);
            changedPartitions = List.of();
        } else if (mode == ChangeSetMode.PARTITION_REPLACE) {
            requirePartitionReplaceable(table, source);
            changedPartitions = changedPartitions(table, fromId, toId, source);
        } else {
            throw unsupported(source, mode);
        }
        String fingerprint = sha256("iceberg-change-set-v2|" + source.canonicalName()
                + '|' + fromId + '|' + toId + '|' + mode.name()
                + '|' + partitionFingerprint(changedPartitions));
        return new ChangeSetPlan(source, from.snapshot(), to.snapshot(), mode,
                fingerprint, Instant.now(), changedPartitions);
    }

    @Override
    public ChangeProof verify(ChangeSetPlan plan) {
        SourceRef source = plan.source();
        requireSource(source);
        Table table = load(source);
        long fromId = parseSnapshot(plan.fromSnapshot(), source);
        long toId = parseSnapshot(plan.toSnapshot(), source);
        boolean complete;
        if (plan.mode() == ChangeSetMode.PARTITION_REPLACE) {
            requirePartitionReplaceable(table, source);
            complete = plan.changedPartitions()
                    .equals(changedPartitions(table, fromId, toId, source));
        } else {
            complete = appendOnly(table, fromId, toId, source);
        }
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
        List<Snapshot> range = snapshotsAfter(table, fromId, toId, source);
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

    private static List<Snapshot> snapshotsAfter(
            Table table, long fromId, long toId, SourceRef source) {
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
        return range;
    }

    private static boolean partitionReplaceSupported(Table table) {
        PartitionSpec spec = table.spec();
        return spec.isPartitioned()
                && spec.fields().stream().allMatch(field -> field.transform().isIdentity());
    }

    private static void requirePartitionReplaceable(Table table, SourceRef source) {
        if (!partitionReplaceSupported(table)) {
            throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.CHANGESET_UNSUPPORTED,
                    "Iceberg source " + source.canonicalName()
                            + " has no identity-partitioned spec for partition replacement");
        }
    }

    /**
     * Collects the partitions whose files were added or removed in the interval.
     *
     * <p>Only identity partition specs are accepted, so a partition value maps
     * directly to one source column. The result is ordered canonically so that
     * planning and verification derive exactly the same list.
     */
    private static List<PartitionChange> changedPartitions(
            Table table, long fromId, long toId, SourceRef source) {
        if (fromId == toId) {
            return List.of();
        }
        PartitionSpec spec = table.spec();
        Map<String, PartitionChange> changes = new LinkedHashMap<>();
        for (Snapshot snapshot : snapshotsAfter(table, fromId, toId, source)) {
            collectPartitions(table, spec, snapshot.addedDataFiles(table.io()), changes, source);
            collectPartitions(table, spec, snapshot.removedDataFiles(table.io()), changes, source);
            // A merge-on-read delete changes the rows of its partition without
            // touching any data file, so delete-file partitions are changes too.
            collectPartitions(table, spec, snapshot.addedDeleteFiles(table.io()), changes, source);
            collectPartitions(table, spec, snapshot.removedDeleteFiles(table.io()), changes, source);
        }
        return changes.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
                .map(Map.Entry::getValue)
                .toList();
    }

    private static void collectPartitions(Table table, PartitionSpec spec,
            Iterable<? extends ContentFile<?>> files, Map<String, PartitionChange> changes,
            SourceRef source) {
        Types.StructType partitionType = spec.partitionType();
        for (ContentFile<?> file : files) {
            if (file.specId() != spec.specId()) {
                throw new MaterializationSourceException(
                        MaterializationSourceException.Reason.SHAPE_MISMATCH,
                        "Iceberg content file of " + source.canonicalName()
                                + " uses partition spec " + file.specId()
                                + " instead of " + spec.specId());
            }
            List<PartitionValue> values = new ArrayList<>(spec.fields().size());
            for (int index = 0; index < spec.fields().size(); index++) {
                PartitionField field = spec.fields().get(index);
                String column = table.schema().findColumnName(field.sourceId());
                if (column == null) {
                    throw new MaterializationSourceException(
                            MaterializationSourceException.Reason.SHAPE_MISMATCH,
                            "Iceberg partition field " + field.name() + " of "
                                    + source.canonicalName() + " has no source column");
                }
                Types.NestedField partitionField = partitionType.fields().get(index);
                Object value = file.partition().get(index,
                        partitionField.type().typeId().javaClass());
                values.add(new PartitionValue(column, literal(partitionField, value, source)));
            }
            PartitionChange change = new PartitionChange(values);
            changes.putIfAbsent(partitionFingerprint(List.of(change)), change);
        }
    }

    private static String literal(
            Types.NestedField field, Object value, SourceRef source) {
        if (value == null) {
            return null;
        }
        return switch (field.type().typeId()) {
            case INTEGER -> ((Number) value).longValue() + "";
            case LONG -> ((Number) value).longValue() + "";
            case DATE -> "DATE '" + LocalDate.ofEpochDay(((Number) value).longValue()) + "'";
            case STRING -> "'" + value.toString().replace("'", "''") + "'";
            case BOOLEAN -> ((Boolean) value) ? "TRUE" : "FALSE";
            case DECIMAL -> ((BigDecimal) value).toPlainString();
            default -> throw new MaterializationSourceException(
                    MaterializationSourceException.Reason.CHANGESET_UNSUPPORTED,
                    "Iceberg partition type " + field.type() + " of " + source.canonicalName()
                            + " cannot be expressed as a SQL literal");
        };
    }

    private static String partitionFingerprint(List<PartitionChange> partitions) {
        StringBuilder canonical = new StringBuilder();
        for (PartitionChange change : partitions) {
            canonical.append('[');
            for (PartitionValue value : change.values()) {
                canonical.append(value.column()).append('=')
                        .append(value.literal() == null ? "\\N" : value.literal())
                        .append(',');
            }
            canonical.append(']');
        }
        return canonical.toString();
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
