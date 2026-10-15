package org.pangrova.connector.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.connector.iceberg.handle.IcebergManifestTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.type.IcebergTypeMapper;

/** Selects a manifest-list object without materializing manifest rows in Java. */
final class IcebergManifestReadBindings {
    private static final int HASH_BUFFER_BYTES = 64 * 1024;
    private static final int MIN_SUPPORTED_FORMAT_VERSION = 2;
    private static final int MAX_SUPPORTED_FORMAT_VERSION = 3;

    private IcebergManifestReadBindings() {}

    static IcebergManifestTableHandle select(IcebergTableHandle base, Table table) {
        if (!(table instanceof HasTableOperations operations)) {
            throw new IllegalStateException("Iceberg $manifests requires table operations");
        }
        var metadata = operations.operations().current();
        if (metadata.formatVersion() < MIN_SUPPORTED_FORMAT_VERSION
                || metadata.formatVersion() > MAX_SUPPORTED_FORMAT_VERSION) {
            throw new UnsupportedOperationException(
                    "Iceberg $manifests requires a verified v2/v3 manifest list");
        }
        if (!table.location().equals(base.tableLocation())
                || metadata.uuid() == null || metadata.uuid().isBlank()) {
            throw new IllegalStateException("Iceberg table identity changed during manifest binding");
        }
        var typedColumns = IcebergTypeMapper.toPangrovaColumns(
                MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.MANIFESTS)
                        .schema());
        Map<Integer, List<IcebergManifestTableHandle.PartitionSummaryField>> specs = new TreeMap<>();
        table.specs().forEach((id, spec) -> {
            if (spec.fields().size() != spec.partitionType().fields().size()) {
                throw new IllegalStateException("Iceberg partition spec and result type disagree");
            }
            var fields = new java.util.ArrayList<IcebergManifestTableHandle.PartitionSummaryField>();
            for (int index = 0; index < spec.fields().size(); index++) {
                fields.add(new IcebergManifestTableHandle.PartitionSummaryField(
                        spec.fields().get(index).transform().toString(),
                        spec.partitionType().fields().get(index).type().toString()));
            }
            specs.put(id, List.copyOf(fields));
        });
        Snapshot snapshot = base.snapshotId().isPresent()
                ? table.snapshot(base.snapshotId().orElseThrow()) : table.currentSnapshot();
        if (snapshot == null) {
            if (base.snapshotId().isPresent()) {
                throw new IllegalStateException("Selected Iceberg snapshot is unavailable");
            }
            return new IcebergManifestTableHandle(base, metadata.uuid(),
                    metadata.metadataFileLocation(), OptionalLong.empty(), 0,
                    null, 0, null, specs, typedColumns);
        }
        String listUri = snapshot.manifestListLocation();
        if (listUri == null || listUri.isBlank()) {
            throw new UnsupportedOperationException("Iceberg snapshot has no manifest-list object");
        }
        InputFile input = table.io().newInputFile(listUri);
        long length = input.getLength();
        if (length <= 0) {
            throw new IllegalStateException("Iceberg manifest list is empty or unavailable");
        }
        return new IcebergManifestTableHandle(base, metadata.uuid(),
                metadata.metadataFileLocation(), OptionalLong.of(snapshot.snapshotId()),
                snapshot.sequenceNumber(), listUri, length, digest(input, length), specs, typedColumns);
    }

    static void validate(IcebergManifestTableHandle bound, Table table) {
        if (!(table instanceof HasTableOperations operations)
                || !table.location().equals(bound.baseTable().tableLocation())
                || !bound.tableUuid().equals(operations.operations().current().uuid())) {
            throw new IllegalStateException("Iceberg table identity changed after manifest binding");
        }
        if (bound.empty()) {
            return;
        }
        Snapshot snapshot = table.snapshot(bound.snapshotId().orElseThrow());
        if (snapshot == null || snapshot.sequenceNumber() != bound.snapshotSequenceNumber()
                || !bound.manifestListLocation().equals(snapshot.manifestListLocation())) {
            throw new IllegalStateException("Selected Iceberg manifest list changed or expired");
        }
        InputFile input = table.io().newInputFile(bound.manifestListLocation());
        if (input.getLength() != bound.manifestListLength()
                || !bound.manifestListSha256().equals(digest(input, bound.manifestListLength()))) {
            throw new IllegalStateException("Selected Iceberg manifest-list bytes changed");
        }
    }

    private static String digest(InputFile input, long expectedLength) {
        var sha = DigestUtils.sha256();
        byte[] buffer = new byte[HASH_BUFFER_BYTES];
        long readBytes = 0;
        try (var stream = input.newStream()) {
            int read;
            while ((read = stream.read(buffer)) != -1) {
                readBytes = Math.addExact(readBytes, read);
                if (readBytes > expectedLength) {
                    throw new IllegalStateException("Iceberg manifest list grew during pinning");
                }
                sha.update(buffer, 0, read);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read Iceberg manifest list", e);
        }
        if (readBytes != expectedLength) {
            throw new IllegalStateException("Iceberg manifest list length changed during pinning");
        }
        return HexFormat.of().formatHex(sha.digest());
    }
}
