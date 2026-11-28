package org.pangrova.connector.paimon.write;

import org.pangrova.common.crypto.Sha256Digests;

import org.pangrova.connector.paimon.schema.PaimonPhysicalSchemas;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.pangrova.connector.api.metadata.NativePhysicalFileAllocation;
import org.pangrova.connector.api.metadata.NativePhysicalWriteAllocation;

/** Captures real SDK path, schema, snapshot and sequence metadata before a Native object exists. */
public final class PaimonPhysicalWriteAllocations {
    private PaimonPhysicalWriteAllocations() {}

    /**
     * Allocates an append object against an exact SDK source snapshot.
     *
     * @param table the SDK table authority
     * @param baseSnapshotId the selected source snapshot, or -1 for the initial empty state
     * @return the immutable physical output allocation
     */
    public static NativePhysicalWriteAllocation append(FileStoreTable table, long baseSnapshotId) {
        requireAppendLayout(table);
        var files = restoredFiles(table, baseSnapshotId, false);
        var pathFactory = table.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
        var path = pathFactory.newPath();
        byte[] digest = fileSetDigest(files);
        var allocation = new NativePhysicalFileAllocation(new NativePhysicalFileAllocation.Location(
                path.toString(), path.getName(), FileSource.APPEND.name()), new byte[0], 0, 0,
                DataFileMeta.getMaxSequenceNumber(files), digest, new byte[0]);
        var schema = table.schema();
        var policy = new NativePhysicalWriteAllocation.Policy("INSERT", List.of(), List.of(),
                schema.bucketKeys().stream().map(key -> schema.fields().stream()
                        .filter(field -> field.name().equals(key)).findFirst().orElseThrow().id()).toList(),
                schema.numBuckets(), writerBufferHint(schema.options()), schema.options());
        return new NativePhysicalWriteAllocation(new NativePhysicalWriteAllocation.Identity(
                UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                PaimonPhysicalSchemas.forAppend(schema), policy, pathFactory.parent().toString(),
                baseSnapshotId, List.of(allocation));
    }

    /** Rechecks exact retained source metadata without generating a new file or write identity. */
    public static void validateSource(FileStoreTable table, NativePhysicalWriteAllocation allocation) {
        requireAppendLayout(table);
        var files = restoredFiles(table, allocation.baseSnapshotId(), true);
        if (allocation.files().size() != 1) {
            throw new IllegalArgumentException("Unexpected append file allocation count");
        }
        var file = allocation.files().get(0);
        var pathFactory = table.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, file.bucketId());
        if (file.bucketId() != 0 || file.fileOrdinal() != 0 || file.partition().length != 0
                || !FileSource.APPEND.name().equals(file.location().fileSource())
                || !pathFactory.parent().toString().equals(allocation.allocationPrefix())
                || !file.location().opaqueLocation().equals(
                        allocation.allocationPrefix() + "/" + file.location().fileName())
                || file.restoredMaxSequence() != DataFileMeta.getMaxSequenceNumber(files)
                || !MessageDigest.isEqual(file.restoredFileSetSha256(), fileSetDigest(files))) {
            throw new IllegalArgumentException(
                    "Physical allocation differs from exact SDK source files or bucket path");
        }
    }

    /** Exact table/catalog-configured SDK TOTAL cache hint, never a task memory grant. */
    public static long writerBufferHint(java.util.Map<String, String> options) {
        String value = options.get("lance.native.encoder-cache-bytes");
        if (value == null || !value.matches("[1-9][0-9]*")) {
            throw new IllegalArgumentException(
                    "Native Lance physical writes require lance.native.encoder-cache-bytes as positive bytes");
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("Native Lance encoder cache hint exceeds signed byte bounds", failure);
        }
    }

    private static void requireAppendLayout(FileStoreTable table) {
        if (!"lance".equalsIgnoreCase(table.options().get("file.format"))
                || !table.primaryKeys().isEmpty() || !table.partitionKeys().isEmpty()
                || table.schema().numBuckets() != 1) {
            throw new IllegalArgumentException(
                    "Native physical append allocation requires an unpartitioned one-bucket append Lance table");
        }
    }

    private static List<DataFileMeta> restoredFiles(FileStoreTable table, long baseSnapshotId, boolean replay) {
        requireSourceSnapshot(table, baseSnapshotId, replay);
        if (baseSnapshotId == -1) return List.of();
        var pinned = table.copyWithoutTimeTravel(java.util.Map.of(
                CoreOptions.SCAN_SNAPSHOT_ID.key(), Long.toString(baseSnapshotId)));
        var files = new TreeMap<String, DataFileMeta>();
        for (var split : pinned.newReadBuilder().newScan().plan().splits()) {
            if (!(split instanceof DataSplit data) || data.snapshotId() != baseSnapshotId
                    || data.bucket() != 0 || data.partition().getFieldCount() != 0) {
                throw new IllegalArgumentException("SDK scan differs from the pinned physical allocation layout");
            }
            for (var file : data.dataFiles()) {
                var existing = files.putIfAbsent(file.fileName(), file);
                if (existing != null && !existing.equals(file)) {
                    throw new IllegalArgumentException("Source snapshot contains conflicting physical file identities");
                }
            }
        }
        return List.copyOf(files.values());
    }

    private static void requireSourceSnapshot(FileStoreTable table, long baseSnapshotId, boolean replay) {
        Long latest = table.snapshotManager().latestSnapshotId();
        if (baseSnapshotId < -1 || (latest == null && baseSnapshotId != -1)
                || (latest != null && baseSnapshotId == -1 && !replay)) {
            throw new IllegalArgumentException("Physical allocation has no matching source snapshot");
        }
        if (baseSnapshotId == -1) {
            Long earliest = table.snapshotManager().earliestSnapshotId();
            if (earliest != null && earliest > 1) {
                throw new IllegalArgumentException("Physical allocation initial source history expired");
            }
            return;
        }
        if (!table.snapshotManager().snapshotExists(baseSnapshotId)) {
            throw new IllegalArgumentException("Physical allocation source snapshot no longer exists");
        }
    }

    private static byte[] fileSetDigest(List<DataFileMeta> files) {
        try {
            return Sha256Digests.sha256().digest(new DataFileMetaSerializer().serializeList(files));
        } catch (IOException failure) {
            throw new IllegalStateException("SDK source file metadata serialization failed", failure);
        }
    }
}
