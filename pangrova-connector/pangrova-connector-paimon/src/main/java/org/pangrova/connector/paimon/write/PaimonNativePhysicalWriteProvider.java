package org.pangrova.connector.paimon.write;

import org.pangrova.connector.paimon.schema.PaimonPhysicalSchemas;
import org.pangrova.connector.paimon.handle.PaimonTableIdentity;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.pangrova.connector.paimon.handle.PaimonOutputHandleCodec;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.protocol.write.PhysicalFileWriteBindings;
import org.pangrova.protocol.write.NativeWriteBindings;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.pangrova.connector.api.metadata.NativePhysicalFileAllocation;
import org.pangrova.connector.api.metadata.NativePhysicalFileReceipt;
import org.pangrova.connector.api.metadata.NativePhysicalWriteAllocation;
import org.pangrova.connector.api.metadata.NativePhysicalWriteProvider;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/** Connector SDK metadata authority for Native physical files, with one SDK publication path. */
public final class PaimonNativePhysicalWriteProvider implements NativePhysicalWriteProvider {
    private static final int MINIMUM_LANCE_FILE_BYTES = 40;
    private final Catalog catalog;
    private final String catalogName;

    public PaimonNativePhysicalWriteProvider(Catalog catalog, String catalogName) {
        this.catalog = java.util.Objects.requireNonNull(catalog, "catalog");
        this.catalogName = java.util.Objects.requireNonNull(catalogName, "catalogName");
    }

    @Override
    public void validateAllocation(ConnectorSession session, ConnectorOutputTableHandle output,
            NativePhysicalWriteAllocation allocation) {
        requireSession(session);
        var handle = requireHandle(output);
        var pinned = handle.nativePhysicalWriteAllocation().orElseThrow();
        if (!PhysicalFileWriteBindings.template(pinned).equals(PhysicalFileWriteBindings.template(allocation))) {
            throw new IllegalArgumentException("Physical output differs from its persisted SDK allocation");
        }
        var table = load(handle);
        var actual = PaimonPhysicalSchemas.forAppend(table.schema());
        if (actual.schemaId() != allocation.schema().schemaId()
                || !Arrays.equals(actual.physicalSchema(), allocation.schema().physicalSchema())
                || !actual.columns().equals(allocation.schema().columns())
                || !policy(table).equals(allocation.policy())) {
            throw new IllegalArgumentException("Physical schema/declaration differs from current SDK table authority");
        }
        PaimonPhysicalWriteAllocations.validateSource(table, allocation);
    }

    @Override
    public List<byte[]> prepareCommitMetadata(ConnectorSession session, ConnectorOutputTableHandle output,
            List<NativePhysicalFileReceipt> receipts, DistributedWriteCommitContext context) {
        var handle = requireHandle(output);
        var allocation = handle.nativePhysicalWriteAllocation().orElseThrow();
        requireSession(session);
        validateCommitContext(session, handle, context);
        var table = load(handle);
        boolean published = table.snapshotManager().latestSnapshotOfUser(context.commitFingerprint()).isPresent();
        if (!published) validateAllocation(session, handle, allocation);
        var locations = new HashSet<String>();
        for (var receipt : receipts) {
            validateReceipt(allocation, receipt, context.executionId());
            if (!locations.add(receipt.allocation().location().opaqueLocation())) {
                throw new IllegalArgumentException("Duplicate winning physical file receipt");
            }
            if (!published) PaimonPhysicalObjectVerifier.verify(table, receipt);
        }
        return List.of(PaimonPhysicalCommitMessages.append(handle, receipts));
    }

    @Override
    public void abortUncommittedFiles(ConnectorSession session, ConnectorOutputTableHandle output,
            List<NativePhysicalFileAllocation> allocations, DistributedWriteCommitContext context) {
        requireSession(session);
        var handle = requireHandle(output);
        validateCommitContext(session, handle, context);
        var allocation = handle.nativePhysicalWriteAllocation().orElseThrow();
        var table = load(handle);
        // Callers reach this only after the durable journal records a non-publication
        // disposition. The format marker must still be absent before any object is removed.
        if (table.snapshotManager().latestSnapshotOfUser(context.commitFingerprint()).isPresent()) {
            throw new IllegalStateException("Physical output is already published; refusing cleanup");
        }
        var permitted = allocation.files().stream()
                .map(NativePhysicalFileAllocation::location).toList();
        String prefix = table.location().toString() + "/";
        for (var candidate : allocations) {
            if (!permitted.contains(candidate.location())
                    || !candidate.location().opaqueLocation().startsWith(prefix)) {
                throw new IllegalArgumentException(
                        "Physical cleanup target differs from the exact bound allocation");
            }
        }
        for (var candidate : allocations) {
            table.fileIO().deleteQuietly(
                    new org.apache.paimon.fs.Path(candidate.location().opaqueLocation()));
        }
    }

    private void validateCommitContext(ConnectorSession session, PaimonOutputTableHandle handle,
            DistributedWriteCommitContext context) {
        if (!session.getUser().equals(context.principal())
                || !context.tenantId().equals(session.getProperties().get("tenant_id"))
                || context.baseSnapshotId() != handle.baseSnapshotId()) {
            throw new IllegalArgumentException("Physical publication differs from its authorized durable context");
        }
        try {
            var output = WriteOutputBinding.parseFrom(context.writeOutputBinding());
            var physical = output.getPhysical();
            var expected = PhysicalFileWriteBindings.template(handle.nativePhysicalWriteAllocation().orElseThrow())
                    .toBuilder().setCatalog(physical.getCatalog()).setOutputHandle(physical.getOutputHandle()).build();
            var codec = new PaimonOutputHandleCodec();
            if (!output.hasPhysical() || !physical.equals(expected)
                    || !physical.getCatalog().getCatalogName().equals(catalogName)
                    || !physical.getCatalog().getConnectorProvider().equals("paimon")
                    || !Arrays.equals(codec.serialize(handle),
                            physical.getOutputHandle().getPayload().toByteArray())) {
                throw new IllegalArgumentException(
                        "Physical publication differs from its exact persisted output binding");
            }
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Malformed durable physical output binding", failure);
        }
    }

    private static void validateReceipt(NativePhysicalWriteAllocation allocation,
            NativePhysicalFileReceipt receipt, String executionId) {
        try {
            var attempt = TaskAttemptId.parseFrom(receipt.allocation().taskAttempt());
            var expected = NativeWriteBindings.bindPhysical(PhysicalFileWriteBindings.template(allocation), attempt);
            var metrics = receipt.metrics();
            if (!attempt.getQueryOrJobId().equals(executionId)
                    || !expected.getFilesList().contains(PhysicalFileWriteBindings.fileAllocation(receipt.allocation()))
                    || metrics.schemaId() != allocation.schema().schemaId()
                    || metrics.sizeBytes() < MINIMUM_LANCE_FILE_BYTES
                    || metrics.encodingMajor() != 0 || metrics.encodingMinor() != 3
                    || metrics.minSequence() != Math.addExact(receipt.allocation().restoredMaxSequence(), 1)
                    || metrics.maxSequence() != Math.addExact(metrics.minSequence(), metrics.physicalRows() - 1)) {
                throw new IllegalArgumentException("Physical receipt differs from its exact winning allocation");
            }
            validateStatistics(allocation, receipt);
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Malformed physical file task attempt", failure);
        }
    }

    private static void validateStatistics(NativePhysicalWriteAllocation allocation,
                                           NativePhysicalFileReceipt receipt) {
        var fields = new HashSet<Integer>();
        for (var statistics : receipt.columns()) {
            var declared = allocation.schema().columns().stream()
                    .filter(column -> column.fieldId() == statistics.fieldId()).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Receipt statistics field is not allocated"));
            if (!fields.add(statistics.fieldId()) || !declared.name().equals(statistics.name())
                    || !statistics.collectionPolicy().equals("SDK_DISABLED")
                    || statistics.minimum().isPresent() || statistics.maximum().isPresent()
                    || (statistics.nullCount().isPresent()
                        && statistics.nullCount().getAsLong() > receipt.metrics().physicalRows())) {
                throw new IllegalArgumentException("Physical receipt statistics differ from actual SDK policy");
            }
        }
    }

    private static NativePhysicalWriteAllocation.Policy policy(FileStoreTable table) {
        var schema = table.schema();
        var buckets = schema.bucketKeys().stream().map(name -> schema.fields().stream()
                .filter(field -> field.name().equals(name)).findFirst().orElseThrow().id()).toList();
        var partitions = schema.partitionKeys().stream().map(name -> schema.fields().stream()
                .filter(field -> field.name().equals(name)).findFirst().orElseThrow().id()).toList();
        var primary = schema.primaryKeys().stream().map(name -> schema.fields().stream()
                .filter(field -> field.name().equals(name)).findFirst().orElseThrow().id()).toList();
        return new NativePhysicalWriteAllocation.Policy("INSERT", partitions, primary, buckets,
                schema.numBuckets(), PaimonPhysicalWriteAllocations.writerBufferHint(schema.options()),
                schema.options());
    }

    private void requireSession(ConnectorSession session) {
        if (session == null || session.getUser() == null || session.getUser().isBlank()
                || !session.getCatalog().equals(java.util.Optional.of(catalogName))
                || session.getProperties().getOrDefault("tenant_id", "").isBlank()) {
            throw new IllegalArgumentException(
                    "Physical metadata access requires its authorized catalog/tenant session");
        }
    }

    private static PaimonOutputTableHandle requireHandle(ConnectorOutputTableHandle output) {
        if (!(output instanceof PaimonOutputTableHandle handle) || !handle.mode().equals("INSERT")
                || handle.nativePhysicalWriteAllocation().isEmpty()) {
            throw new IllegalArgumentException("Native physical publication requires an allocated SDK append handle");
        }
        return handle;
    }

    private FileStoreTable load(PaimonOutputTableHandle handle) {
        try {
            var table = PaimonTableIdentity.loadFresh(catalog,
                    Identifier.create(handle.schemaName(), handle.tableName()));
            return org.pangrova.connector.paimon.handle.PaimonTableIdentity.requireSame(
                    table, handle.tableUuid(), handle.tableLocation(), handle.schemaFingerprint(),
                    "Physical output");
        } catch (Catalog.TableNotExistException failure) {
            throw new IllegalStateException("Physical output table no longer exists", failure);
        }
    }
}
