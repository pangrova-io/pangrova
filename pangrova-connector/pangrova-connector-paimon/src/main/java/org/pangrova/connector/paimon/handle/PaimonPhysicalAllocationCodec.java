package org.pangrova.connector.paimon.handle;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.connector.paimon.write.PaimonPhysicalSchemas;
import org.pangrova.protocol.PhysicalFileWriteBindings;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.PhysicalFileWriteBinding;
import org.pangrova.spi.connector.metadata.NativePhysicalFileAllocation;
import org.pangrova.spi.connector.metadata.NativePhysicalWriteAllocation;

/** Persisted canonical allocation is cross-checked against the complete persisted SDK row declaration. */
final class PaimonPhysicalAllocationCodec {
    private PaimonPhysicalAllocationCodec() {}

    static byte[] encode(NativePhysicalWriteAllocation allocation) {
        return PhysicalFileWriteBindings.bytes(PhysicalFileWriteBindings.template(allocation));
    }

    static NativePhysicalWriteAllocation decode(byte[] bytes, PaimonOutputTableHandle output) {
        try {
            var value = PhysicalFileWriteBinding.parseFrom(bytes);
            var policy = value.getPolicy();
            var schema = PaimonPhysicalSchemas.fromRowType(value.getSchemaId(),
                    PaimonTableHandleFactory.parseRowType(output), policy.getOptionsMap());
            var files = value.getFilesList().stream().map(file -> new NativePhysicalFileAllocation(
                    new NativePhysicalFileAllocation.Location(file.getOpaqueLocation(), file.getFileName(),
                            file.getFileSource()), file.getPartitionCount() == 0 ? new byte[0]
                            : PhysicalFileWriteBindings.bytes(DataSplit.newBuilder()
                                    .addAllPartitionValues(file.getPartitionList()).build()),
                    file.getBucketId(), file.getFileOrdinal(), file.getRestoredMaxSequence(),
                    file.getRestoredFileSetChecksum().getDigest().toByteArray(),
                    file.hasTaskAttempt()
                            ? PhysicalFileWriteBindings.bytes(file.getTaskAttempt()) : new byte[0])).toList();
            var allocation = new NativePhysicalWriteAllocation(new NativePhysicalWriteAllocation.Identity(
                    value.getWriteId(), value.getGeneration()), schema,
                    new NativePhysicalWriteAllocation.Policy(policy.getWriteMode(), policy.getPartitionFieldIdsList(),
                            policy.getPrimaryKeyFieldIdsList(), policy.getBucketKeyFieldIdsList(),
                            policy.getBucketCount(), policy.getWriterBufferHintBytes(), policy.getOptionsMap()),
                    value.getAllocationPrefix(), value.getBaseSnapshotId(), files);
            if (!value.equals(PhysicalFileWriteBindings.template(allocation))) {
                throw new IllegalArgumentException(
                        "Persisted physical allocation differs from its exact SDK declaration");
            }
            return allocation;
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Malformed persisted physical allocation", failure);
        }
    }
}
