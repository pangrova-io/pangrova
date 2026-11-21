package org.pangrova.protocol.write;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import org.pangrova.protocol.type.DeclaredLogicalTypes;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.PhysicalFileAllocation;
import org.pangrova.protocol.v2.PhysicalFileWriteBinding;
import org.pangrova.protocol.v2.PhysicalWriteColumn;
import org.pangrova.protocol.v2.PhysicalWritePolicy;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.connector.api.metadata.NativePhysicalFileAllocation;
import org.pangrova.connector.api.metadata.NativePhysicalWriteAllocation;

import java.io.IOException;
import java.security.MessageDigest;

/** Canonical connector-owned physical metadata; no row payload or credentials. */
public final class PhysicalFileWriteBindings {
    private PhysicalFileWriteBindings() {}

    /** Serializes the allocation before catalog and exact output-handle admission are attached. */
    public static PhysicalFileWriteBinding template(NativePhysicalWriteAllocation allocation) {
        var schema = allocation.schema();
        validatePolicyFields(allocation);
        if (!MessageDigest.isEqual(schema.sha256(), ProtocolChecksums.sha256(
                schema.physicalSchema()).getDigest().toByteArray())) {
            throw new IllegalArgumentException("Physical schema checksum differs from allocated metadata");
        }
        var binding = PhysicalFileWriteBinding.newBuilder()
                .setWriteId(allocation.identity().writeId()).setGeneration(allocation.identity().generation())
                .setBaseSnapshotId(allocation.baseSnapshotId()).setSchemaId(schema.schemaId())
                .setFileFormat(fileFormat(allocation.policy()))
                .setPhysicalSchema(ByteString.copyFrom(schema.physicalSchema()))
                .setPhysicalSchemaChecksum(ProtocolChecksums.sha256(schema.physicalSchema()))
                .setPolicy(policy(allocation.policy())).setAllocationPrefix(allocation.allocationPrefix());
        for (var column : schema.columns()) {
            binding.addColumns(PhysicalWriteColumn.newBuilder().setFieldId(column.fieldId())
                    .setName(column.name()).setLogicalType(DeclaredLogicalTypes.logicalType(column.declaredType()))
                    .setNullable(column.nullable()));
        }
        allocation.files().forEach(file -> binding.addFiles(fileAllocation(file)));
        return binding.build();
    }

    /** Deterministic protobuf bytes are used for both persisted templates and integrity hashes. */
    public static byte[] bytes(MessageLite message) {
        byte[] bytes = new byte[message.getSerializedSize()];
        var output = CodedOutputStream.newInstance(bytes);
        output.useDeterministicSerialization();
        try {
            message.writeTo(output);
            output.checkNoSpaceLeft();
            return bytes;
        } catch (IOException failure) {
            throw new IllegalStateException("In-memory physical metadata serialization failed", failure);
        }
    }

    /** Seals the entire allocation, including the complete canonical task attempt when bound. */
    public static PhysicalFileAllocation sealed(PhysicalFileAllocation.Builder file) {
        file.clearAllocationChecksum();
        return file.setAllocationChecksum(ProtocolChecksums.sha256(bytes(file.build()))).build();
    }

    private static PhysicalWritePolicy policy(NativePhysicalWriteAllocation.Policy policy) {
        var value = PhysicalWritePolicy.newBuilder().setWriteMode(policy.writeMode())
                .addAllPartitionFieldIds(policy.partitionFields()).addAllPrimaryKeyFieldIds(policy.primaryKeyFields())
                .addAllBucketKeyFieldIds(policy.bucketKeyFields()).setBucketCount(policy.bucketCount())
                .setWriterBufferHintBytes(policy.writerBufferHintBytes())
                .putAllOptions(policy.options()).setMergeEngine(policy.options().getOrDefault("merge-engine", ""));
        return value.setDeclarationChecksum(ProtocolChecksums.sha256(bytes(value.build()))).build();
    }

    /** Complete generic file provenance used to compare a receipt with its sealed allocation. */
    public static PhysicalFileAllocation fileAllocation(NativePhysicalFileAllocation file) {
        var value = PhysicalFileAllocation.newBuilder().setOpaqueLocation(file.location().opaqueLocation())
                .setFileName(file.location().fileName()).setFileSource(file.location().fileSource())
                .setBucketId(file.bucketId()).setFileOrdinal(file.fileOrdinal())
                .setRestoredMaxSequence(file.restoredMaxSequence()).setRestoredFileSetChecksum(
                        Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                                .setDigest(ByteString.copyFrom(file.restoredFileSetSha256())));
        try {
            if (file.partition().length != 0) {
                var partition = DataSplit.parseFrom(file.partition());
                if (!partition.equals(DataSplit.newBuilder()
                        .addAllPartitionValues(partition.getPartitionValuesList()).build())) {
                    throw new IllegalArgumentException(
                            "Physical partition contains fields outside its canonical values");
                }
                value.addAllPartition(partition.getPartitionValuesList());
            }
            if (file.taskAttempt().length != 0) value.setTaskAttempt(TaskAttemptId.parseFrom(file.taskAttempt()));
            return sealed(value);
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Physical file contains malformed canonical metadata", failure);
        }
    }

    private static void validatePolicyFields(NativePhysicalWriteAllocation allocation) {
        var fields = allocation.schema().columns().stream()
                .map(NativePhysicalWriteAllocation.Column::fieldId).collect(java.util.stream.Collectors.toSet());
        var policy = allocation.policy();
        for (var keys : java.util.List.of(
                policy.partitionFields(), policy.primaryKeyFields(), policy.bucketKeyFields())) {
            if (!fields.containsAll(keys) || keys.stream().distinct().count() != keys.size()) {
                throw new IllegalArgumentException("Physical policy contains unknown or duplicate field identities");
            }
        }
    }

    private static FileFormat fileFormat(NativePhysicalWriteAllocation.Policy policy) {
        return switch (policy.options().getOrDefault("file.format", "").toLowerCase(java.util.Locale.ROOT)) {
            case "lance" -> FileFormat.FILE_FORMAT_LANCE;
            case "parquet" -> FileFormat.FILE_FORMAT_PARQUET;
            case "orc" -> FileFormat.FILE_FORMAT_ORC;
            default -> throw new IllegalArgumentException("Physical file format must be explicitly allocated");
        };
    }
}
