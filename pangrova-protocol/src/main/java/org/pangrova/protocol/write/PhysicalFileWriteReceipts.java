package org.pangrova.protocol.write;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.PhysicalFileReceipt;
import org.pangrova.protocol.v2.PhysicalFileWriteBinding;
import org.pangrova.protocol.v2.PhysicalFileWriteFragment;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.connector.api.metadata.NativePhysicalFileAllocation;
import org.pangrova.connector.api.metadata.NativePhysicalFileReceipt;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Validates winning closed-file receipts against their exact durable allocation. */
public final class PhysicalFileWriteReceipts {
    public static final int CONTRACT_VERSION = 1;
    private static final int MAX_FRAGMENT_BYTES = 16 * 1024 * 1024;
    private static final int MIN_LANCE_FILE_BYTES = 40;

    private PhysicalFileWriteReceipts() {}

    /**
     * Decodes winning task fragments against their exact durable allocation and append metadata.
     *
     * @param template connector-owned allocation before task-attempt binding
     * @param fragments canonical fragments from winning task attempts
     * @return immutable validated receipts, not evidence that their objects exist or are published
     * @throws IllegalArgumentException if fragments are absent, malformed, duplicated, or mismatched
     * @throws ArithmeticException if receipt row or sequence accounting overflows
     */
    public static List<NativePhysicalFileReceipt> decode(
            PhysicalFileWriteBinding template, List<byte[]> fragments) {
        var result = new ArrayList<NativePhysicalFileReceipt>();
        var locations = new HashSet<String>();
        var attempts = new HashSet<org.pangrova.protocol.v2.TaskAttemptId>();
        for (byte[] bytes : fragments) {
            var fragment = parse(bytes);
            validateFragment(template, fragment);
            if (!attempts.add(fragment.getTaskAttempt())) {
                throw new IllegalArgumentException("Duplicate physical write task-attempt fragment");
            }
            var bound = NativeWriteBinder.bindPhysical(template, fragment.getTaskAttempt());
            validateRows(bound, fragment);
            for (var receipt : fragment.getFilesList()) {
                if (!locations.add(receipt.getAllocation().getOpaqueLocation())) {
                    throw new IllegalArgumentException("Duplicate physical object in winning receipts");
                }
                result.add(convert(receipt));
            }
        }
        if (fragments.isEmpty()) throw new IllegalArgumentException("Physical commit requires typed task fragments");
        return List.copyOf(result);
    }

    /**
     * Validates a control-channel fragment against the exact dispatched allocation and attempt.
     *
     * @param bound allocation already bound before task dispatch
     * @param expected trusted attempt from the registered task request
     * @param bytes received canonical physical-file fragment
     * @return validated metadata, not evidence that the referenced objects exist
     * @throws IllegalArgumentException if the fragment or dispatched identity does not match
     */
    public static PhysicalFileWriteFragment validateTaskFragment(
            PhysicalFileWriteBinding bound, TaskAttemptId expected, byte[] bytes) {
        var fragment = parse(bytes);
        validateFragment(bound, fragment);
        if (!fragment.getTaskAttempt().equals(expected)
                || !bound.getAttemptPrefix().equals(
                        NativeWriteBinder.attemptPrefix(bound.getAllocationPrefix(), expected))
                || bound.getFilesList().stream().anyMatch(file -> !file.getTaskAttempt().equals(expected))) {
            throw new IllegalArgumentException("Physical receipt differs from its dispatched task attempt");
        }
        validateRows(bound, fragment);
        return fragment;
    }

    private static void validateRows(PhysicalFileWriteBinding bound, PhysicalFileWriteFragment fragment) {
        long rows = 0;
        var locations = new HashSet<String>();
        for (var receipt : fragment.getFilesList()) {
            validateReceipt(bound, receipt);
            if (!locations.add(receipt.getAllocation().getOpaqueLocation())) {
                throw new IllegalArgumentException("Duplicate physical object in task receipts");
            }
            rows = Math.addExact(rows, receipt.getPhysicalRows());
        }
        if (rows != fragment.getInputRows()) {
            throw new IllegalArgumentException("Physical receipt rows differ from task input rows");
        }
    }

    private static PhysicalFileWriteFragment parse(byte[] bytes) {
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_FRAGMENT_BYTES) {
            throw new IllegalArgumentException("Physical receipt fragment violates its size limit");
        }
        try {
            var fragment = PhysicalFileWriteFragment.parseFrom(bytes);
            if (!fragment.getUnknownFields().asMap().isEmpty()) {
                throw new IllegalArgumentException("Physical receipt contains unknown contract fields");
            }
            return fragment;
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Malformed canonical physical receipt", failure);
        }
    }

    private static void validateFragment(PhysicalFileWriteBinding template, PhysicalFileWriteFragment fragment) {
        if (fragment.getContractVersion() != CONTRACT_VERSION
                || !fragment.getWriteId().equals(template.getWriteId())
                || !fragment.getGeneration().equals(template.getGeneration())
                || !fragment.hasTaskAttempt()
                || !fragment.getPhysicalSchemaChecksum().equals(template.getPhysicalSchemaChecksum())
                || fragment.getInputRows() < 0 || fragment.getInputBytes() < 0
                || fragment.getFilesCount() > template.getFilesCount()) {
            throw new IllegalArgumentException("Physical receipt differs from its durable write/schema identity");
        }
    }

    private static void validateReceipt(PhysicalFileWriteBinding bound, PhysicalFileReceipt receipt) {
        validateAppendAllocation(bound, receipt);
        requireSha256(receipt.getObjectChecksum());
        validateAppendSequence(receipt);
        validateStatistics(bound, receipt);
    }

    private static void validateAppendAllocation(PhysicalFileWriteBinding bound, PhysicalFileReceipt receipt) {
        if (!receipt.getUnknownFields().asMap().isEmpty() || !receipt.hasAllocation()
                || !bound.getFilesList().contains(receipt.getAllocation())
                || receipt.getFileFormat() != FileFormat.FILE_FORMAT_LANCE
                || receipt.getFileFormat() != bound.getFileFormat()
                || receipt.getSchemaId() != bound.getSchemaId()
                || receipt.getSizeBytes() < MIN_LANCE_FILE_BYTES || receipt.getPhysicalRows() <= 0
                || receipt.hasMinKey() || receipt.hasMaxKey() || receipt.getLevel() != 0
                || receipt.getDeleteRowCount() != 0) {
            throw new IllegalArgumentException("Physical object receipt differs from its exact append allocation");
        }
    }

    private static void validateAppendSequence(PhysicalFileReceipt receipt) {
        long expectedMin = Math.addExact(receipt.getAllocation().getRestoredMaxSequence(), 1);
        if (receipt.getMinSequence() != expectedMin
                || receipt.getMaxSequence() != Math.addExact(expectedMin, receipt.getPhysicalRows() - 1)
                || receipt.getEncodingMajor() != 0 || receipt.getEncodingMinor() != 3) {
            throw new IllegalArgumentException("Physical append receipt sequence or actual encoding is invalid");
        }
    }

    private static void validateStatistics(PhysicalFileWriteBinding bound, PhysicalFileReceipt receipt) {
        var fields = new HashSet<Integer>();
        for (var column : receipt.getColumnsList()) {
            var declared = bound.getColumnsList().stream().filter(value -> value.getFieldId() == column.getFieldId())
                    .findFirst().orElseThrow(() -> new IllegalArgumentException("Statistics field is not allocated"));
            if (!fields.add(column.getFieldId()) || !declared.getName().equals(column.getColumnName())
                    || !column.getUnknownFields().asMap().isEmpty()
                    || !column.getCollectionPolicy().equals("SDK_DISABLED")
                    || column.hasMinimum() || column.hasMaximum()
                    || (column.hasNullCount() && (column.getNullCount() < 0
                            || column.getNullCount() > receipt.getPhysicalRows()))) {
                throw new IllegalArgumentException("Physical statistics differ from the supported SDK policy");
            }
        }
    }

    private static void requireSha256(Checksum checksum) {
        if (checksum.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || checksum.getDigest().size() != 32 || !checksum.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Physical receipt requires an exact SHA-256 object checksum");
        }
    }

    private static NativePhysicalFileReceipt convert(PhysicalFileReceipt receipt) {
        var file = receipt.getAllocation();
        var partition = DataSplit.newBuilder().addAllPartitionValues(file.getPartitionList()).build();
        var allocation = new NativePhysicalFileAllocation(new NativePhysicalFileAllocation.Location(
                file.getOpaqueLocation(), file.getFileName(), file.getFileSource()),
                partition.getPartitionValuesCount() == 0 ? new byte[0] : PhysicalFileWriteBindingFactory.bytes(partition),
                file.getBucketId(), file.getFileOrdinal(), file.getRestoredMaxSequence(),
                file.getRestoredFileSetChecksum().getDigest().toByteArray(),
                PhysicalFileWriteBindingFactory.bytes(file.getTaskAttempt()));
        var metrics = new NativePhysicalFileReceipt.Metrics(receipt.getSizeBytes(), receipt.getPhysicalRows(),
                receipt.getSchemaId(), receipt.getEncodingMajor(), receipt.getEncodingMinor(),
                receipt.getMinSequence(), receipt.getMaxSequence());
        var statistics = receipt.getColumnsList().stream().map(column ->
                new NativePhysicalFileReceipt.ColumnStatistics(column.getFieldId(), column.getColumnName(),
                        column.hasNullCount() ? OptionalLong.of(column.getNullCount()) : OptionalLong.empty(),
                        Optional.empty(), Optional.empty(), column.getCollectionPolicy())).toList();
        return new NativePhysicalFileReceipt(
                allocation, metrics, receipt.getObjectChecksum().getDigest().toByteArray(), statistics);
    }
}
