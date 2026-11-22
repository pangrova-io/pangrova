package org.pangrova.coordinator.streaming;

import com.google.protobuf.ByteString;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.protocol.v2.CdcIngestBinding;
import org.pangrova.protocol.v2.CdcSourceRange;
import org.pangrova.protocol.v2.CdcWriteOperation;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.cdc.CdcTaskRange;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Builds the exact CDC task binding from an authorized allocation and admitted ranges.
 *
 * <p>The task-level commit identity is deterministic over the pre-commit allocation
 * and ranges. The journal's final fingerprint adds the returned fragment digests;
 * it is never substituted by this pre-dispatch identity.</p>
 */
public final class CdcTaskBindingFactory {

    private static final String INTENT_VERSION = "pangrova-cdc-task-intent-v2";

    private CdcTaskBindingFactory() {}

    /** Creates the exact canonical CDC task for one authorized allocation and range set. */
    public static TaskRequest create(
            TaskAttemptId attempt,
            SecurityContext security,
            StageResourceGrant grant,
            long deadlineEpochMs,
            StreamState stream,
            CdcWriteAuthority.CdcWritePlan writePlan,
            List<CdcTaskRange> ranges,
            String targetTableUuid,
            long baseSnapshotId) {
        Objects.requireNonNull(attempt, "attempt");
        Objects.requireNonNull(security, "security");
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(writePlan, "writePlan");
        List<CdcTaskRange> ordered = new ArrayList<>(Objects.requireNonNull(ranges, "ranges"));
        ordered.sort(Comparator.comparing(CdcTaskRange::topic)
                .thenComparingInt(CdcTaskRange::partition)
                .thenComparingLong(CdcTaskRange::startOffset)
                .thenComparingLong(CdcTaskRange::endOffset));
        if (ordered.isEmpty()) {
            throw new IllegalArgumentException("CDC task requires at least one admitted range");
        }
        if (targetTableUuid == null || targetTableUuid.isBlank() || baseSnapshotId < -1) {
            throw new IllegalArgumentException("CDC task requires its exact target snapshot identity");
        }
        String commitIntentId = attempt.getTaskId() + ":" + attempt.getPlanEpoch();
        byte[] writeBinding = writePlan.writeBinding().toByteArray();
        List<CdcWriteOperation> operations = CdcWriteAuthority.privileges(stream).stream()
                .map(privilege -> CdcWriteOperation.valueOf("CDC_WRITE_OPERATION_" + privilege.name()))
                .sorted()
                .toList();
        byte[] identity = identity(attempt, stream, writeBinding, ordered,
                targetTableUuid, baseSnapshotId, operations);
        CdcIngestBinding.Builder binding = CdcIngestBinding.newBuilder()
                .setStreamId(stream.streamName())
                .setTenantId(security.getTenantId())
                .setSourceOwnershipEpoch(String.valueOf(stream.epoch()))
                .setSourceProvider(stream.sourceProvider())
                .setFormatId(stream.sourceFormat())
                .setWrite(writePlan.writeBinding())
                .setCommitIntentId(commitIntentId)
                .setCommitIdentitySha256(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(identity)))
                .setTargetTableUuid(targetTableUuid)
                .setBaseSnapshotId(baseSnapshotId)
                .addAllAllowedOperations(operations);
        for (CdcTaskRange range : ordered) {
            binding.addRanges(CdcSourceRange.newBuilder()
                    .setTopic(range.topic()).setPartition(range.partition())
                    .setStartOffset(range.startOffset()).setEndOffset(range.endOffset()));
        }
        return CdcTaskRequests.create(attempt, security, grant, deadlineEpochMs, binding.build());
    }

    private static byte[] identity(
            TaskAttemptId attempt,
            StreamState stream,
            byte[] writeBinding,
            List<CdcTaskRange> ranges,
            String targetTableUuid,
            long baseSnapshotId,
            List<CdcWriteOperation> operations) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                writeString(out, INTENT_VERSION);
                writeString(out, attempt.getQueryOrJobId());
                writeString(out, attempt.getTaskId());
                out.writeLong(attempt.getPlanEpoch());
                out.writeLong(attempt.getFencingToken());
                out.writeLong(attempt.getControlEpoch());
                writeString(out, stream.streamName());
                writeString(out, String.valueOf(stream.epoch()));
                writeString(out, stream.sourceProvider());
                writeString(out, stream.sourceFormat());
                out.writeInt(ranges.size());
                for (CdcTaskRange range : ranges) {
                    writeString(out, range.topic());
                    out.writeInt(range.partition());
                    out.writeLong(range.startOffset());
                    out.writeLong(range.endOffset());
                }
                out.writeInt(writeBinding.length);
                out.write(writeBinding);
                writeString(out, targetTableUuid);
                out.writeLong(baseSnapshotId);
                out.writeInt(operations.size());
                for (var operation : operations) {
                    out.writeInt(operation.getNumber());
                }
            }
            return DigestUtils.sha256().digest(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("Unexpected CDC intent encoding failure", impossible);
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] utf8 = Objects.requireNonNull(value, "CDC intent field must not be null")
                .getBytes(StandardCharsets.UTF_8);
        out.writeInt(utf8.length);
        out.write(utf8);
    }
}
