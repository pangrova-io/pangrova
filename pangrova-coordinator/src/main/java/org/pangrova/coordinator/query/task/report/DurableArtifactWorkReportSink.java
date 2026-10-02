package org.pangrova.coordinator.query.task.report;

import org.pangrova.metadata.artifact.build.ArtifactBuildOutput;
import org.pangrova.metadata.artifact.contract.ArtifactBuildOutputScope;
import org.pangrova.metadata.artifact.build.ArtifactBuildStore;
import org.pangrova.metadata.artifact.build.ArtifactPageCompletion;
import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactStageExecutionStore;
import org.pangrova.protocol.v2.ArtifactObject;
import org.pangrova.protocol.v2.ArtifactWorkReport;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.TaskReport;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Objects;

/** Converts validated canonical reports into durable metadata-only page commits. */
public final class DurableArtifactWorkReportSink implements ArtifactWorkReportSink {

    private final ArtifactBuildStore store;
    private final Clock clock;
    private final JdbcArtifactStageExecutionStore stages;

    /** Uses the same persistent page engine for artifact outputs and relational producer winners. */
    public DurableArtifactWorkReportSink(ArtifactBuildStore store, JdbcArtifactStageExecutionStore stages) {
        this(store, stages, Clock.systemUTC());
    }

    DurableArtifactWorkReportSink(ArtifactBuildStore store, JdbcArtifactStageExecutionStore stages, Clock clock) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.stages = Objects.requireNonNull(stages, "stages");
    }

    @Override
    public boolean commit(String tenantId, TaskReport status, byte[] originalWireBytes) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        Objects.requireNonNull(status, "status must not be null");
        requireOriginalReport(status, originalWireBytes);
        requireFinishedAttempt(status);
        var job = store.findExecutionJob(status.getAttempt().getQueryOrJobId()).orElseThrow(() ->
                new IllegalArgumentException("Native artifact completion has no durable execution association"));
        if (!job.tenantId().equals(tenantId)) {
            throw new IllegalArgumentException(
                    "Native artifact completion differs from its exact tenant/job association");
        }
        if (!status.hasArtifactWork()) return completeProducer(job, status, originalWireBytes);
        ArtifactWorkReport report = status.getArtifactWork();
        if (!status.getAttempt().getQueryOrJobId().equals(report.getJobId())) {
            throw new IllegalArgumentException("Native artifact report differs from its durable execution");
        }
        byte[] bytes = originalWireBytes.clone();
        return store.completePage(new ArtifactPageCompletion(
                tenantId,
                job.jobId(), report.getPageId(), report.getFencingToken(),
                status.getAttempt().getControlEpoch(), bytes, sha256Hex(bytes),
                report.getOutputsList().stream()
                        .map(DurableArtifactWorkReportSink::toOutput)
                        .toList(),
                clock.instant(), new org.pangrova.metadata.artifact.build.ArtifactExecutionOwner(
                        status.getAttempt().getExecutionOwner().getOwnerId(),
                        status.getAttempt().getExecutionOwner().getOwnerIncarnation(),
                        status.getAttempt().getExecutionOwner().getFencingToken())));
    }

    @Override
    public boolean ownsExecution(String tenantId, String executionId) {
        return store.findExecutionJob(executionId).map(key -> {
            if (!key.tenantId().equals(tenantId)) {
                throw new IllegalArgumentException("Durable execution routing belongs to another tenant");
            }
            return true;
        }).orElse(false);
    }

    private boolean completeProducer(org.pangrova.metadata.artifact.build.ArtifactBuildJobKey job,
            TaskReport status, byte[] bytes) {
        var attempt = status.getAttempt();
        var owner = attempt.getExecutionOwner();
        return stages.completeProducer(new JdbcArtifactStageExecutionStore.ProducerCompletion(job,
                new org.pangrova.metadata.artifact.build.ArtifactExecutionOwner(
                        owner.getOwnerId(), owner.getOwnerIncarnation(), owner.getFencingToken()),
                attempt.getStageId(), attempt.getPartitionId(), attempt.getPlanEpoch(), attempt.getAttemptId(),
                attempt.getFencingToken(), attempt.getControlEpoch(), bytes, clock.instant()));
    }

    private static void requireFinishedAttempt(TaskReport status) {
        if (!status.hasAttempt() || status.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED) {
            throw new IllegalArgumentException("Native durable completion has no finished fenced report");
        }
        if (!status.getAttempt().hasExecutionOwner()) {
            throw new IllegalArgumentException("Native artifact completion has no execution owner");
        }
    }

    private static void requireOriginalReport(TaskReport status, byte[] originalWireBytes) {
        Objects.requireNonNull(originalWireBytes, "originalWireBytes");
        try {
            if (originalWireBytes.length == 0 || originalWireBytes.length > 4 * 1024 * 1024
                    || !TaskReport.parseFrom(originalWireBytes).equals(status)) {
                throw new IllegalArgumentException(
                        "Durable completion must retain the original validated Native wire report");
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Original Native report bytes are malformed", malformed);
        }
    }

    private static ArtifactBuildOutput toOutput(ArtifactObject object) {
        return new ArtifactBuildOutput(
                object.getObjectId(), object.getOpaqueUri(), object.getObjectRole(),
                object.getSourceObjectId(), object.getOrdinal(), scope(object), object.getSizeBytes(),
                object.getRowCount(), checksumHex(object.getChecksum()),
                object.getEncryptionKeyRef());
    }

    private static ArtifactBuildOutputScope scope(ArtifactObject object) {
        return switch (object.getScope()) {
            case ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT ->
                    ArtifactBuildOutputScope.SOURCE_OBJECT;
            case ARTIFACT_OBJECT_SCOPE_WORK_UNIT -> ArtifactBuildOutputScope.WORK_UNIT;
            case ARTIFACT_OBJECT_SCOPE_GENERATION -> ArtifactBuildOutputScope.GENERATION;
            default -> throw new IllegalArgumentException(
                    "artifact object scope must be explicit");
        };
    }

    private static String checksumHex(Checksum checksum) {
        if (checksum.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || checksum.getDigest().size() != 32) {
            throw new IllegalArgumentException("artifact object checksum must be SHA-256");
        }
        return HexFormat.of().formatHex(checksum.getDigest().toByteArray());
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
