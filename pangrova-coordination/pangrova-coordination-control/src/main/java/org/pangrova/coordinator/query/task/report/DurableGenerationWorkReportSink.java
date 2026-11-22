package org.pangrova.coordinator.query.task.report;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.generation.api.page.GenerationPageCompletion;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore;
import org.pangrova.protocol.v2.ArtifactObject;
import org.pangrova.protocol.v2.ArtifactWorkReport;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.TaskReport;

import java.time.Clock;
import java.util.HexFormat;
import java.util.Objects;

/** Converts validated canonical reports into durable metadata-only page commits. */
public final class DurableGenerationWorkReportSink implements GenerationWorkReportSink {

    private final GenerationJobStore store;
    private final Clock clock;
    private final GenerationStageExecutionStore stages;

    /** Uses the same persistent page engine for artifact outputs and relational producer winners. */
    public DurableGenerationWorkReportSink(GenerationJobStore store, GenerationStageExecutionStore stages) {
        this(store, stages, Clock.systemUTC());
    }

    DurableGenerationWorkReportSink(GenerationJobStore store, GenerationStageExecutionStore stages, Clock clock) {
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
        return store.completePage(new GenerationPageCompletion(
                tenantId,
                job.jobId(), report.getPageId(), report.getFencingToken(),
                status.getAttempt().getControlEpoch(), bytes, DigestUtils.sha256Hex(bytes),
                report.getOutputsList().stream()
                        .map(DurableGenerationWorkReportSink::toOutput)
                        .toList(),
                clock.instant(), new org.pangrova.generation.api.execution.GenerationExecutionOwner(
                        status.getAttempt().getExecutionOwner().getOwnerId(),
                        status.getAttempt().getExecutionOwner().getOwnerIncarnation(),
                        status.getAttempt().getExecutionOwner().getFencingToken())));
    }

    private boolean completeProducer(org.pangrova.generation.api.identity.GenerationJobKey job,
            TaskReport status, byte[] bytes) {
        var attempt = status.getAttempt();
        var owner = attempt.getExecutionOwner();
        return stages.completeProducer(new GenerationStageExecutionStore.ProducerCompletion(job,
                new org.pangrova.generation.api.execution.GenerationExecutionOwner(
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

    private static GenerationOutput toOutput(ArtifactObject object) {
        return new GenerationOutput(
                object.getObjectId(), object.getOpaqueUri(), object.getObjectRole(),
                object.getSourceObjectId(), object.getOrdinal(), scope(object), object.getSizeBytes(),
                object.getRowCount(), checksumHex(object.getChecksum()),
                object.getEncryptionKeyRef());
    }

    private static GenerationOutputScope scope(ArtifactObject object) {
        return switch (object.getScope()) {
            case ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT ->
                    GenerationOutputScope.SOURCE_OBJECT;
            case ARTIFACT_OBJECT_SCOPE_WORK_UNIT -> GenerationOutputScope.WORK_UNIT;
            case ARTIFACT_OBJECT_SCOPE_GENERATION -> GenerationOutputScope.GENERATION;
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
}
