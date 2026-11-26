package org.pangrova.protocol.task;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.TaskAdmissionSealRequest;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.security.DigestOutputStream;

/** Exact control identity shared by network submission and its sole cancellation/seal path. */
public final class CanonicalTaskAdmissionSeals {
    private CanonicalTaskAdmissionSeals() {}

    /**
     * Builds cancellation identity from the original validated NETWORK task, not a local JNI injection.
     * @param request original network task request
     * @return exact admission seal identity and deterministic checksum
     * @throws IllegalArgumentException for a noncanonical task request
     */
    public static TaskAdmissionSealRequest create(TaskRequest request) {
        CanonicalTaskRequests.validateForAdmission(request);
        return TaskAdmissionSealRequest.newBuilder().setProtocol(request.getProtocol())
                .setAttempt(request.getAttempt()).setDeadlineEpochMs(request.getDeadlineEpochMs())
                .setCanonicalRequestChecksum(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(digest(request)))).build();
    }

    /**
     * Hashes without allocating a second serialized plan-sized byte array.
     * @param request original network task request
     * @return deterministic SHA-256 digest
     * @throws IllegalArgumentException if local storage bindings have been injected
     * @throws UncheckedIOException if deterministic serialization fails
     */
    public static byte[] digest(TaskRequest request) {
        if (request.getWorkerLocalStorageBindingsCount() != 0 || request.hasWorkerLocalCopyInput()) {
            throw new IllegalArgumentException("Admission digest must describe the original NETWORK request");
        }
        var digest = DigestUtils.sha256();
        try (var sink = new DigestOutputStream(OutputStream.nullOutputStream(), digest)) {
            var coded = CodedOutputStream.newInstance(sink);
            coded.useDeterministicSerialization();
            request.writeTo(coded);
            coded.flush();
            return digest.digest();
        } catch (IOException failure) {
            throw new UncheckedIOException("Cannot hash canonical task admission", failure);
        }
    }

    /**
     * Rejects incomplete, unknown, out-of-domain or path-conflicting canonical control identities.
     * @param request admission seal to validate
     * @param pathTaskId exact task identifier from the control endpoint
     * @throws IllegalArgumentException for invalid or conflicting control identity
     */
    public static void validate(TaskAdmissionSealRequest request, String pathTaskId) {
        if (request == null || !request.getUnknownFields().asMap().isEmpty()
                || !request.hasProtocol() || !request.hasAttempt() || !request.hasCanonicalRequestChecksum()
                || request.getDeadlineEpochMs() <= 0 || !request.getAttempt().getTaskId().equals(pathTaskId)) {
            throw new IllegalArgumentException("Admission seal is missing its exact control authority");
        }
        CanonicalProtocolIdentity.validate(request.getProtocol());
        CanonicalTaskRequests.validateAttempt(request.getAttempt());
        var checksum = request.getCanonicalRequestChecksum();
        if (!checksum.getUnknownFields().asMap().isEmpty()
                || checksum.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || checksum.getDigest().size() != 32) {
            throw new IllegalArgumentException("Admission seal checksum is not canonical SHA-256");
        }
    }

    /**
     * Caller must hold the real sealed-admission/no-owner fact, including completed grant rollback.
     * @param request exact validated admission seal
     * @return exit-only cancelled receipt for the caller's actual no-owner fact
     * @throws IllegalArgumentException for a noncanonical admission seal
     */
    public static TaskReport closedWithoutOwner(TaskAdmissionSealRequest request) {
        validate(request, request.getAttempt().getTaskId());
        return noOwnerReceipt(request, "TASK_ADMISSION_CLOSED", "Worker admission closed without Native ownership");
    }

    /**
     * Projects the admission authority's proof that an expired request has no remaining execution owner.
     * @param request exact admission seal bound to the Worker incarnation that established absence
     * @return exit-only receipt describing current absence
     * @throws IllegalArgumentException for an invalid or unbound admission seal
     */
    public static TaskReport expiredWithoutLiveOwner(TaskAdmissionSealRequest request) {
        validate(request, request.getAttempt().getTaskId());
        if (!request.getAttempt().hasWorkerExecutionIdentity()) {
            throw new IllegalArgumentException("Expired admission receipt requires exact Worker execution identity");
        }
        return noOwnerReceipt(request, "TASK_ADMISSION_EXPIRED_WITHOUT_LIVE_OWNER",
                "Worker admission expired and no execution owner remains");
    }

    private static TaskReport noOwnerReceipt(TaskAdmissionSealRequest request, String code, String message) {
        return TaskReport.newBuilder().setAttempt(request.getAttempt()).setState(TaskState.TASK_STATE_CANCELLED)
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED)
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER)
                .setError(ExecutionError.newBuilder().setErrorClass(ErrorClass.ERROR_CLASS_CANCELLED)
                        .setCode(code).setMessage(message).setFailedComponent("worker-admission")).build();
    }

    /** Caller must prove retirement of the exact durable target incarnation before invoking this. */
    public static TaskReport retiredIncarnation(TaskAdmissionSealRequest request) {
        validate(request, request.getAttempt().getTaskId());
        if (!request.getAttempt().hasWorkerExecutionIdentity()) {
            throw new IllegalArgumentException("Retirement receipt requires exact Worker execution identity");
        }
        return TaskReport.newBuilder().setAttempt(request.getAttempt()).setState(TaskState.TASK_STATE_CANCELLED)
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED)
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_WORKER_INCARNATION_RETIRED)
                .setError(ExecutionError.newBuilder().setErrorClass(ErrorClass.ERROR_CLASS_CANCELLED)
                        .setCode("WORKER_INCARNATION_RETIRED")
                        .setMessage("Target Worker incarnation has physically relinquished all execution owners")
                        .setFailedComponent("worker-admission")).build();
    }

    /**
     * Projects bounded exit-only metadata from an authenticated, validated actual receipt.
     * @param actual actual terminal report with execution drain evidence
     * @return compact drain receipt without publication payloads
     * @throws IllegalArgumentException for absent, conflicting or unbounded evidence
     */
    public static TaskReport compactExecutionDrainReceipt(TaskReport actual) {
        if (!actual.hasAttempt() || !actual.getUnknownFields().asMap().isEmpty()
                || actual.getExecutionDrainState() == ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED
                || actual.getExecutionDrainState() == ExecutionDrainState.UNRECOGNIZED
                || actual.getState() != TaskState.TASK_STATE_FINISHED
                        && actual.getState() != TaskState.TASK_STATE_FAILED
                        && actual.getState() != TaskState.TASK_STATE_CANCELLED) {
            throw new IllegalArgumentException("Cannot compact a report without actual terminal drain evidence");
        }
        CanonicalTaskRequests.validateAttempt(actual.getAttempt());
        requireDrainAuthority(actual);
        var result = TaskReport.newBuilder().setAttempt(actual.getAttempt()).setState(actual.getState())
                .setReportOrigin(actual.getReportOrigin()).setExecutionDrainState(actual.getExecutionDrainState());
        if (actual.hasError()) {
            result.setError(compactError(actual.getError()));
        }
        return result.build();
    }

    private static ExecutionError compactError(ExecutionError error) {
        if (error.getCode().length() > 256 || error.getFailedComponent().length() > 256
                || !error.getUnknownFields().asMap().isEmpty() || error.getCode().isBlank()
                || error.getFailedComponent().isBlank() || error.getMessage().isBlank()
                || error.getErrorClass() == ErrorClass.ERROR_CLASS_UNSPECIFIED
                || error.getErrorClass() == ErrorClass.UNRECOGNIZED) {
            throw new IllegalArgumentException("Drain error identity exceeds its bounded control contract");
        }
        // The full diagnostic stays in the ordinary TaskReport. Compact
        // retention keeps classification/retryability and <= 4 KiB UTF-8.
        String message = error.getMessage();
        int points = Math.min(1024, message.codePointCount(0, message.length()));
        return error.toBuilder().clearFailedPartitionIds()
                .setMessage(message.substring(0, message.offsetByCodePoints(0, points))).build();
    }

    private static void requireDrainAuthority(TaskReport actual) {
        boolean failed = actual.getState() == TaskState.TASK_STATE_FAILED;
        boolean cancelled = actual.getState() == TaskState.TASK_STATE_CANCELLED;
        if (!failed && !cancelled && actual.hasError() || failed && !actual.hasError()) {
            throw new IllegalArgumentException("Terminal drain state contradicts its actual error");
        }
        switch (actual.getReportOrigin()) {
            case TASK_REPORT_ORIGIN_NATIVE -> { }
            case TASK_REPORT_ORIGIN_WORKER_RECONCILED -> {
                if ((!failed && !cancelled) || !actual.hasError()) {
                    throw new IllegalArgumentException(
                            "Worker reconciliation cannot invent successful Native execution");
                }
            }
            case TASK_REPORT_ORIGIN_WORKER_EXECUTED -> {
                if (actual.getState() != TaskState.TASK_STATE_FINISHED || actual.hasError()) {
                    throw new IllegalArgumentException(
                            "Worker-JVM execution receipt must be a successful terminal report");
                }
            }
            default -> throw new IllegalArgumentException("Drain receipt has no canonical report origin");
        }
        if (actual.getExecutionDrainState()
                == ExecutionDrainState.EXECUTION_DRAIN_STATE_WORKER_INCARNATION_RETIRED
                && (!cancelled || actual.getReportOrigin() != TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED
                        || !actual.getAttempt().hasWorkerExecutionIdentity() || actual.hasNativeTaskStats()
                        || !actual.getError().getCode().equals("WORKER_INCARNATION_RETIRED"))) {
            throw new IllegalArgumentException("Incarnation retirement receipt contradicts its physical authority");
        }
        if (actual.getExecutionDrainState()
                == ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER
                && (actual.getReportOrigin() != TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED
                        || actual.hasNativeTaskStats())) {
            throw new IllegalArgumentException("No-owner receipt contradicts its actual execution authority");
        }
        if (actual.getExecutionDrainState()
                == ExecutionDrainState.EXECUTION_DRAIN_STATE_WORKER_TASK_COMPLETED
                && (actual.getReportOrigin() != TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_EXECUTED
                        || actual.getState() != TaskState.TASK_STATE_FINISHED
                        || actual.hasError() || actual.hasNativeTaskStats())) {
            throw new IllegalArgumentException(
                    "Worker-JVM completion receipt contradicts its actual execution authority");
        }
        if (actual.getReportOrigin() == TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_EXECUTED
                && actual.getExecutionDrainState() != ExecutionDrainState.EXECUTION_DRAIN_STATE_WORKER_TASK_COMPLETED) {
            throw new IllegalArgumentException("Worker-JVM completion requires its exact drain evidence");
        }
    }
}
