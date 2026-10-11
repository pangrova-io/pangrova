package org.pangrova.worker.execution;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.worker.ai.invocation.AiInvocationService;
import org.pangrova.worker.exchange.remote.RemoteShuffleByteEgress;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.jni.NativeRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Sole {@link TaskExecutor} implementation -- delegates every call to {@link NativeRuntime} static
 * native methods.
 *
 * <p>Data-plane operations (result batches, shuffle batches, exchange batches) are handled entirely
 * by C++ data exchange servers. Java only issues control-plane commands via {@link
 * #executeCommand}.
 *
 * <p>Implements {@link AutoCloseable} to ensure native resources are released. The {@link #close()}
 * method calls {@link NativeRuntime#nativeReleaseExecutor} if a task handle is active.
 *
 * <p>Requires {@link NativeRuntime#initNativeRuntime} to have been called before any JNI method is
 * invoked. In production, this is guaranteed by {@code NativeRuntimeBootstrap} running before
 * {@code WorkerTaskBootstrap}.
 *
 * <p>Short-lock operation leases protect each exact creation owner without holding locks during JNI
 * or remote cleanup. Exclusive cleanup claims join actual entered operations outside locks. The
 * lifecycle lock seals public admission; close then joins admitted creation transactions and
 * operations.
 */
@ThreadSafe
public final class NativeTaskExecutor implements TaskExecutor, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeTaskExecutor.class);

    private final RemoteShuffleByteEgress externalShuffleEgress;
    private final NativeCreationOwners creationOwners = new NativeCreationOwners();
    private final NativeOperationLifetime globalOperations = new NativeOperationLifetime();

    /** Serializes only in-memory admission with close; never held during JNI. */
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();

    private volatile boolean closed = false;
    private volatile org.pangrova.worker.jni.write.catalog.WorkerCatalogRuntime catalogRuntime;

    /**
     * Supplies the production Worker-local catalog authority before Native dataset task creation.
     */
    public void setCatalogRuntime(
            org.pangrova.worker.jni.write.catalog.WorkerCatalogRuntime runtime) {
        if (!creationOwners.isEmpty()) {
            throw new IllegalStateException("Cannot replace catalogs with Native owners");
        }
        catalogRuntime = java.util.Objects.requireNonNull(runtime);
    }

    /** Creates an executor for the Native Worker-local data plane. */
    public NativeTaskExecutor() {
        this(null);
    }

    /** Creates an executor with the sole external-shuffle JNI egress. */
    public NativeTaskExecutor(RemoteShuffleByteEgress externalShuffleEgress) {
        this.externalShuffleEgress = externalShuffleEgress;
    }

    @Override
    public long createTask(byte[] taskRequestBytes) {
        org.pangrova.protocol.v2.TaskRequest request;
        try {
            request = parseTaskRequest(taskRequestBytes);
        } catch (RuntimeException failure) {
            throw new NativeTaskCreationFailure(0, true, failure);
        }
        var owner = prepareCreation(request);
        try {
            var local =
                    org.pangrova.worker.jni.write.catalog.WorkerNativeStorageTasks.prepare(
                            request, catalogRuntime);
            prepareExternalShuffle(owner);
            AiInvocationService.registerTask(request);
            owner.aiRegistered = true;
            var trace = request.getTraceContext();
            byte[] localBytes = local.toByteArray();
            owner.enteredNative = true;
            owner.handle =
                    NativeRuntime.nativeCreateTaskExecutorWithTraceContext(
                            localBytes, trace.getTraceId(), trace.getSpanId());
            if (owner.handle <= 0) {
                throw new IllegalStateException("JNI creation returned no positive actual handle");
            }
            creationOwners.publish(owner);
            return owner.handle;
        } catch (Throwable failure) {
            throw reconcileCreationFailure(owner, failure);
        } finally {
            owner.constructionExit.complete(null);
        }
    }

    private NativeTaskCreationFailure reconcileCreationFailure(
            NativeCreationOwners.Owner owner, Throwable failure) {
        if (owner.enteredNative && owner.handle == 0) {
            try {
                recoverCreation(owner);
            } catch (Throwable recovery) {
                failure.addSuppressed(recovery);
                return new NativeTaskCreationFailure(owner.handle, failure);
            }
        }
        if (owner.handle > 0) {
            return new NativeTaskCreationFailure(owner.handle, failure);
        }
        if (!owner.enteredNative || owner.nativeClosedWithoutOwner) {
            return creationOwners.rollbackWithoutNativeOwner(owner, externalShuffleEgress, failure);
        }
        return new NativeTaskCreationFailure(0, failure);
    }

    private void recoverCreation(NativeCreationOwners.Owner owner) {
        var lease = globalOperations.enter();
        try {
            NativeCreationRecovery.recover(owner);
            if (owner.handle > 0) {
                creationOwners.publish(owner);
            }
        } finally {
            lease.close();
        }
    }

    private NativeCreationOwners.Owner prepareCreation(
            org.pangrova.protocol.v2.TaskRequest request) {
        lifecycleLock.writeLock().lock();
        try {
            if (closed) {
                throw NativeTaskCreationFailure.closedExecutor(
                        creationOwners.ownsAttempt(request.getAttempt()));
            }
            return prepareOpenCreation(request);
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    private NativeCreationOwners.Owner prepareOpenCreation(
            org.pangrova.protocol.v2.TaskRequest request) {
        try {
            taskIdentity(request);
            return creationOwners.prepare(request);
        } catch (Throwable failure) {
            // No Native/callback operation has run in this local admission
            // section. A pre-existing exact owner must nevertheless be retained.
            throw new NativeTaskCreationFailure(
                    0, !creationOwners.ownsAttempt(request.getAttempt()), failure);
        }
    }

    /** Admits an actual operation; the returned lease must span its entire JNI lifetime. */
    NativeOperationLifetime.Lease acquireHandleLease(long executorHandle) {
        lifecycleLock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException(
                        "Handle " + executorHandle + " is not tracked: executor is closed");
            }
            if (executorHandle == 0) {
                return globalOperations.enter();
            }
            var owner = creationOwners.forHandle(executorHandle);
            if (owner == null || !creationOwners.contains(owner)) {
                throw new IllegalStateException("Handle " + executorHandle + " is not tracked");
            }
            return owner.operations.enter();
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public void startTask(long executorHandle) {
        var lease = acquireHandleLease(executorHandle);
        try {
            String traceId =
                    creationOwners.forHandle(executorHandle).request.getTraceContext().getTraceId();
            NativeRuntime.nativeStartTaskWithTrace(executorHandle, traceId);
        } finally {
            lease.close();
        }
    }

    /** Joins Native before removing the handle; failed cleanup remains retryable. */
    @Override
    public java.util.Optional<byte[]> releaseTask(long executorHandle) {
        var owner = creationOwners.forHandle(executorHandle);
        if (owner == null) {
            return java.util.Optional.empty();
        }
        owner.awaitConstructionExit();
        try (var claim = owner.operations.claim(true)) {
            claim.awaitReaders();
            return releaseOwner(owner);
        }
    }

    private java.util.Optional<byte[]> releaseOwner(NativeCreationOwners.Owner owner) {
        if (!creationOwners.contains(owner)) {
            return java.util.Optional.empty();
        }
        revokeAiTask(owner.handle);
        byte[] nativeReport = owner.releaseReceipt;
        if (nativeReport == null) {
            nativeReport = NativeRuntime.nativeReleaseExecutor(owner.handle);
            owner.releaseReceipt = nativeReport;
        }
        byte[] finalReport = finalReleaseReport(owner.handle, nativeReport);
        NativeCreationOwners.cleanupCallbacks(owner, externalShuffleEgress);
        creationOwners.remove(owner);
        return java.util.Optional.of(finalReport);
    }

    private byte[] finalReleaseReport(long executorHandle, byte[] encoded) {
        var report = parseNativeTaskReport(encoded);
        TaskIdentity identity = requireTaskIdentity(executorHandle, report.getAttempt());
        if (report.getReportOrigin()
                        != org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE
                || report.getExecutionDrainState()
                        != org.pangrova.protocol.v2.ExecutionDrainState
                                .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED
                || (report.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED
                        && report.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED
                        && report.getState()
                                != org.pangrova.protocol.v2.TaskState.TASK_STATE_CANCELLED)) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Joined Native release returned a nonterminal or non-Native task report");
        }
        return mergeProviderResources(
                        report,
                        AiInvocationService.resourceUsage(identity.tenantId(), identity.attempt()))
                .toByteArray();
    }

    @Override
    public void cancelTask(long executorHandle) {
        var lease = acquireHandleLease(executorHandle);
        try {
            revokeAiTask(executorHandle);
            NativeRuntime.nativeCancelTask(executorHandle);
        } finally {
            lease.close();
        }
    }

    /**
     * Queries native task state to determine if the task has reached a terminal state. Calls {@link
     * NativeRuntime#nativeGetTaskStatus} and decodes the canonical v2 TaskReport response.
     *
     * <p>Terminal states: FINISHED(4), FAILED(5), CANCELLED(6).
     *
     * <p>When the task has FAILED (stateValue == 5), throws {@link NativeRuntimeException} with the
     * error message and native stack trace extracted from the TaskStatus proto.
     */
    @Override
    public boolean isFinished(long executorHandle) {
        var lease = acquireHandleLease(executorHandle);
        try {
            byte[] statusBytes = NativeRuntime.nativeGetTaskStatus(executorHandle);
            return evaluateNativeStatus(statusBytes, executorHandle);
        } finally {
            lease.close();
        }
    }

    @Override
    public byte[] getTaskStatus(long executorHandle) {
        var lease = acquireHandleLease(executorHandle);
        try {
            byte[] status = NativeRuntime.nativeGetTaskStatus(executorHandle);
            if (status == null || status.length == 0) {
                return new byte[0];
            }
            var report = parseNativeTaskReport(status);
            var attempt = report.getAttempt();
            TaskIdentity identity = requireTaskIdentity(executorHandle, attempt);
            return mergeProviderResources(
                            report,
                            AiInvocationService.resourceUsage(
                                    identity.tenantId(), identity.attempt()))
                    .toByteArray();
        } finally {
            lease.close();
        }
    }

    @Override
    public byte[] getTaskStats(long executorHandle) {
        var lease = acquireHandleLease(executorHandle);
        try {
            byte[] stats = NativeRuntime.nativeGetTaskStats(executorHandle);
            return stats == null ? new byte[0] : stats;
        } finally {
            lease.close();
        }
    }

    /**
     * Interprets the serialized TaskStatus proto and returns whether the task reached a terminal
     * state. Throws {@link NativeRuntimeException} when the task has FAILED.
     */
    static boolean evaluateNativeStatus(byte[] statusBytes, long executorHandle) {
        if (statusBytes == null || statusBytes.length == 0) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Native returned an empty v2 TaskReport");
        }
        org.pangrova.protocol.v2.TaskReport status = parseNativeTaskReport(statusBytes);
        int stateValue = status.getStateValue();
        if (stateValue < 1 || stateValue > 6) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Native returned an invalid task state: " + stateValue);
        }
        boolean finished = stateValue == 4 || stateValue == 6;
        if (stateValue >= 2) {
            LOG.debug(
                    "event=is_finished_poll executorHandle={} stateValue={} finished={}",
                    executorHandle,
                    stateValue,
                    finished);
        }
        if (stateValue == 5) {
            throwNativeFailure(status);
        }
        return finished;
    }

    /** Throws a {@link NativeRuntimeException} carrying the native failure details. */
    private static void throwNativeFailure(org.pangrova.protocol.v2.TaskReport status) {
        String message =
                status.hasError() && !status.getError().getMessage().isBlank()
                        ? status.getError().getMessage()
                        : "Native task failed";
        throw new NativeRuntimeException(
                org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION, message);
    }

    private static org.pangrova.protocol.v2.TaskReport parseNativeTaskReport(byte[] statusBytes) {
        try {
            return org.pangrova.protocol.v2.TaskReport.parseFrom(statusBytes);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Native returned malformed v2 TaskReport",
                    failure);
        }
    }

    private void revokeAiTask(long executorHandle) {
        var owner = creationOwners.forHandle(executorHandle);
        if (owner != null && owner.aiRegistered) {
            AiInvocationService.cancelTask(
                    owner.request.getSecurity().getTenantId(), owner.request.getAttempt());
        }
    }

    private static org.pangrova.protocol.v2.TaskRequest parseTaskRequest(byte[] taskRequestBytes) {
        try {
            var request = org.pangrova.protocol.v2.TaskRequest.parseFrom(taskRequestBytes);
            org.pangrova.protocol.task.CanonicalTaskRequests.validateForAdmission(request);
            return request;
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "TaskRequest is malformed",
                    failure);
        }
    }

    private static TaskIdentity taskIdentity(org.pangrova.protocol.v2.TaskRequest request) {
        if (!request.hasAttempt()) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "TaskRequest has no attempt identity");
        }
        var attempt = request.getAttempt();
        return new TaskIdentity(request.getSecurity().getTenantId(), attempt);
    }

    private void prepareExternalShuffle(NativeCreationOwners.Owner owner) {
        var request = owner.request;
        boolean hasBinding =
                request.hasExecutionOptions()
                        && request.getExecutionOptions().hasShuffle()
                        && request.getExecutionOptions().getShuffle().hasExecutionBinding();
        if (!hasBinding) {
            return;
        }
        if (externalShuffleEgress == null) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "TaskRequest requires external shuffle but this Worker uses Native shuffle");
        }
        // Installation can publish a retained candidate before failing its
        // actual drain. This flag is a cleanup obligation, not a success claim.
        owner.externalRegistered = true;
        externalShuffleEgress.installTask(request);
    }

    private TaskIdentity requireTaskIdentity(
            long executorHandle, org.pangrova.protocol.v2.TaskAttemptId attempt) {
        var owner = creationOwners.forHandle(executorHandle);
        TaskIdentity expected = owner == null ? null : taskIdentity(owner.request);
        if (expected == null || !expected.attempt().equals(attempt)) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Native task status identity differs from its request");
        }
        return expected;
    }

    private static org.pangrova.protocol.v2.TaskReport mergeProviderResources(
            org.pangrova.protocol.v2.TaskReport report,
            org.pangrova.protocol.v2.ResourceUsage actual) {
        var nativeUsage = report.getUsage();
        if (nativeUsage.getProviderItems() != 0
                || nativeUsage.getProviderAttempts() != 0
                || nativeUsage.getProviderRequestBytes() != 0
                || nativeUsage.getProviderResponseBytes() != 0
                || nativeUsage.getObservedProviderResponseBytes() != 0) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Native reported provider resources owned by the Worker transport");
        }
        return report.toBuilder()
                .setUsage(
                        nativeUsage.toBuilder()
                                .setProviderItems(actual.getProviderItems())
                                .setProviderAttempts(actual.getProviderAttempts())
                                .setProviderRequestBytes(actual.getProviderRequestBytes())
                                .setProviderResponseBytes(actual.getProviderResponseBytes())
                                .setObservedProviderResponseBytes(
                                        actual.getObservedProviderResponseBytes())
                                .setMediaBytes(
                                        Math.addExact(
                                                nativeUsage.getMediaBytes(),
                                                actual.getMediaBytes())))
                .build();
    }

    @Override
    public byte[] executeCommand(long executorHandle, byte[] commandProto) {
        requireCommandHandle(executorHandle);
        org.pangrova.protocol.v2.RuntimeCommand command;
        try {
            command = org.pangrova.protocol.v2.RuntimeCommand.parseFrom(commandProto);
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Malformed canonical runtime command", malformed);
        }
        var lease = executorHandle != 0 && isRetirementCleanup(command.getCommandType())
                ? acquireCleanupLease(executorHandle) : acquireHandleLease(executorHandle);
        try {
            if (executorHandle == 0) {
                if (command.getCommandType()
                        == org.pangrova.protocol.v2.RuntimeCommandType
                                .RUNTIME_COMMAND_TYPE_RECOVER_EXECUTOR_CREATION) {
                    return NativeCreationRecovery.recoverTracked(
                            creationOwners, externalShuffleEgress, command);
                }
            }
            return NativeRuntime.nativeCommand(executorHandle, commandProto);
        } finally {
            lease.close();
        }
    }

    /** Validates presence before decoding; the later actual lease still serializes every JNI operation. */
    private void requireCommandHandle(long handle) {
        lifecycleLock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("Handle " + handle + " is not tracked: executor is closed");
            }
            if (handle != 0) {
                var owner = creationOwners.forHandle(handle);
                if (owner == null || !creationOwners.contains(owner)) {
                    throw new IllegalStateException("Handle " + handle + " is not tracked");
                }
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private static boolean isRetirementCleanup(org.pangrova.protocol.v2.RuntimeCommandType type) {
        return type == org.pangrova.protocol.v2.RuntimeCommandType.RUNTIME_COMMAND_TYPE_GET_RESOURCE_DRAIN_EVIDENCE
                || type == org.pangrova.protocol.v2.RuntimeCommandType.RUNTIME_COMMAND_TYPE_STOP_SHUFFLE_SERVER;
    }

    private NativeOperationLifetime.Lease acquireCleanupLease(long handle) {
        while (true) {
            NativeCreationOwners.Owner owner;
            lifecycleLock.readLock().lock();
            try {
                if (closed) throw new IllegalStateException("Native executor is closed during cleanup observation");
                owner = creationOwners.forHandle(handle);
                if (owner == null || !creationOwners.contains(owner) || owner.releaseReceipt != null) {
                    throw new IllegalStateException("Native handle has retired its physical command owner: " + handle);
                }
                var lease = owner.operations.tryEnterCleanup();
                if (lease != null) return lease;
            } finally {
                lifecycleLock.readLock().unlock();
            }
            // Never wait for actual JNI exit while holding the lifecycle lock.
            owner.operations.awaitCleanupExit();
        }
    }

    /** Joins all handles; failed cleanup throws and remains tracked for retry. */
    @Override
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            closed = true;
        } finally {
            lifecycleLock.writeLock().unlock();
        }
        NativeRuntimeException failure = null;
        for (var owner : creationOwners.snapshot()) {
            try {
                closeCreationOwner(owner);
            } catch (Throwable e) {
                LOG.error("event=close_release_failed executorHandle={}", owner.handle, e);
                if (failure == null) {
                    failure =
                            new NativeRuntimeException(
                                    org.pangrova.common.exception.PangrovaErrorCode
                                            .NATIVE_JNI_EXCEPTION,
                                    "Native executor cleanup did not complete; tracked handles require retry",
                                    e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (creationOwners.isEmpty()) {
            globalOperations.sealAndAwait();
            if (externalShuffleEgress != null) {
                externalShuffleEgress.close();
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void closeCreationOwner(NativeCreationOwners.Owner owner) {
        owner.awaitConstructionExit();
        try (var claim = owner.operations.claim(true)) {
            claim.awaitReaders();
            if (!creationOwners.contains(owner)) {
                return;
            }
            if (owner.enteredNative && owner.handle == 0 && !owner.nativeClosedWithoutOwner) {
                recoverCreation(owner);
            }
            if (owner.handle > 0) {
                releaseOwner(owner);
            } else if (!owner.enteredNative || owner.nativeClosedWithoutOwner) {
                NativeCreationOwners.cleanupCallbacks(owner, externalShuffleEgress);
                creationOwners.remove(owner);
            } else {
                throw new IllegalStateException(
                        "JNI creation returned no resolvable owner for exact attempt "
                                + owner.request.getAttempt().getTaskId());
            }
        }
    }

    private record TaskIdentity(String tenantId, org.pangrova.protocol.v2.TaskAttemptId attempt) {}
}
