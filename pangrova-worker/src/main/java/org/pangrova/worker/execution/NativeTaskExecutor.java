package org.pangrova.worker.execution;

import org.pangrova.worker.exception.NativeTaskCreationFailure;
import org.pangrova.protocol.execution.AiTaskLifecycle;
import org.pangrova.worker.function.WorkerCallbackProviders;

import org.pangrova.shuffle.api.worker.ShuffleProducerSession;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.task.ValidatedTaskSubmission;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.common.concurrency.ThreadSafe;

import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.exception.NativeRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
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

    private final ShuffleProducerSession<TaskRequest> externalShuffleEgress;
    private final AiTaskLifecycle aiLifecycle;
    private final NativeCreationOwners creationOwners = new NativeCreationOwners();
    private final NativeOperationLifetime globalOperations = new NativeOperationLifetime();

    /** Serializes only in-memory admission with close; never held during JNI. */
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();

    private volatile boolean closed = false;
    private volatile org.pangrova.worker.catalog.WorkerCatalogRuntime catalogRuntime;
    private volatile org.pangrova.worker.copy.CopyStageLeases copyInputs;

    /** Resolves the Native owner for COPY-specific operations outside normal task routing. */
    public static Optional<NativeTaskExecutor> ownerOf(TaskExecutor executor) {
        TaskExecutor owner = executor instanceof CompositeTaskExecutor composite
                ? composite.nativeDelegate() : executor;
        return owner instanceof NativeTaskExecutor nativeOwner ? Optional.of(nativeOwner) : Optional.empty();
    }

    /** Supplies the governed source materializer before task admission starts. */
    public void setCopyInputs(org.pangrova.worker.copy.CopyStageLeases runtime) {
        if (!creationOwners.isEmpty()) throw new IllegalStateException("Cannot replace COPY input runtime with owners");
        if (!runtime.supportsNativeInputs()) {
            throw new IllegalArgumentException("COPY runtime cannot materialize Native inputs");
        }
        copyInputs = java.util.Objects.requireNonNull(runtime, "runtime");
    }

    /** Reports whether private COPY tasks have their real source materializer. */
    public boolean hasCopyInputs() { return copyInputs != null; }

    /**
     * Supplies the production Worker-local catalog authority before Native dataset task creation.
     */
    public void setCatalogRuntime(
            org.pangrova.worker.catalog.WorkerCatalogRuntime runtime) {
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
    public NativeTaskExecutor(ShuffleProducerSession<TaskRequest> externalShuffleEgress) {
        this.aiLifecycle = WorkerCallbackProviders.taskLifecycle(NativeTaskExecutor.class.getClassLoader());
        this.externalShuffleEgress = externalShuffleEgress;
    }

    @Override
    public long createTask(TaskRequest request) {
        ValidatedTaskSubmission admission;
        try {
            admission = new ValidatedTaskSubmission(SubmitTaskRequest.from(request));
        } catch (RuntimeException failure) {
            throw new NativeTaskCreationFailure(0, true, failure);
        }
        return createAdmittedTask(admission);
    }

    @Override
    public long createAdmittedTask(ValidatedTaskSubmission admission) {
        var envelope = admission.submission();
        var request = envelope.taskRequest();
        boolean hasCopyInput = envelope.copySourceRead() != null;
        if (hasCopyInput && copyInputs == null) {
            throw new NativeTaskCreationFailure(0, true,
                    new IllegalStateException("COPY input runtime and private source ticket are required"));
        }
        var owner = prepareCreation(request);
        try {
            var local = org.pangrova.worker.catalog
                    .WorkerNativeStorageTasks.prepare(request, catalogRuntime);
            if (hasCopyInput) {
                owner.copyReadBudget = new org.pangrova.worker.copy.CopyTaskReadBudget(
                        request.getResourceGrant().getObjectStoreReadBytes());
                var input = org.pangrova.worker.copy.CopyNativeTaskInput.prepare(envelope, copyInputs,
                        new org.pangrova.worker.copy.CopyMaterialization(owner.copyReadBudget,
                                file -> owner.copyInput = file));
                local = local.toBuilder().setWorkerLocalCopyInput(input).build();
            }
            prepareExternalShuffle(owner);
            if (aiLifecycle != AiTaskLifecycle.NONE) {
                aiLifecycle.registerTask(admission.callbackContext(), request.getOperatorBindingsList());
                owner.aiRegistered = true;
            }
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
            throw reconcileCreationFailure(owner, failure).withWorkerObjectReadBytes(
                    owner.copyReadBudget == null ? 0 : owner.copyReadBudget.requestedBytes());
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
            return creationOwners.prepare(request, aiLifecycle);
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
        startTask(executorHandle, null);
    }

    @Override
    public void startResultTask(long executorHandle, Runnable onReady) {
        startTask(executorHandle, java.util.Objects.requireNonNull(onReady, "onReady"));
    }

    private void startTask(long executorHandle, Runnable onReady) {
        var lease = acquireHandleLease(executorHandle);
        try {
            String traceId =
                    creationOwners.forHandle(executorHandle).request.getTraceContext().getTraceId();
            NativeRuntime.nativeStartTaskWithTrace(executorHandle, traceId, onReady);
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
                    org.pangrova.common.enums.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Joined Native release returned a nonterminal or non-Native task report");
        }
        return mergeCopyReads(executorHandle, mergeProviderResources(
                        report,
                        aiLifecycle.usage(identity.tenantId(), identity.attempt())))
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

    /** Reads the native lifecycle state. A failed task fetches its original error details. */
    @Override
    public boolean isFinished(long executorHandle) {
        return awaitFinished(executorHandle, 0);
    }

    @Override
    public boolean awaitFinished(long executorHandle, int waitMillis) {
        var lease = acquireHandleLease(executorHandle);
        try {
            var state = requireNativeTaskState(NativeRuntime.nativeReadTaskState(executorHandle, waitMillis));
            if (state == TaskState.TASK_STATE_FAILED) {
                throw nativeTaskFailure(NativeRuntime.nativeGetTaskStatus(executorHandle));
            }
            return state == TaskState.TASK_STATE_FINISHED || state == TaskState.TASK_STATE_CANCELLED;
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
            return mergeCopyReads(executorHandle, mergeProviderResources(
                            report,
                            aiLifecycle.usage(
                                    identity.tenantId(), identity.attempt())))
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

    static TaskState requireNativeTaskState(int stateValue) {
        var state = TaskState.forNumber(stateValue);
        if (state == null || state == TaskState.TASK_STATE_UNSPECIFIED) {
            throw new NativeRuntimeException(
                    org.pangrova.common.enums.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Native returned an invalid task state: " + stateValue);
        }
        return state;
    }

    static NativeRuntimeException nativeTaskFailure(byte[] statusBytes) {
        if (statusBytes == null || statusBytes.length == 0) {
            throw new NativeRuntimeException(
                    org.pangrova.common.enums.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Native returned an empty v2 TaskReport");
        }
        TaskReport status = parseNativeTaskReport(statusBytes);
        if (status.getState() != TaskState.TASK_STATE_FAILED) {
            throw new NativeRuntimeException(
                    org.pangrova.common.enums.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Native failure report has unexpected task state: " + status.getStateValue());
        }
        String message =
                status.hasError() && !status.getError().getMessage().isBlank()
                        ? status.getError().getMessage()
                        : "Native task failed";
        return new NativeRuntimeException(
                org.pangrova.common.enums.PangrovaErrorCode.NATIVE_JNI_EXCEPTION, message);
    }

    private static TaskReport parseNativeTaskReport(byte[] statusBytes) {
        try {
            return TaskReport.parseFrom(statusBytes);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new NativeRuntimeException(
                    org.pangrova.common.enums.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Native returned malformed v2 TaskReport",
                    failure);
        }
    }

    private void revokeAiTask(long executorHandle) {
        var owner = creationOwners.forHandle(executorHandle);
        if (owner != null && owner.aiRegistered) {
            aiLifecycle.cancelTask(
                    owner.request.getSecurity().getTenantId(), owner.request.getAttempt());
        }
    }

    private static TaskIdentity taskIdentity(org.pangrova.protocol.v2.TaskRequest request) {
        if (!request.hasAttempt()) {
            throw new NativeRuntimeException(
                    org.pangrova.common.enums.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
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
                    org.pangrova.common.enums.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
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
                    org.pangrova.common.enums.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
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
                    org.pangrova.common.enums.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
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

    private org.pangrova.protocol.v2.TaskReport mergeCopyReads(
            long handle, org.pangrova.protocol.v2.TaskReport report) {
        var owner = creationOwners.forHandle(handle);
        if (owner == null || owner.copyReadBudget == null) return report;
        long reads = Math.addExact(report.getUsage().getObjectStoreReadBytes(), owner.copyReadBudget.requestedBytes());
        if (reads > owner.request.getResourceGrant().getObjectStoreReadBytes()) {
            throw new IllegalStateException("COPY combined read usage exceeded the admitted task grant");
        }
        return report.toBuilder().setUsage(report.getUsage().toBuilder().setObjectStoreReadBytes(reads)).build();
    }

    /** Registers one signal for the exact finished Native result owner. */
    public void watchResultDrain(long executorHandle, Runnable callback) {
        var lease = acquireHandleLease(executorHandle);
        try {
            NativeRuntime.nativeWatchResultDrain(executorHandle, callback);
        } finally {
            lease.close();
        }
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

    @Override
    public byte[] pushExchangeChunk(long executorHandle, byte[] metadata, byte[] data) {
        var lease = acquireHandleLease(executorHandle);
        try {
            return NativeRuntime.nativePushExchangeChunk(executorHandle, metadata, data);
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
                                    org.pangrova.common.enums.PangrovaErrorCode
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
