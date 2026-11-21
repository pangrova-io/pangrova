package org.pangrova.worker.task;

import org.pangrova.worker.enums.TaskAdmissionReason;
import org.pangrova.worker.exception.TaskAdmissionException;
import org.pangrova.protocol.rest.copy.PrepareCopySourceRequest;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.SourcePreparationResult;
import org.pangrova.protocol.v2.TaskAdmissionSealRequest;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.worker.resource.memory.NodeMemoryPool;
import org.pangrova.worker.copy.CopyStageLeases;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Source preparation uses the same exact admission/seal authority, with no relational task or Native owner. */
final class SourcePreparationControl implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SourcePreparationControl.class);
    private final TaskManager owner;
    private final CopyStageLeases leases;
    private final NodeMemoryPool nodePool;
    private final ConcurrentHashMap<String, Resources> resources = new ConcurrentHashMap<>();
    private volatile boolean closed;

    SourcePreparationControl(TaskManager owner, CopyStageLeases leases, NodeMemoryPool nodePool) {
        this.owner = owner;
        this.leases = leases;
        this.nodePool = nodePool;
    }

    boolean uses(CopyStageLeases expected) { return leases == expected; }

    SourcePreparationResult execute(PrepareCopySourceRequest request) throws IOException {
        if (closed || owner.drainExecutor.isDraining()) throw new IllegalStateException("Source preparation is closed");
        var command = request.preparation();
        owner.verifyWorkerExecutionIdentity(command.getAttempt());
        var authority = request.admissionSeal();
        var previous = owner.taskAdmissions.preparationTerminal(authority);
        if (previous.isPresent()) {
            owner.verifyExistingCleanupControlEpoch(command.getAttempt().getControlEpoch());
            return replay(request, previous.orElseThrow());
        }
        owner.verifyControlEpochForAdmission(command.getAttempt().getControlEpoch());
        owner.verifyAdmissionDeadline(command.getDeadlineEpochMs());
        owner.verifyExecutionOwnerForControl(command.getAttempt().getQueryOrJobId(),
                command.getAttempt().getExecutionOwner());
        TaskAdmissionRegistry.Reservation reservation;
        try {
            reservation = owner.taskAdmissions.reserve(authority, "");
        } catch (TaskAdmissionException rejected) {
            // Another exact delivery can finish between the terminal read and reservation.
            // Recheck the same authority; a seal without a real preparation outcome still rejects.
            var completed = owner.taskAdmissions.preparationTerminal(authority);
            if (rejected.reason() == TaskAdmissionReason.SEALED && completed.isPresent()) {
                return replay(request, completed.orElseThrow());
            }
            throw rejected;
        }
        if (reservation.first()) {
            Resources acquired = new Resources(request, owner.quotaEnforcer);
            acquired.reservation = reservation;
            try {
                acquired.reserve();
                reservation.publish(() -> {
                    acquired.admission = leases.admit(request);
                    resources.put(authority.getAttempt().getTaskId(), acquired);
                });
                reservation.admitted();
            } catch (RuntimeException | Error failure) {
                acquired.rollbackFailure = failure;
                try {
                    acquired.release();
                    reservation.rejectedBeforeOwnership(failure);
                } catch (RuntimeException | Error cleanup) {
                    if (cleanup != failure) failure.addSuppressed(cleanup);
                    resources.put(authority.getAttempt().getTaskId(), acquired);
                }
                throw failure;
            }
        } else {
            reservation.awaitAdmission();
        }
        var active = resources.get(authority.getAttempt().getTaskId());
        if (active == null) {
            var terminal = owner.taskAdmissions.preparationTerminal(authority)
                    .orElseThrow(() -> new IllegalStateException("Source preparation lost its actual owner"));
            return replay(request, terminal);
        }
        try {
            return active.admission.execute(request);
        } finally {
            finish(authority, active);
        }
    }

    private SourcePreparationResult replay(PrepareCopySourceRequest request,
                                           TaskAdmissionRegistry.PreparationTerminal terminal) {
        try {
            return leases.replay(request, terminal.usage(), terminal.error(), terminal.succeeded());
        } catch (RuntimeException denied) {
            boolean policy = denied instanceof org.pangrova.security.api.exception.AccessDeniedException
                    || denied instanceof org.pangrova.worker.exception.CopySourceReadRejectedException rejected
                            && rejected.isPolicyDenial();
            if (!policy) throw denied;
            return SourcePreparationResult.newBuilder().setAttempt(request.preparation().getAttempt())
                    .setRequestSha256(request.admissionSeal().getCanonicalRequestChecksum())
                    .setUsage(terminal.usage()).setIoJoined(true)
                    .setError(org.pangrova.protocol.v2.ExecutionError.newBuilder()
                            .setErrorClass(org.pangrova.protocol.v2.ErrorClass.ERROR_CLASS_POLICY_DENIED)
                            .setCode("COPY_PREPARATION_REPLAY_DENIED").setMessage("Source authority replay was denied")
                            .setFailedComponent("worker-copy-source")).build();
        }
    }

    boolean owns(TaskAdmissionSealRequest authority) { return owner.taskAdmissions.isPreparation(authority); }

    Optional<TaskReport> seal(TaskAdmissionSealRequest authority) {
        var active = resources.get(authority.getAttempt().getTaskId());
        if (active != null) {
            if (active.admission != null) active.admission.cancel();
            finish(authority, active);
        }
        var sealed = owner.taskAdmissions.seal(authority, true);
        if (sealed.receipt().isPresent()) return sealed.receipt();
        return sealed.noOwnerExited() ? Optional.of(CanonicalTaskAdmissionSeals.closedWithoutOwner(authority))
                : Optional.empty();
    }

    private void finish(TaskAdmissionSealRequest authority, Resources active) {
        if (active.rollbackFailure != null) {
            active.release();
            active.reservation.rejectedBeforeOwnership(active.rollbackFailure);
            if (active.admission != null) {
                active.admission.cancel();
                active.admission.retire();
            }
            resources.remove(authority.getAttempt().getTaskId(), active);
            return;
        }
        if (active.admission == null) return;
        try {
            var joined = active.admission.drain();
            if (joined.isEmpty()) return;
            var result = joined.orElseThrow();
            active.release();
            // This is exit-only Native admission evidence. Preparation success belongs only to
            // SourcePreparationResult; it must never become an invented successful Native report.
            var receipt = CanonicalTaskAdmissionSeals.closedWithoutOwner(authority).toBuilder();
            if (result.hasError()) {
                boolean cancelled = result.getError().getErrorClass()
                        == org.pangrova.protocol.v2.ErrorClass.ERROR_CLASS_CANCELLED;
                receipt.setState(cancelled
                        ? TaskState.TASK_STATE_CANCELLED : TaskState.TASK_STATE_FAILED).setError(result.getError());
            }
            owner.taskAdmissions.preparationFinished(authority, receipt.build(), result);
            active.admission.retire();
            resources.remove(authority.getAttempt().getTaskId(), active);
        } catch (IOException failure) {
            LOG.warn("event=copy_preparation_cleanup_pending taskId={}", authority.getAttempt().getTaskId(), failure);
        }
    }

    @Override public void close() throws Exception {
        closed = true;
        for (var active : resources.values()) {
            if (active.admission != null) active.admission.cancel();
        }
        leases.close();
        for (var active : resources.values()) {
            finish(active.request.admissionSeal(), active);
        }
        if (!resources.isEmpty()) throw new IllegalStateException("Source preparation resources have not drained");
    }

    private final class Resources {
        private final PrepareCopySourceRequest request;
        private final TenantExecutionQuotaEnforcer quota;
        private CopyStageLeases.Admission admission;
        private TaskAdmissionRegistry.Reservation reservation;
        private Throwable rollbackFailure;
        private long memory;
        private boolean tenant;

        private Resources(PrepareCopySourceRequest request, TenantExecutionQuotaEnforcer quota) {
            this.request = request;
            this.quota = quota;
        }

        private synchronized void reserve() {
            String task = "source-preparation:" + request.preparation().getAttempt().getTaskId();
            if (quota != null) {
                tenant = quota.tryReserveTask(task, request.preparation().getSecurity().getTenantId(),
                        request.preparation().getResourceGrant().getJvmHeapBytes());
                if (!tenant) throw new TaskAdmissionException(TaskAdmissionReason.CAPACITY,
                        "Source preparation tenant quota exceeded");
            }
            if (nodePool != null) {
                long requested = request.preparation().getResourceGrant().getJvmHeapBytes();
                memory = nodePool.tryReserve(requested);
                if (memory != requested) throw new TaskAdmissionException(TaskAdmissionReason.CAPACITY,
                        "Source preparation JVM memory grant is not available");
            }
        }

        private synchronized void release() {
            if (tenant) {
                quota.releaseTask("source-preparation:" + request.preparation().getAttempt().getTaskId());
                tenant = false;
            }
            if (memory > 0) {
                nodePool.free(memory);
                memory = 0;
            }
        }
    }
}
