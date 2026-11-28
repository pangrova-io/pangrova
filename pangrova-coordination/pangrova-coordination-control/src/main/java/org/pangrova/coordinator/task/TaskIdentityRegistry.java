package org.pangrova.coordinator.task;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.coordinator.task.TaskReportPublication.Fact;
import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;

import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.pangrova.coordinator.worker.WorkerInfo;

/** Atomically retains the sole issued attempt and its exact request for fencing and speculative cloning. */
@org.pangrova.common.concurrent.ThreadSafe
final class TaskIdentityRegistry {
    private final ConcurrentHashMap<String, Identity> identities = new ConcurrentHashMap<>();
    // Published under the issuance lock and read by placement without locking or scanning attempts.
    private final ConcurrentHashMap<WorkerExecutionIdentity, Integer> workerLoads = new ConcurrentHashMap<>();
    // This is the admission state of the SAME issued identities, not a second
    // completion registry. All mutations below are local; no RPC runs here.
    @javax.annotation.concurrent.GuardedBy("this")
    private final HashMap<String, Scope> scopes = new HashMap<>();
    private final java.util.function.LongSupplier clock;
    private volatile java.util.function.Consumer<String> connectorOperationExited = ignored -> { };

    TaskIdentityRegistry() { this(System::nanoTime); }
    TaskIdentityRegistry(java.util.function.LongSupplier clock) {
        this.clock = java.util.Objects.requireNonNull(clock);
    }

    void onConnectorOperationExit(java.util.function.Consumer<String> listener) {
        connectorOperationExited = java.util.Objects.requireNonNull(listener);
    }

    synchronized org.pangrova.connector.api.query.ConnectorQueryAuthority openQuery(String queryId) {
        if (queryId == null || queryId.isBlank()) throw new IllegalArgumentException("queryId must not be blank");
        purgeRetiredQueries();
        if (!scopes.containsKey(queryId) && scopes.size()
                >= org.pangrova.protocol.constants.TerminalReportDeliveryPolicy.MAX_RETAINED_IDENTITIES) {
            throw new IllegalStateException("Query issuance identity capacity exhausted");
        }
        Scope scope = scopes.computeIfAbsent(queryId, Scope::new);
        if (scope.sealed) throw new IllegalStateException("Query issuance is sealed: " + queryId);
        return scope.connectorAuthority;
    }

    synchronized void requireOpenQuery(String queryId) {
        Scope scope = scopes.get(queryId);
        if (scope == null || scope.sealed) {
            throw new IllegalStateException("Query has no open issuance authority: " + queryId);
        }
    }

    synchronized void registerLogicalTask(String queryId, Runnable publication) {
        openQuery(queryId);
        publication.run(); // Local identity publication only: never JNI, RPC or connector I/O.
    }

    synchronized Runnable enterQueryOperation(String queryId) {
        requireOpenQuery(queryId);
        Scope scope = scopes.get(queryId);
        Runnable exited = () -> exitQueryOperation(scope);
        scope.operations++;
        return exited;
    }

    private synchronized void exitQueryOperation(Scope scope) {
        if (--scope.operations < 0) throw new IllegalStateException("Query operation was released twice");
    }

    synchronized boolean queryOperationsDrained(String queryId) {
        Scope scope = scopes.get(queryId);
        return scope != null && scope.operations == 0;
    }

    synchronized Runnable enterMetadataOperation(String queryId) {
        Scope scope = requireMetadataScope(queryId);
        Runnable exited = () -> exitQueryOperation(scope);
        scope.operations++;
        return exited;
    }

    private Scope requireMetadataScope(String queryId) {
        Scope scope = scopes.get(queryId);
        if (scope == null || scope.retired || scope.cleanupClaimed) {
            throw new IllegalStateException("Query metadata admission is closed: " + queryId);
        }
        return scope;
    }

    synchronized TaskReportPublication enterReport(SubmitTaskRequest request,
            org.pangrova.protocol.rest.task.TaskStatusReport report, boolean resourceOnly,
            Runnable notifyDrain, Runnable operationExited) {
        requireIdentity(request);
        Scope scope = requireMetadataScope(report.queryId());
        Fact fact = scope.publications.get(report.taskId());
        if (fact == null || fact.sequence != report.reportSequence()) {
            if (fact != null && !fact.notificationsCompleted) {
                throw new IllegalStateException("Prior report publication remains owned");
            }
            fact = new Fact(request, report, resourceOnly);
            scope.publications.put(report.taskId(), fact);
        } else if (!fact.matches(report)) {
            throw new IllegalArgumentException("Same report sequence has a different frozen fact");
        }
        Fact owner = fact;
        if (owner.notificationsCompleted) {
            throw new IllegalStateException("Report publication was already acknowledged");
        }
        TaskReportPublication operation = new TaskReportPublication(owner.report, owner.resourceOnly, owner.completed,
                () -> completeReport(scope, owner),
                () -> exitReport(scope, owner, notifyDrain, operationExited));
        scope.operations++;
        owner.operations++;
        return operation;
    }

    private synchronized void completeReport(Scope scope, Fact fact) { fact.completed = true; }

    synchronized boolean hasPendingPublication(org.pangrova.protocol.rest.task.TaskStatusReport report) {
        Scope scope = scopes.get(report.queryId());
        Fact fact = scope == null ? null : scope.publications.get(report.taskId());
        return fact != null && !fact.notificationsCompleted && !fact.resourceOnly && fact.matches(report);
    }

    private void exitReport(Scope scope, Fact fact, Runnable notifyDrain,
            Runnable operationExited) {
        boolean notify;
        synchronized (this) {
            if (--fact.operations < 0) throw new IllegalStateException("Report operation exited twice");
            notify = fact.completed && fact.operations == 0 && !fact.notifying && !fact.notificationsCompleted;
            if (notify) fact.notifying = true;
            else exitQueryOperation(scope);
        }
        if (!notify) {
            operationExited.run();
            return;
        }
        boolean completed = false;
        try {
            notifyDrain.run();
            completed = true;
        } finally {
            synchronized (this) {
                fact.notifying = false;
                if (completed) {
                    fact.notificationsCompleted = true;
                    fact.report = null;
                    fact.request = null;
                }
                exitQueryOperation(scope);
            }
            operationExited.run();
        }
    }

    synchronized boolean drainNotificationReady(String taskId) {
        Identity identity = identities.get(taskId);
        if (identity == null || identity.drainReceipt() == null) return false;
        Scope scope = scopes.get(identity.drainReceipt().getAttempt().getQueryOrJobId());
        Fact fact = scope == null ? null : scope.publications.get(taskId);
        return fact == null || fact.notificationsCompleted;
    }

    synchronized boolean finishedPublicationComplete(org.pangrova.protocol.v2.TaskReport receipt) {
        String taskId = receipt.getAttempt().getTaskId();
        Identity identity = identities.get(taskId);
        if (identity == null || !receipt.equals(identity.drainReceipt())) return false;
        Scope scope = scopes.get(receipt.getAttempt().getQueryOrJobId());
        Fact fact = scope == null ? null : scope.publications.get(taskId);
        return fact != null && fact.notificationsCompleted && fact.operations == 0
                && "FINISHED".equals(fact.state)
                && fact.attempt.equals(TaskAttemptDescriptor.from(receipt.getAttempt()))
                && fact.canonicalDigest != null
                && fact.canonicalDigest.equals(org.pangrova.protocol.checksum.ProtocolChecksums
                        .sha256(receipt.toByteString()).getDigest());
    }

    synchronized void registerRequest(String taskId, SubmitTaskRequest request) {
        requireTaskId(taskId);
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        var canonicalAttempt = request.taskRequest().getAttempt();
        if (!taskId.equals(canonicalAttempt.getTaskId())) {
            throw new IllegalArgumentException("taskId does not match request.taskId: taskId=" + taskId
                    + ", requestTaskId=" + canonicalAttempt.getTaskId());
        }
        if (!request.taskRequest().hasAttempt()) {
            throw new IllegalArgumentException("request.attempt must not be null");
        }
        TaskAttemptDescriptor issued = TaskAttemptDescriptor.from(canonicalAttempt);
        requireOpenQuery(canonicalAttempt.getQueryOrJobId());
        Scope scope = scopes.get(canonicalAttempt.getQueryOrJobId());
        var scheduled = scope.scheduled.get(taskId);
        if (scheduled == null) {
            scheduled = new org.pangrova.query.api.journal.TaskStateEvent(
                    TaskJournalEventContext.create(canonicalAttempt, 0L, System.currentTimeMillis()),
                    canonicalAttempt.getQueryOrJobId(), taskId, null, "SCHEDULED", canonicalAttempt.getStageId(), 0L);
        }
        if (!identities.containsKey(taskId) && identities.size()
                >= org.pangrova.protocol.constants.TerminalReportDeliveryPolicy.MAX_RETAINED_IDENTITIES) {
            throw new IllegalStateException("Issued task identity capacity exhausted");
        }
        publishRequest(taskId, request, issued, scope, scheduled);
    }

    private void publishRequest(String taskId, SubmitTaskRequest request, TaskAttemptDescriptor issued,
            Scope scope, org.pangrova.query.api.journal.TaskStateEvent scheduled) {
        Identity existing = identities.get(taskId);
        requireSameAttempt(taskId, existing, issued);
        if (existing != null && existing.request() != null && !existing.request().equals(request)) {
            throw new IllegalStateException("Conflicting task request already registered: taskId=" + taskId);
        }
        Identity candidate = existing != null && existing.request() != null ? existing
                : new Identity(issued, request, existing == null ? null : existing.drainReceipt(),
                        existing == null ? null : existing.worker(), existing != null && existing.entered());
        boolean hadTask = scope.tasks.contains(taskId);
        boolean hadFact = scope.scheduled.containsKey(taskId);
        try {
            scope.tasks.add(taskId);
            scope.scheduled.putIfAbsent(taskId, scheduled);
            identities.put(taskId, candidate);
        } catch (RuntimeException | Error failure) {
            if (!hadFact) scope.scheduled.remove(taskId);
            if (!hadTask) scope.tasks.remove(taskId);
            throw failure;
        }
    }

    synchronized org.pangrova.query.api.journal.TaskStateEvent scheduledEvent(String taskId) {
        Identity identity = identities.get(taskId);
        if (identity == null || identity.workerId() == null) {
            throw new IllegalStateException("Scheduled fact lacks its exact issued dispatch target");
        }
        String queryId = identity.request() != null ? identity.request().taskRequest().getAttempt().getQueryOrJobId()
                : identity.drainReceipt().getAttempt().getQueryOrJobId();
        var fact = scopes.get(queryId).scheduled.get(taskId);
        if (fact == null) throw new IllegalStateException("Issued task lacks its frozen scheduled fact");
        return new org.pangrova.query.api.journal.TaskStateEvent(fact.context(), fact.queryId(), fact.taskId(),
                identity.workerId(), fact.state(), fact.stageId(), fact.reportSequence());
    }

    synchronized void bindTarget(SubmitTaskRequest request, WorkerInfo worker) {
        String taskId = request.taskRequest().getAttempt().getTaskId();
        Identity existing = requireIdentity(request);
        requireOpenQuery(request.taskRequest().getAttempt().getQueryOrJobId());
        if (existing.drainReceipt() != null) {
            throw new IllegalStateException("Drained task cannot acquire another dispatch target");
        }
        if (existing.worker() != null) {
            if (!existing.worker().equals(worker)) {
                throw new IllegalStateException("Issued task target changed: " + taskId);
            }
            return;
        }
        identities.put(taskId, new Identity(existing.attempt(), existing.request(), existing.drainReceipt(),
                java.util.Objects.requireNonNull(worker, "worker"), existing.entered()));
        workerLoads.merge(worker.workerExecutionIdentity(), 1, Integer::sum);
    }

    CoordinatorTaskTracker.IssuedTask liveReadTarget(String taskId, String queryId) {
        var identity = identities.get(taskId);
        if (identity == null || !identity.entered() || identity.drainReceipt() != null
                || identity.worker() == null || identity.request() == null) return null;
        var task = identity.request().taskRequest();
        if (!task.getAttempt().getQueryOrJobId().equals(queryId) || task.getSplitsCount() == 0
                || task.getTaskKind() != org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ) return null;
        return new CoordinatorTaskTracker.IssuedTask(identity.request(), identity.worker(), java.util.Optional.empty());
    }

    int workerLoad(WorkerExecutionIdentity worker) {
        return workerLoads.getOrDefault(worker, 0);
    }

    private void releaseWorkerLoad(Identity identity) {
        if (identity.worker() == null) return;
        var target = identity.worker().workerExecutionIdentity();
        workerLoads.compute(target, (key, count) -> {
            if (count == null) {
                throw new IllegalStateException("Issued Worker has no placement load: " + key);
            }
            return count == 1 ? null : count - 1;
        });
    }

    synchronized void enterDispatch(SubmitTaskRequest request) {
        Identity existing = requireIdentity(request);
        requireOpenQuery(request.taskRequest().getAttempt().getQueryOrJobId());
        if (existing.worker() == null || existing.entered() || existing.drainReceipt() != null) {
            throw new IllegalStateException("Task lacks an unentered exact dispatch target");
        }
        identities.put(request.taskRequest().getAttempt().getTaskId(),
                new Identity(existing.attempt(), existing.request(), existing.drainReceipt(), existing.worker(), true));
    }

    synchronized List<CoordinatorTaskTracker.IssuedTask> sealQuery(String queryId) {
        Scope scope = scopes.get(queryId);
        if (scope == null) throw new IllegalStateException("Query has no issuance authority: " + queryId);
        scope.sealed = true;
        return scope.tasks.stream().map(identities::get).map(identity ->
                new CoordinatorTaskTracker.IssuedTask(identity.request(), identity.worker(),
                        java.util.Optional.ofNullable(identity.drainReceipt()))).toList();
    }

    synchronized void requireQueryDrained(String queryId) {
        Scope scope = scopes.get(queryId);
        if (scope == null || !scope.sealed) throw new IllegalStateException("Query issuance is not sealed: " + queryId);
        if (scope.operations != 0) {
            throw new IllegalStateException("Query dispatch operations have not exited: " + queryId);
        }
        claimDrainedScope(queryId, scope);
    }

    synchronized boolean tryClaimQueryCleanup(String queryId) {
        Scope scope = scopes.get(queryId);
        if (scope == null) throw new IllegalStateException("Query has no issuance authority: " + queryId);
        scope.sealed = true;
        if (scope.operations != 0) return false;
        if (hasPendingReport(scope) || taskWithoutDrain(scope) != null) return false;
        scope.cleanupClaimed = true;
        return true;
    }

    private void claimDrainedScope(String queryId, Scope scope) {
        if (hasPendingReport(scope)) {
            throw new IllegalStateException("Query still owns required report journal/publication work: " + queryId);
        }
        String missing = taskWithoutDrain(scope);
        if (missing != null) {
            throw new IllegalStateException("Issued task still lacks its exact execution drain receipt: " + missing);
        }
        scope.cleanupClaimed = true;
    }

    private static boolean hasPendingReport(Scope scope) {
        return scope.publications.values().stream().anyMatch(fact -> !fact.notificationsCompleted);
    }

    private String taskWithoutDrain(Scope scope) {
        for (String taskId : scope.tasks) {
            if (identities.get(taskId).drainReceipt() == null) return taskId;
        }
        return null;
    }

    synchronized void retireQuery(String queryId) {
        requireQueryDrained(queryId);
        Scope scope = scopes.get(queryId);
        scope.retired = true;
        if (scope.tasks.isEmpty()) scope.deadline = clock.getAsLong();
        for (String taskId : scope.tasks) {
            Identity identity = identities.get(taskId);
            scope.deadline = Math.max(scope.deadline, identity.ackDeadline());
            identities.put(taskId, identity.compact());
        }
    }

    synchronized boolean acknowledgesDrain(org.pangrova.protocol.rest.task.TaskStatusReport report) {
        Identity identity = identities.get(report.taskId());
        if (!hasLiveDrainReceipt(report, identity)) {
            return false;
        }
        Scope scope = scopes.get(report.queryId());
        Fact publication = scope == null ? null : scope.publications.get(report.taskId());
        // A canceled loser may have been explicitly retired using the exact
        // execution drain receipt before its delayed terminal HTTP report
        // arrives. Its issued request is gone, so the report cannot publish
        // outputs; acknowledge only that already-retired physical owner.
        if (publication != null && (!publication.notificationsCompleted || publication.operations != 0)) {
            return false;
        }
        if (identity.request() != null && (publication == null || !publication.matches(report))) {
            return false;
        }
        try {
            return matchesDrainReceipt(report, identity, report.canonicalStatus());
        } catch (IllegalArgumentException invalidDrainEvidence) {
            return false;
        }
    }

    /** A retired read-result owner can discard its earlier undrained FINISHED notification. */
    synchronized boolean supersedesResultNotice(org.pangrova.protocol.rest.task.TaskStatusReport report) {
        Identity identity = identities.get(report.taskId());
        if (!hasLiveDrainReceipt(report, identity) || identity.request() != null || !identity.directReadResult()
                || !"OUTPUT".equals(report.taskType()) || !"FINISHED".equals(report.state())
                || report.writeResult() != null || report.error() != null
                || !identity.attempt().equals(report.attempt())) {
            return false;
        }
        var scope = scopes.get(report.queryId());
        if (scope == null || !scope.retired) return false;
        var retained = identity.drainReceipt();
        var attempt = retained.getAttempt();
        if (retained.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED
                || retained.getExecutionDrainState() != org.pangrova.protocol.v2.ExecutionDrainState
                        .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED
                || !report.queryId().equals(attempt.getQueryOrJobId())
                || report.stageId() != attempt.getStageId() || report.partitionId() != attempt.getPartitionId()) {
            return false;
        }
        var notice = report.canonicalStatus();
        return notice.getAttempt().equals(attempt) && notice.getState() == retained.getState()
                && notice.getReportOrigin() == org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE
                && !notice.hasArtifactWork() && !notice.hasError() && notice.getUnknownFields().asMap().isEmpty()
                && notice.getExecutionDrainState() == org.pangrova.protocol.v2.ExecutionDrainState
                        .EXECUTION_DRAIN_STATE_UNSPECIFIED;
    }

    private boolean hasLiveDrainReceipt(org.pangrova.protocol.rest.task.TaskStatusReport report, Identity identity) {
        return identity != null && identity.drainReceipt() != null
                && identity.ackDeadline() - clock.getAsLong() > 0
                && report.workerId().equals(identity.workerId())
                && report.canonicalEvidence() != null && report.reportSequence() > 0;
    }

    private static boolean matchesDrainReceipt(org.pangrova.protocol.rest.task.TaskStatusReport report,
            Identity identity, org.pangrova.protocol.v2.TaskReport actual) {
        return actual.getAttempt().getQueryOrJobId().equals(report.queryId())
                && actual.getState().name().equals("TASK_STATE_" + report.state())
                && actual.getAttempt().getStageId() == report.stageId()
                && actual.getAttempt().getPartitionId() == report.partitionId()
                && identity.attempt().equals(report.attempt())
                && sameExecutionDrain(actual, identity.drainReceipt());
    }

    private static boolean sameExecutionDrain(org.pangrova.protocol.v2.TaskReport first,
            org.pangrova.protocol.v2.TaskReport second) {
        var observed = org.pangrova.protocol.task.CanonicalTaskAdmissionSeals
                .compactExecutionDrainReceipt(first);
        var retained = org.pangrova.protocol.task.CanonicalTaskAdmissionSeals
                .compactExecutionDrainReceipt(second);
        if (observed.equals(retained)) {
            return true;
        }
        // A Worker-reconciled terminal report can carry the same Native exit
        // evidence as a cancellation response. Only the envelope origin differs;
        // attempt, state, failure classification and drain checkpoint must match.
        if (observed.getExecutionDrainState() != org.pangrova.protocol.v2.ExecutionDrainState
                .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED
                || retained.getExecutionDrainState() != org.pangrova.protocol.v2.ExecutionDrainState
                .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED) {
            return false;
        }
        boolean reconciledNativePair = (observed.getReportOrigin()
                == org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED
                && retained.getReportOrigin()
                == org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                || (observed.getReportOrigin()
                == org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE
                && retained.getReportOrigin()
                == org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
        return reconciledNativePair && observed.toBuilder().clearReportOrigin().build()
                .equals(retained.toBuilder().clearReportOrigin().build());
    }

    synchronized boolean acknowledgesPublication(org.pangrova.protocol.rest.task.TaskStatusReport report) {
        Scope scope = scopes.get(report.queryId());
        if (scope == null || scope.retired) return false;
        Fact fact = scope.publications.get(report.taskId());
        return fact != null && fact.notificationsCompleted && fact.matches(report);
    }

    private void purgeRetiredQueries() {
        long now = clock.getAsLong();
        scopes.entrySet().removeIf(entry -> {
            Scope scope = entry.getValue();
            if (!scope.retired || scope.deadline - now > 0) return false;
            scope.tasks.forEach(identities::remove);
            return true;
        });
    }

    synchronized boolean removeRequest(String taskId, SubmitTaskRequest request) {
        var removed = new boolean[1];
        identities.computeIfPresent(taskId, (ignored, existing) -> {
            if (request.equals(existing.request())) {
                requireRetirable(existing);
                Scope scope = scopes.get(request.taskRequest().getAttempt().getQueryOrJobId());
                Fact publication = scope.publications.get(taskId);
                if (publication != null && !publication.notificationsCompleted) {
                    throw new IllegalStateException("Task still owns required report publication");
                }
                if (existing.drainReceipt() == null) releaseWorkerLoad(existing);
                scope.tasks.remove(taskId);
                scope.scheduled.remove(taskId);
                removed[0] = true;
                return null;
            }
            return existing;
        });
        return removed[0];
    }

    synchronized void compactDrainedTask(String taskId, SubmitTaskRequest request,
            org.pangrova.protocol.v2.TaskReport receipt) {
        Identity identity = identities.get(taskId);
        if (identity == null || identity.drainReceipt() == null) {
            throw new IllegalStateException("Task has no actual drain receipt");
        }
        if (!taskId.equals(request.taskRequest().getAttempt().getTaskId())) {
            throw new IllegalArgumentException("Task compaction identity changed");
        }
        if (!request.taskRequest().getAttempt().equals(receipt.getAttempt())
                || !identity.attempt().equals(org.pangrova.protocol.rest.task.TaskAttemptDescriptor
                        .from(receipt.getAttempt()))
                || !sameExecutionDrain(identity.drainReceipt(), receipt)) {
            throw new IllegalArgumentException("Task compaction drain receipt changed");
        }
        // A settled task can already be compacted while an in-flight report
        // fact briefly retains its issued request. That fact may finish and
        // clear the request between the caller's lookup and this method.
        if (identity.request() == null) return;
        if (!request.equals(identity.request())) {
            throw new IllegalStateException("Task has no exact retained issued request");
        }
        identities.put(taskId, identity.compact());
    }

    synchronized void registerAttempt(String taskId, TaskAttemptDescriptor attempt) {
        requireTaskId(taskId);
        if (attempt == null) {
            throw new IllegalArgumentException("attempt must not be null");
        }
        identities.compute(taskId, (ignored, existing) -> {
            requireSameAttempt(taskId, existing, attempt);
            return existing != null ? existing : new Identity(attempt, null, null, null, false);
        });
    }

    TaskAttemptDescriptor attempt(String taskId) {
        Identity identity = identities.get(taskId);
        return identity != null ? identity.attempt() : null;
    }

    synchronized SubmitTaskRequest request(String taskId) {
        Identity identity = identities.get(taskId);
        return retainedRequest(taskId, identity);
    }

    synchronized void remove(String taskId) {
        Identity existing = identities.get(taskId);
        if (existing == null) return;
        requireRetirable(existing);
        if (existing.drainReceipt() != null) {
            identities.put(taskId, existing.compact());
            return;
        }
        if (existing.request() != null) {
            scopes.get(existing.request().taskRequest().getAttempt().getQueryOrJobId()).tasks.remove(taskId);
        }
        releaseWorkerLoad(existing);
        identities.remove(taskId, existing);
    }

    synchronized void recordExecutionDrain(SubmitTaskRequest request, org.pangrova.protocol.v2.TaskReport report) {
        if (!report.getAttempt().equals(request.taskRequest().getAttempt())
                || report.getExecutionDrainState() == org.pangrova.protocol.v2.ExecutionDrainState
                        .EXECUTION_DRAIN_STATE_UNSPECIFIED
                || report.getExecutionDrainState() == org.pangrova.protocol.v2.ExecutionDrainState.UNRECOGNIZED) {
            throw new IllegalArgumentException("Execution drain receipt lacks its exact issued authority");
        }
        String taskId = report.getAttempt().getTaskId();
        org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(report);
        identities.compute(taskId, (ignored, existing) -> {
            if (existing == null || !request.equals(retainedRequest(taskId, existing))) {
                throw new IllegalStateException("Execution drain receipt has no exact live task request");
            }
            if (existing.drainReceipt() != null && !sameExecutionDrain(existing.drainReceipt(), report)) {
                throw new IllegalStateException("Exact execution drain evidence changed after first acceptance");
            }
            if (existing.drainReceipt() != null) {
                return existing;
            }
            long ackDeadline = clock.getAsLong()
                    + org.pangrova.protocol.constants.TerminalReportDeliveryPolicy.ACK_RETENTION.toNanos();
            Identity drained = new Identity(existing.attempt(), existing.request(), report, existing.worker(),
                    existing.entered(), existing.workerId(), existing.deadline(), ackDeadline,
                    existing.directReadResult());
            releaseWorkerLoad(existing);
            return drained;
        });
    }

    private Identity requireIdentity(SubmitTaskRequest request) {
        Identity existing = identities.get(request.taskRequest().getAttempt().getTaskId());
        if (existing == null || !request.equals(retainedRequest(
                request.taskRequest().getAttempt().getTaskId(), existing))) {
            throw new IllegalStateException("Task has no exact retained issued request");
        }
        return existing;
    }

    private SubmitTaskRequest retainedRequest(String taskId, Identity identity) {
        if (identity == null || identity.request() != null) return identity == null ? null : identity.request();
        if (identity.drainReceipt() == null) return null;
        Scope scope = scopes.get(identity.drainReceipt().getAttempt().getQueryOrJobId());
        Fact fact = scope == null ? null : scope.publications.get(taskId);
        return fact != null && !fact.notificationsCompleted ? fact.request : null;
    }

    private static void requireRetirable(Identity identity) {
        if (identity.entered() && identity.drainReceipt() == null) {
            throw new IllegalStateException("Cannot retire an entered task without actual drain evidence");
        }
    }

    java.util.Optional<org.pangrova.protocol.v2.TaskReport> executionDrainReceipt(String taskId) {
        var identity = identities.get(taskId);
        return java.util.Optional.ofNullable(identity == null ? null : identity.drainReceipt());
    }

    private static void requireTaskId(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be null or blank");
        }
    }

    private static void requireSameAttempt(String taskId, Identity existing, TaskAttemptDescriptor requested) {
        if (existing != null && !existing.attempt().equals(requested)) {
            throw new IllegalStateException("Conflicting task attempt already registered: taskId=" + taskId
                    + ", existing=" + existing.attempt() + ", requested=" + requested);
        }
    }

    private record Identity(TaskAttemptDescriptor attempt, SubmitTaskRequest request,
                            org.pangrova.protocol.v2.TaskReport drainReceipt, WorkerInfo worker, boolean entered,
                            String workerId, long deadline, long ackDeadline, boolean directReadResult) {
        private Identity(TaskAttemptDescriptor attempt, SubmitTaskRequest request,
                         org.pangrova.protocol.v2.TaskReport drainReceipt, WorkerInfo worker, boolean entered) {
            this(attempt, request, drainReceipt, worker, entered,
                    worker == null ? null : worker.workerId(),
                    request == null ? 0L : request.taskRequest().getDeadlineEpochMs(), 0L,
                    request != null && request.taskRequest().getExecutionOptions().getDirectResultOutput()
                            && !request.taskRequest().getExecutionOptions().hasWrite()
                            && (request.taskRequest().getTaskKind() == org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ
                            || request.taskRequest().getTaskKind()
                                    == org.pangrova.protocol.v2.TaskKind.TASK_KIND_SHUFFLE_READ));
        }

        private Identity compact() {
            return new Identity(attempt, null,
                    org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(drainReceipt),
                    null, entered, workerId, deadline, ackDeadline, directReadResult);
        }
    }

    private final class Scope {
        private final org.pangrova.connector.api.query.ConnectorQueryAuthority connectorAuthority;
        private final Set<String> tasks = new HashSet<>();
        private final HashMap<String, org.pangrova.query.api.journal.TaskStateEvent> scheduled = new HashMap<>();
        private boolean sealed;
        private boolean retired;
        private long deadline;
        private int operations;
        private boolean cleanupClaimed;
        private final HashMap<String, Fact> publications = new HashMap<>();

        private Scope(String queryId) {
            connectorAuthority = new IssuedConnectorAuthority(queryId, this);
        }
    }

    private final class IssuedConnectorAuthority implements org.pangrova.connector.api.query.ConnectorQueryAuthority {
        private final String queryId;
        private final Scope scope;

        private IssuedConnectorAuthority(String queryId, Scope scope) {
            this.queryId = queryId;
            this.scope = scope;
        }

        @Override public String queryId() { return queryId; }

        @Override
        public boolean isClosed() {
            synchronized (TaskIdentityRegistry.this) {
                return scope.sealed || scope.retired || scope.cleanupClaimed || scopes.get(queryId) != scope;
            }
        }

        @Override
        public void requireDrained() {
            synchronized (TaskIdentityRegistry.this) {
                if (scopes.get(queryId) != scope) {
                    if (scope.cleanupClaimed && scope.retired && scope.operations == 0) return;
                    throw new IllegalStateException(
                            "Connector cleanup does not own the issued query scope: " + queryId);
                }
                requireQueryDrained(queryId);
            }
        }

        @Override
        public org.pangrova.connector.api.query.ConnectorQueryOperation enterOperation() {
            synchronized (TaskIdentityRegistry.this) {
                if (scope.sealed || scope.retired || scope.cleanupClaimed || scopes.get(queryId) != scope) {
                    throw new IllegalStateException("Connector query admission is closed: " + queryId);
                }
                QueryIssuanceOperation operation = new QueryIssuanceOperation(() -> {
                    exitQueryOperation(scope);
                    connectorOperationExited.accept(queryId);
                });
                scope.operations++;
                return operation;
            }
        }
    }

}
