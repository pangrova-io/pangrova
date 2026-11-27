package org.pangrova.coordinator.task;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.query.execution.QueryOwnershipTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.persistence.query.api.lifecycle.ShuffleCleanupIntentStore;
import org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore;
import org.pangrova.query.api.lifecycle.QueryExecutionCleanup;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.query.lifecycle.QueryTerminalCleanup;
import org.pangrova.shuffle.api.ShuffleCleanupRecovery;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.protocol.constants.TerminalReportDeliveryPolicy;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Protocol/issuance boundary oracles; generated receipts are not Native execution proof. */
class TaskIdentityRegistryDrainContractTest {
    @Test
    void enteredIdentityCannotBeRemovedOrRetiredOnLogicalCompletion() {
        var registry = new TaskIdentityRegistry();
        var request = request();
        issue(registry, request);
        registry.enterDispatch(request);
        registry.sealQuery("query");
        assertThat(registry.tryClaimQueryCleanup("query")).isFalse();
        assertThatThrownBy(() -> registry.remove("task")).hasMessageContaining("without actual drain evidence");
        assertThatThrownBy(() -> registry.requireQueryDrained("query")).hasMessageContaining("lacks its exact");
        registry.recordExecutionDrain(request, receipt(request));
        assertThat(registry.tryClaimQueryCleanup("query")).isTrue();
        registry.requireQueryDrained("query");
        registry.retireQuery("query");
        assertThat(registry.request("task")).isNull();
        assertThat(registry.executionDrainReceipt("task")).contains(receipt(request));
    }

    @Test
    void exactExitSignalsActualQueryAndRemoteShuffleCleanupWithoutPolling() {
        var tracker = new CoordinatorTaskTracker();
        var request = request();
        tracker.openQueryIssuance("query");
        tracker.registerTaskRequest("task", request);
        tracker.bindDispatchTarget(request, TestWorkerInfoBuilder.active("worker", "original-host", 9001, 9002));
        tracker.enterTaskDispatch(request);
        var ready = new java.util.ArrayList<String>();
        var shuffle = new java.util.ArrayList<String>();
        var workerManager = new WorkerManager(20_000, 60_000,
                new InMemoryWorkerRegistry());
        try (var authority = new HighAvailabilityCoordinationService(
                new InMemoryDistributedLease(),
                new InMemoryExecutionAnchorStore())) {
            assertThat(authority.acquireQueryOwnership("query", "owner")).isTrue();
            var grant = authority.requireExecutionAnchor("query");
            var executionOwner = new QueryOwnershipTracker(
                    tracker, workerManager, null, new TaskCancelRetrier((attempt, worker) -> null, tracker));
            executionOwner.setOwnershipTracking(authority, "owner");
            executionOwner.setShuffleCleanup(new ShuffleCleanupIntentStore() {
                @Override
                public void markPending(ExecutionAnchor anchor) {
                    throw new AssertionError("Physical cleanup never recreates the terminal intent");
                }

                @Override
                public boolean completeWithoutScope(ExecutionAnchor anchor) {
                    throw new AssertionError("A tracked scope uses its current cleanup capability");
                }
            }, new ShuffleCleanupRecovery() {
                @Override
                public boolean cleanupTrackedShuffle(String queryId, ShuffleExecutionOwner owner) {
                    assertThat(owner.fencingToken()).isEqualTo(grant.fencingToken());
                    shuffle.add(queryId);
                    return true;
                }

                @Override
                public void recoverCleanupShuffle(String queryId, ShuffleExecutionOwner owner) {
                    throw new AssertionError("Normal tracked cleanup has no recovery RPC");
                }
            });
            verifyExactDrainTriggersCleanup(tracker, executionOwner, grant, request, ready, shuffle);
        }
    }

    private static void verifyExactDrainTriggersCleanup(CoordinatorTaskTracker tracker,
            QueryOwnershipTracker executionOwner,
            ExecutionAnchor grant, SubmitTaskRequest request,
            java.util.List<String> ready, java.util.List<String> shuffle) {
        var execution = new QueryExecutionCleanup() {
            @Override public void cancelDispatchedTasks(String queryId) {
                throw new AssertionError("A finished query must not request cancellation");
            }
            @Override public boolean tryCleanupQuery(String queryId,
                    ExecutionAnchor originalGrant) {
                return executionOwner.tryCleanupQuery(queryId, originalGrant);
            }
            @Override public boolean tryCleanupUnadmittedQuery(String queryId) {
                throw new AssertionError("The query already owns execution");
            }
        };
        var cleanup = new QueryTerminalCleanup(execution,
                ignored -> { }, ignored -> { }, context -> ready.add(context.queryId()), () -> 0L);
        tracker.addQueryDrainProgressListener(cleanup::onDrainProgress);
        var state = new QueryStateMachine();
        state.bindJournalGrant(grant);
        state.toFinisher().run();
        cleanup.complete("query", state, "test");
        cleanup.retryPending();
        assertThat(ready).isEmpty();
        assertThat(shuffle).isEmpty();

        tracker.recordExecutionDrain(request, receipt(request));
        cleanup.retryPending();
        assertThat(ready).containsExactly("query");
        assertThat(shuffle).containsExactly("query");
        tracker.retireQueryIssuance("query");
    }

    @Test
    void sealingBlocksBothLateTransportEntryAndNewIdentity() {
        var registry = new TaskIdentityRegistry();
        var request = request();
        issue(registry, request);
        registry.sealQuery("query");
        assertThatThrownBy(() -> registry.enterDispatch(request)).hasMessageContaining("no open issuance");
        assertThatThrownBy(() -> registry.openQuery("query")).hasMessageContaining("sealed");
    }

    @Test
    void placementLoadTracksOneExactTargetUntilItsFirstPhysicalReceipt() {
        var registry = new TaskIdentityRegistry();
        var request = request();
        var worker = TestWorkerInfoBuilder.active("worker", "original-host", 8001, 8002);
        var replacement = new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity(
                worker.workerExecutionIdentity().admissionDomainId(),
                java.util.UUID.randomUUID().toString());
        registry.openQuery("query");
        registry.registerRequest("task", request);
        registry.bindTarget(request, worker);
        registry.bindTarget(request, worker);
        assertThat(registry.workerLoad(worker.workerExecutionIdentity())).isOne();
        assertThat(registry.workerLoad(replacement)).isZero();

        registry.enterDispatch(request);
        registry.sealQuery("query");
        assertThatThrownBy(() -> registry.remove("task"))
                .hasMessageContaining("without actual drain evidence");
        assertThat(registry.workerLoad(worker.workerExecutionIdentity())).isOne();
        var drained = receipt(request);
        registry.recordExecutionDrain(request, drained);
        registry.recordExecutionDrain(request, drained);
        registry.retireQuery("query");
        assertThat(registry.workerLoad(worker.workerExecutionIdentity())).isZero();
    }

    @Test
    void placementLoadReturnsWhenPreparedTaskIsDiscardedBeforeTransport() {
        var registry = new TaskIdentityRegistry();
        var request = request();
        var target = TestWorkerInfoBuilder.executionIdentity("worker");
        issue(registry, request);
        assertThat(registry.removeRequest("task", request)).isTrue();
        assertThat(registry.workerLoad(target)).isZero();
        issue(registry, request);
        assertThat(registry.workerLoad(target)).isOne();
        registry.remove("task");
        assertThat(registry.workerLoad(target)).isZero();
    }

    @Test
    void activeFrontendOperationBlocksEvenAnEmptyIssuanceScope() {
        var registry = new TaskIdentityRegistry();
        registry.openQuery("query");
        Runnable exited = registry.enterQueryOperation("query");
        registry.sealQuery("query");
        assertThatThrownBy(() -> registry.requireQueryDrained("query")).hasMessageContaining("operations have not exited");
        exited.run();
        registry.requireQueryDrained("query");
        assertThatThrownBy(() -> registry.requireQueryDrained("never-issued")).hasMessageContaining("not sealed");
    }

    @Test
    void actualDispatchTargetIsImmutableAndSurvivesLogicalTrackingRemoval() {
        var registry = new TaskIdentityRegistry();
        var request = request();
        issue(registry, request);
        assertThatThrownBy(() -> registry.bindTarget(request,
                TestWorkerInfoBuilder.active("replacement", "other-host", 9001, 9002)))
                .hasMessageContaining("target changed");
        assertThat(registry.sealQuery("query")).singleElement().satisfies(issued -> {
            assertThat(issued.worker().workerId()).isEqualTo("worker");
            assertThat(issued.worker().host()).isEqualTo("original-host");
        });
    }

    @Test
    void firstAcceptedReceiptAfterExecutionDeadlineHasFixedLostAckRetention() {
        AtomicLong clock = new AtomicLong(1000);
        var registry = new TaskIdentityRegistry(clock::get);
        var request = SubmitTaskRequest.from(request().taskRequest().toBuilder().setDeadlineEpochMs(100).build());
        issue(registry, request);
        registry.enterDispatch(request);
        registry.sealQuery("query");
        TaskReport actual = receipt(request);
        TaskStatusReport retry = projection(request, actual);
        registry.recordExecutionDrain(request, actual);
        assertThat(registry.acknowledgesDrain(retry)).isFalse();
        // This is the actual publication lease/acceptance/exit contract, not Native drain proof.
        AtomicBoolean operationExitObserved = new AtomicBoolean();
        try (var publication = registry.enterReport(request, retry, false, () -> { }, () -> {
            assertThat(registry.queryOperationsDrained("query")).isTrue();
            operationExitObserved.set(true);
        })) {
            assertThat(publication.report()).isSameAs(retry);
            publication.accepted();
            assertThat(registry.acknowledgesDrain(retry)).isFalse();
            assertThatThrownBy(() -> registry.retireQuery("query"))
                    .hasMessageContaining("operations have not exited");
        }
        assertThat(operationExitObserved).isTrue();
        registry.retireQuery("query");
        assertThat(registry.acknowledgesDrain(retry)).isTrue();
        clock.addAndGet(TerminalReportDeliveryPolicy.ACK_RETENTION.toNanos() - 1);
        registry.openQuery("unrelated");
        assertThat(registry.acknowledgesDrain(retry)).isTrue();
        clock.incrementAndGet();
        registry.openQuery("another");
        assertThat(registry.acknowledgesDrain(retry)).isFalse();
        assertThat(registry.executionDrainReceipt("task")).isEmpty();
    }

    @Test
    void alreadyCompactedDrainSurvivesPublicationFactClearingItsRequest() {
        var registry = new TaskIdentityRegistry();
        var request = request();
        issue(registry, request);
        registry.enterDispatch(request);
        registry.sealQuery("query");
        TaskReport actual = receipt(request);
        registry.recordExecutionDrain(request, actual);
        var report = projection(request, actual);
        var publication = registry.enterReport(request, report, false, () -> { }, () -> { });
        registry.remove("task");
        assertThat(registry.request("task")).isEqualTo(request);
        publication.accepted();
        publication.close();
        assertThat(registry.request("task")).isNull();

        registry.compactDrainedTask("task", request, actual);
        assertThat(registry.executionDrainReceipt("task")).contains(
                org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(actual));
        assertThatThrownBy(() -> registry.compactDrainedTask("task", request,
                actual.toBuilder().setAttempt(actual.getAttempt().toBuilder().setTaskId("other")).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void delayedLoserReportAcknowledgesOnlyAnExplicitlyRetiredExactDrain() {
        var registry = new TaskIdentityRegistry();
        var request = request();
        issue(registry, request);
        registry.enterDispatch(request);
        var actual = receipt(request);
        var delayed = projection(request, actual);
        registry.recordExecutionDrain(request, actual);
        assertThat(registry.acknowledgesDrain(delayed)).isFalse();

        registry.compactDrainedTask("task", request, actual);
        assertThat(registry.acknowledgesDrain(delayed)).isTrue();
        var changed = new TaskStatusReport("task", "query", 0, 0, "OUTPUT", "other-worker", "FINISHED",
                null, null, null, 1L, 1000L, null, null,
                TaskAttemptDescriptor.from(request.taskRequest().getAttempt()), actual.toByteArray());
        assertThat(registry.acknowledgesDrain(changed)).isFalse();
        var changedReceipt = actual.toBuilder().setState(TaskState.TASK_STATE_FAILED).build();
        assertThat(registry.acknowledgesDrain(projection(request, changedReceipt))).isFalse();
    }

    @Test
    void anIndependentReleaseCanSupersedeOnlyTheRetiredUndrainedResultNotice() {
        var registry = new TaskIdentityRegistry();
        var result = request().taskRequest().toBuilder();
        result.getExecutionOptionsBuilder().setDirectResultOutput(true);
        var request = SubmitTaskRequest.from(result.build());
        issue(registry, request);
        registry.enterDispatch(request);
        var released = receipt(request);
        var notice = released.toBuilder().clearExecutionDrainState().build();
        var envelope = projection(request, notice);
        registry.recordExecutionDrain(request, released);
        assertThat(registry.supersedesResultNotice(envelope)).isFalse();
        registry.sealQuery("query");
        registry.retireQuery("query");

        assertThat(registry.supersedesResultNotice(envelope)).isTrue();
        assertThat(registry.acknowledgesDrain(envelope)).isFalse();
        assertThat(registry.supersedesResultNotice(projection(request, released))).isFalse();
        assertThat(registry.supersedesResultNotice(projection(request,
                notice.toBuilder().setAttempt(notice.getAttempt().toBuilder().setFencingToken(123456)).build())))
                .isFalse();
        assertThat(registry.supersedesResultNotice(projection(request,
                notice.toBuilder().setArtifactWork(org.pangrova.protocol.v2.ArtifactWorkReport.getDefaultInstance())
                        .build()))).isFalse();
    }

    @Test
    void retiredNativeFailureAcknowledgesOnlyItsMatchingWorkerReconciliation() {
        var registry = new TaskIdentityRegistry();
        var request = request();
        issue(registry, request);
        registry.enterDispatch(request);
        var nativeFailure = receipt(request).toBuilder()
                .setState(TaskState.TASK_STATE_FAILED)
                .setError(ExecutionError.newBuilder().setErrorClass(ErrorClass.ERROR_CLASS_INTERNAL)
                        .setCode("NATIVE_TASK_FAILED").setFailedComponent("native-execution")
                        .setMessage("attempt failed"))
                .build();
        var reconciled = nativeFailure.toBuilder()
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED).build();
        registry.recordExecutionDrain(request, nativeFailure);
        assertThat(registry.acknowledgesDrain(failedProjection(request, reconciled))).isFalse();
        registry.recordExecutionDrain(request, reconciled);
        registry.compactDrainedTask("task", request, reconciled);
        assertThat(registry.acknowledgesDrain(failedProjection(request, reconciled))).isTrue();
        assertThat(registry.acknowledgesDrain(failedProjection(request,
                reconciled.toBuilder().setError(reconciled.getError().toBuilder()
                        .setCode("DIFFERENT_FAILURE")).build()))).isFalse();
        assertThat(registry.acknowledgesDrain(failedProjection(request,
                reconciled.toBuilder().setState(TaskState.TASK_STATE_CANCELLED).build()))).isFalse();
        assertThat(registry.acknowledgesDrain(failedProjection(request,
                reconciled.toBuilder().setAttempt(reconciled.getAttempt().toBuilder()
                        .setTaskId("different-task")).build()))).isFalse();
    }

    @Test
    void undrainedScopeIsNotPurgedAfterItsExecutionDeadline() {
        AtomicLong clock = new AtomicLong(1000);
        var registry = new TaskIdentityRegistry(clock::get);
        var request = request();
        issue(registry, request);
        registry.enterDispatch(request);
        registry.sealQuery("query");
        clock.set(Long.MAX_VALUE / 2);
        registry.openQuery("unrelated");
        assertThat(registry.request("task")).isEqualTo(request);
        assertThatThrownBy(() -> registry.retireQuery("query")).hasMessageContaining("lacks its exact");
    }

    private static void issue(TaskIdentityRegistry registry, SubmitTaskRequest request) {
        registry.openQuery("query");
        registry.registerRequest("task", request);
        registry.bindTarget(request, TestWorkerInfoBuilder.active("worker", "original-host", 8001, 8002));
    }

    private static SubmitTaskRequest request() {
        return CanonicalTaskRequestTestFixture.request("task", "query", 0, 0, TaskKind.TASK_KIND_READ, new byte[]{1});
    }

    private static TaskReport receipt(SubmitTaskRequest request) {
        return TaskReport.newBuilder().setAttempt(request.taskRequest().getAttempt())
                .setState(TaskState.TASK_STATE_FINISHED).setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED).build();
    }

    private static TaskStatusReport projection(SubmitTaskRequest request, TaskReport actual) {
        return new TaskStatusReport("task", "query", 0, 0, "OUTPUT", "worker", "FINISHED",
                null, null, null, 1L, 1000L, null, null,
                TaskAttemptDescriptor.from(request.taskRequest().getAttempt()), actual.toByteArray());
    }

    private static TaskStatusReport failedProjection(SubmitTaskRequest request, TaskReport actual) {
        return new TaskStatusReport("task", "query", 0, 0, "OUTPUT", "worker", "FAILED",
                null, null, null, 1L, 1000L, null, null,
                TaskAttemptDescriptor.from(request.taskRequest().getAttempt()), actual.toByteArray());
    }
}
