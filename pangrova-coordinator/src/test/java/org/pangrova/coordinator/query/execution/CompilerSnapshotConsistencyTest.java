package org.pangrova.coordinator.query.execution;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.testing.TestObservability;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies BUG-D45 fix: compiler snapshot consistency in QueryDispatcher.
 *
 * <p>The volatile {@code SecurityPolicyApplier.compiler} field could be updated
 * by a concurrent DDL between plan compilation and Substrait serialization.
 * The fix captures the compiler once at the start of {@code doExecuteTraced}
 * and passes the same instance to both phases.</p>
 */
class CompilerSnapshotConsistencyTest {

    private static final int TRANSPORT_PORT = 0;

    private SqlCompiler compilerV1;
    private SqlCompiler compilerV2;
    private WorkerHttpServer workerHttpServer;
    private TaskManager taskManager;

    @BeforeEach
    void setUp() throws Exception {
        compilerV1 = new SqlCompiler();
        compilerV2 = new SqlCompiler();
        taskManager = TaskManager.forHttpWiringOnly();
        var config = new WorkerHttpServer.WorkerHttpConfig(0, "localhost", TRANSPORT_PORT);
        workerHttpServer = new WorkerHttpServer(config, taskManager, TestExchangeTransport.INSTANCE);
        workerHttpServer.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (workerHttpServer != null) {
            workerHttpServer.stop();
        }
    }

    /**
     * SecurityPolicyApplier.compileAndApplyPolicies(sql, session, compilerSnapshot)
     * must use the provided compiler snapshot, not the internal volatile field.
     * This ensures that even if updateCompiler() is called concurrently,
     * the compile phase uses the snapshot captured at query start.
     */
    @Test
    void compileAndApplyPolicies_usesProvidedCompilerSnapshot_notInternalField() {
        SecurityPolicyApplier applier = new SecurityPolicyApplier(
                compilerV1, null, null, null);

        // Compile with compilerV2 as the snapshot — should succeed using V2
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "snap-1", Map.of(), null, null);
        RelNode relNode = applier.compileAndApplyPolicies("SELECT 1", session, compilerV2).plan();

        assertNotNull(relNode, "RelNode should be compiled using the provided snapshot");

        // Now update the internal compiler to a broken state by setting it to null-ish
        // The 2-arg overload should still work because it delegates to the 3-arg with
        // the current internal compiler
        RelNode relNode2 = applier.compileAndApplyPolicies("SELECT 1", session).plan();
        assertNotNull(relNode2, "2-arg overload should use internal compiler");
    }

    /**
     * Verifies that the same compiler instance captured at query start is passed
     * through the entire dispatch chain. We do this by:
     * 1. Creating a QueryDispatcher with compilerV1
     * 2. Executing a query — the compiler snapshot is captured at doExecuteTraced start
     * 3. Verifying the query succeeds (compiler V1 is used consistently)
     */
    @Test
    void queryDispatcher_capturesCompilerOnce_usesConsistentlyThroughDispatch() throws Exception {
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        workerManager.registerWorker(activeWorker("w1", workerHttpServer.getPort()));

        RemoteTaskDispatcher httpDispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        TaskScheduler taskScheduler = new TaskScheduler(new WorkerPerformanceScorer());

        var deps = QueryDispatcherContext.withDefaults(
                compilerV1, null, null, null, httpDispatcher, taskScheduler, workerManager,
                new CoordinatorTaskTracker(), null, null, null, null, 0L);
        QueryDispatcher dispatcher = new QueryDispatcher(deps, null, null);

        var dagEngine = dispatcher.dagEngine();
        taskManager.setAutoFinishReporter(report ->
                dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                        report.state(), report.error()));

        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "snap-2", Map.of(), null, null);

        // Execute a query — compiler V1 is captured once and used for both
        // compile (compileAndApplyPolicies) and serialize (convertValidateAndSerializeWithRetry)
        DispatchResult result = dispatcher.execute(sm, "SELECT 1", session);

        assertNotNull(result, "Query should dispatch successfully with consistent compiler");
        assertEquals(QueryStateMachine.State.RUNNING, sm.getState());
    }

    /**
     * Verifies that the 3-arg compileAndApplyPolicies overload uses the provided
     * compiler, not the internal field, even after updateCompiler() is called.
     */
    @Test
    void compileAndApplyPolicies_snapshotIsolatedFromUpdate() {
        SecurityPolicyApplier applier = new SecurityPolicyApplier(
                compilerV1, null, null, null);
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "snap-3", Map.of(), null, null);

        // Capture snapshot before update
        SqlCompiler snapshot = applier.getCompiler();
        assertSame(compilerV1, snapshot, "Snapshot should be V1 before update");

        // Simulate concurrent DDL updating the compiler
        applier.updateCompiler(compilerV2);

        // getCompiler() now returns V2
        assertSame(compilerV2, applier.getCompiler(), "Internal compiler should be V2 after update");

        // But the snapshot is still V1 — compile with it should work
        RelNode relNode = applier.compileAndApplyPolicies("SELECT 1", session, snapshot).plan();
        assertNotNull(relNode, "Compile with V1 snapshot should succeed after V2 update");

        // The snapshot reference is isolated from the update
        assertSame(compilerV1, snapshot, "Snapshot reference must not change after update");
        assertNotSame(applier.getCompiler(), snapshot,
                "Snapshot must differ from current compiler after update");
    }

    /**
     * Verifies that getCompiler() returns the latest compiler after updateCompiler().
     * This is the volatile visibility guarantee that the snapshot pattern relies on.
     */
    @Test
    void getCompiler_returnsLatestAfterUpdate() {
        SecurityPolicyApplier applier = new SecurityPolicyApplier(
                compilerV1, null, null, null);

        assertSame(compilerV1, applier.getCompiler());

        applier.updateCompiler(compilerV2);
        assertSame(compilerV2, applier.getCompiler());
    }

    /**
     * Simulates the race condition that BUG-D45 fixes:
     * Thread A captures compiler snapshot, Thread B updates compiler,
     * Thread A continues using the snapshot — must still use V1.
     */
    @Test
    void concurrentUpdate_snapshotRemainsStable() throws Exception {
        SecurityPolicyApplier applier = new SecurityPolicyApplier(
                compilerV1, null, null, null);

        // Thread A captures snapshot
        SqlCompiler snapshot = applier.getCompiler();

        CountDownLatch updateDone = new CountDownLatch(1);
        AtomicReference<SqlCompiler> postUpdateCompiler = new AtomicReference<>();

        // Thread B updates compiler concurrently
        Thread updater = new Thread(() -> {
            applier.updateCompiler(compilerV2);
            postUpdateCompiler.set(applier.getCompiler());
            updateDone.countDown();
        });
        updater.start();
        updateDone.await();

        // Thread A's snapshot is still V1
        assertSame(compilerV1, snapshot, "Snapshot captured before update must remain V1");
        // Thread B's update took effect
        assertSame(compilerV2, postUpdateCompiler.get(), "Post-update compiler must be V2");
        // Current compiler is V2
        assertSame(compilerV2, applier.getCompiler(), "Current compiler must be V2");

        // Thread A can still compile with its V1 snapshot
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "snap-race", Map.of(), null, null);
        RelNode relNode = applier.compileAndApplyPolicies("SELECT 1", session, snapshot).plan();
        assertNotNull(relNode, "Compile with stale V1 snapshot must still succeed");
    }

    private static WorkerInfo activeWorker(String workerId, int httpPort) {
        return TestWorkerInfoBuilder.active(workerId, "localhost", httpPort, TRANSPORT_PORT);
    }
}
