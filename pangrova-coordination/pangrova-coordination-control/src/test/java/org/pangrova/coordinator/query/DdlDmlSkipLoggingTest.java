package org.pangrova.coordinator.query;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.query.api.session.QuerySession;

import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.phase.StatementDispatchPhase;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.testing.testkit.constants.TestObservability;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that QueryDispatcher emits a structured DEBUG log when
 * StatementDispatcher is null (metadata module disabled).
 *
 * <p>Uses Logback ListAppender to capture log output (no mocks).</p>
 *
 * <p>Validates: Requirements R2.4 — Null Propagation Logging</p>
 */
class DdlDmlSkipLoggingTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger dispatcherLogger;
    private QueryDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcherLogger = (Logger) LoggerFactory.getLogger(StatementDispatchPhase.class);
        appender = new ListAppender<>();
        appender.start();
        dispatcherLogger.addAppender(appender);
        dispatcherLogger.setLevel(Level.DEBUG);

        SqlCompiler compiler = new SqlCompiler();
        RemoteTaskDispatcher httpDispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        workerManager.registerWorker(
                TestWorkerInfoBuilder.active("w1", "localhost", 9999, 0));
        var deps = QueryDispatcherContext.withDefaults(
                compiler, null, null, null, httpDispatcher,
                new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()),
                workerManager, new CoordinatorTaskTracker(),
                null, null, null, 0L);
        // statementDispatcher = null → QueryDispatcher skips DDL/DML dispatch
        dispatcher = new QueryDispatcher(deps, null, null);
    }

    @AfterEach
    void tearDown() {
        dispatcherLogger.detachAppender(appender);
        appender.stop();
    }

    @Test
    void statementDispatcherNull_emitsDebugLogOnExecute() {
        QueryStateMachine sm = new QueryStateMachine();
        QuerySession session = new QuerySession(
                "q-ddl-skip", Map.of(), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        // Execute a SELECT — tryExecuteDdlDml is called first, finds statementDispatcher==null,
        // logs the skip, then falls through to normal query dispatch.
        // The dispatch itself may fail (no real worker), but the log should be captured.
        try {
            dispatcher.execute(sm, "SELECT 1", session);
        } catch (Exception ignored) {
            // dispatch failure is expected — we only care about the log
        }

        assertThat(appender.list.stream()
                .filter(e -> e.getLevel() == Level.DEBUG)
                .map(ILoggingEvent::getFormattedMessage)
                .anyMatch(msg -> msg.contains("event=ddl_dml_skip")
                        && msg.contains("reason=metadata_disabled")))
                .as("Expected DEBUG log with event=ddl_dml_skip reason=metadata_disabled")
                .isTrue();
    }
}
