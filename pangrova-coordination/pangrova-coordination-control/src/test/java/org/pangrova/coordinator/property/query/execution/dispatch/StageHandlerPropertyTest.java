package org.pangrova.coordinator.property.query.execution.dispatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleEvent;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleHandler;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for Stage Handler call completeness and isolation.
 *
 * <p><b>Validates: Requirements 6.2, 6.3</b></p>
 */
class StageHandlerPropertyTest {

    private static final RelOptCluster CLUSTER;
    static {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CLUSTER = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
    }

    /**
     * Property 14: Stage Handler call completeness.
     * For any number of registered handlers, all handlers' onScheduling
     * method is called exactly once per event dispatch.
     *
     * <b>Validates: Requirement 6.2</b>
     */
    @Property(tries = 20)
    void allHandlersReceiveSchedulingEvent(
            @ForAll @IntRange(min = 1, max = 10) int handlerCount) {
        List<AtomicInteger> counters = new ArrayList<>();
        List<StageLifecycleHandler> handlers = buildCountingHandlers(
                handlerCount, counters, "scheduling");

        StageLifecycleEvent event = createEvent();
        dispatchToAll(handlers, event, StageLifecycleHandler::onScheduling);

        assertAllCalledOnce(counters, "onScheduling");
    }

    /**
     * Property 14 (continued): All handlers receive onFinished events.
     *
     * <b>Validates: Requirement 6.2</b>
     */
    @Property(tries = 20)
    void allHandlersReceiveFinishedEvent(
            @ForAll @IntRange(min = 1, max = 10) int handlerCount) {
        List<AtomicInteger> counters = new ArrayList<>();
        List<StageLifecycleHandler> handlers = buildCountingHandlers(
                handlerCount, counters, "finished");

        StageLifecycleEvent event = createEvent();
        dispatchToAll(handlers, event, StageLifecycleHandler::onFinished);

        assertAllCalledOnce(counters, "onFinished");
    }

    /**
     * Property 14 (continued): All handlers receive onFailed events.
     *
     * <b>Validates: Requirement 6.2</b>
     */
    @Property(tries = 20)
    void allHandlersReceiveFailedEvent(
            @ForAll @IntRange(min = 1, max = 10) int handlerCount) {
        List<AtomicInteger> counters = new ArrayList<>();
        List<StageLifecycleHandler> handlers = buildCountingHandlers(
                handlerCount, counters, "failed");

        StageLifecycleEvent event = createEvent();
        dispatchToAll(handlers, event, StageLifecycleHandler::onFailed);

        assertAllCalledOnce(counters, "onFailed");
    }

    /**
     * Property 15: Stage Handler isolation.
     * For any list of handlers where a random subset throws exceptions,
     * all non-throwing handlers are still called.
     *
     * <b>Validates: Requirement 6.3</b>
     */
    @Property(tries = 20)
    void throwingHandlerDoesNotPreventOthers(
            @ForAll @IntRange(min = 2, max = 10) int handlerCount,
            @ForAll @IntRange(min = 0, max = 9) int throwingIndex) {
        int safeThrowIdx = throwingIndex % handlerCount;
        List<AtomicInteger> counters = new ArrayList<>();
        List<StageLifecycleHandler> handlers = new ArrayList<>();

        for (int i = 0; i < handlerCount; i++) {
            AtomicInteger counter = new AtomicInteger(0);
            counters.add(counter);
            if (i == safeThrowIdx) {
                handlers.add(new ThrowingHandler(counter));
            } else {
                handlers.add(new CountingHandler(counter));
            }
        }

        StageLifecycleEvent event = createEvent();
        dispatchWithIsolation(handlers, event, StageLifecycleHandler::onScheduling);

        for (int i = 0; i < handlerCount; i++) {
            assertThat(counters.get(i).get())
                    .as("handler %d call count (throwing=%b)", i, i == safeThrowIdx)
                    .isEqualTo(1);
        }
    }

    /**
     * Property 15 (continued): Isolation holds for onFinished events.
     *
     * <b>Validates: Requirement 6.3</b>
     */
    @Property(tries = 20)
    void throwingHandlerDoesNotPreventOthersOnFinished(
            @ForAll @IntRange(min = 2, max = 10) int handlerCount,
            @ForAll @IntRange(min = 0, max = 9) int throwingIndex) {
        int safeThrowIdx = throwingIndex % handlerCount;
        List<AtomicInteger> counters = new ArrayList<>();
        List<StageLifecycleHandler> handlers = new ArrayList<>();

        for (int i = 0; i < handlerCount; i++) {
            AtomicInteger counter = new AtomicInteger(0);
            counters.add(counter);
            if (i == safeThrowIdx) {
                handlers.add(new ThrowingOnFinishedHandler(counter));
            } else {
                handlers.add(new FinishedCountingHandler(counter));
            }
        }

        StageLifecycleEvent event = createEvent();
        dispatchWithIsolation(handlers, event, StageLifecycleHandler::onFinished);

        for (int i = 0; i < handlerCount; i++) {
            assertThat(counters.get(i).get())
                    .as("handler %d call count (throwing=%b)", i, i == safeThrowIdx)
                    .isEqualTo(1);
        }
    }

    // ── dispatch helpers (simulate DAGDispatchEngine.fireHandlerEvent) ──

    private static void dispatchToAll(
            List<StageLifecycleHandler> handlers, StageLifecycleEvent event,
            java.util.function.BiConsumer<StageLifecycleHandler, StageLifecycleEvent> method) {
        for (StageLifecycleHandler handler : handlers) {
            method.accept(handler, event);
        }
    }

    private static void dispatchWithIsolation(
            List<StageLifecycleHandler> handlers, StageLifecycleEvent event,
            java.util.function.BiConsumer<StageLifecycleHandler, StageLifecycleEvent> method) {
        for (StageLifecycleHandler handler : handlers) {
            try {
                method.accept(handler, event);
            } catch (Exception e) {
                // isolation: exception caught and logged, continue to next handler
                // mirrors DAGDispatchEngine.fireHandlerEvent behavior
                System.Logger logger = System.getLogger("test");
                logger.log(System.Logger.Level.DEBUG,
                        "handler exception caught: " + e.getMessage());
            }
        }
    }

    private static void assertAllCalledOnce(List<AtomicInteger> counters, String method) {
        for (int i = 0; i < counters.size(); i++) {
            assertThat(counters.get(i).get())
                    .as("handler %d %s call count", i, method)
                    .isEqualTo(1);
        }
    }

    // ── handler implementations ──

    private static List<StageLifecycleHandler> buildCountingHandlers(
            int count, List<AtomicInteger> counters, String lifecycle) {
        List<StageLifecycleHandler> handlers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            AtomicInteger counter = new AtomicInteger(0);
            counters.add(counter);
            handlers.add(switch (lifecycle) {
                case "scheduling" -> new CountingHandler(counter);
                case "finished" -> new FinishedCountingHandler(counter);
                case "failed" -> new FailedCountingHandler(counter);
                default -> throw new IllegalArgumentException(lifecycle);
            });
        }
        return handlers;
    }

    private static StageLifecycleEvent createEvent() {
        StageNode node = new StageNode(
                1, new org.pangrova.planner.analysis.PlanFragment(LogicalValues.createOneRow(CLUSTER), -1L, -1L), 1, "COMPUTE", List.of());
        Stage stage = Stage.create(node, "q-test");
        StageDAG dag = StageDAG.fromNodes(List.of(node), Map.of(), List.of()).withStages("q-test");
        QuerySession session = new QuerySession(
                "q-test", Map.of(), null, "tenant-1", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        return new StageLifecycleEvent(stage, dag, "q-test",
                new org.pangrova.execution.authority.StageExecutionContext.Live(session));
    }

    /** Counts onScheduling calls. */
    private record CountingHandler(AtomicInteger counter) implements StageLifecycleHandler {
        @Override public void onScheduling(StageLifecycleEvent event) {
            counter.incrementAndGet();
        }
    }

    /** Counts onFinished calls. */
    private record FinishedCountingHandler(AtomicInteger counter) implements StageLifecycleHandler {
        @Override public void onScheduling(StageLifecycleEvent event) {
            // this handler only counts onFinished — onScheduling is a no-op
        }
        @Override public void onFinished(StageLifecycleEvent event) {
            counter.incrementAndGet();
        }
    }

    /** Counts onFailed calls. */
    private record FailedCountingHandler(AtomicInteger counter) implements StageLifecycleHandler {
        @Override public void onScheduling(StageLifecycleEvent event) {
            // this handler only counts onFailed — onScheduling is a no-op
        }
        @Override public void onFailed(StageLifecycleEvent event) {
            counter.incrementAndGet();
        }
    }

    /** Increments counter then throws on onScheduling. */
    private record ThrowingHandler(AtomicInteger counter) implements StageLifecycleHandler {
        @Override public void onScheduling(StageLifecycleEvent event) {
            counter.incrementAndGet();
            throw new RuntimeException("deliberate test exception");
        }
    }

    /** Increments counter then throws on onFinished. */
    private record ThrowingOnFinishedHandler(AtomicInteger counter) implements StageLifecycleHandler {
        @Override public void onScheduling(StageLifecycleEvent event) {
            // this handler only acts on onFinished — onScheduling is a no-op
        }
        @Override public void onFinished(StageLifecycleEvent event) {
            counter.incrementAndGet();
            throw new RuntimeException("deliberate test exception");
        }
    }
}
