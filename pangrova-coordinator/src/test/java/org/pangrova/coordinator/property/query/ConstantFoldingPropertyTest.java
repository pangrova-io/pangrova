package org.pangrova.coordinator.property.query;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.result.CoordinatorResultWriter;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.planner.SqlCompiler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.calcite.rel.RelNode;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 7: Constant Folding path correctness.
 *
 * <p>For any constant expression query (e.g. SELECT 1, SELECT 1+2, SELECT 'abc'),
 * the Coordinator should bypass Worker dispatch and return the correct result locally.
 *
 * <p><b>Validates: Requirements 1.9</b>
 */
@Tag("Feature: full-pipeline-audit-fix, Property 7: Constant Folding path correctness")
class ConstantFoldingPropertyTest {

    private static final ResultEndpoint TRANSPORT_ENDPOINT =
            new ResultEndpoint("localhost", 19091, "unused", "grpc");

    private final SqlCompiler compiler = new SqlCompiler();

    private ScheduledExecutorService expirationExecutor;
    private BufferAllocator allocator;
    private CoordinatorResultBuffer resultBuffer;

    @BeforeProperty
    void setUp() {
        expirationExecutor = Executors.newSingleThreadScheduledExecutor();
        allocator = new RootAllocator(64 * 1024 * 1024);
        resultBuffer = new CoordinatorResultBuffer(expirationExecutor, 60_000L);
    }

    @AfterProperty
    void tearDown() {
        expirationExecutor.shutdownNow();
        allocator.close();
    }

    // ── Generators ──

    @Provide
    Arbitrary<String> constantIntSql() {
        return Arbitraries.integers().between(1, 9999)
                .map(n -> "SELECT " + n);
    }

    @Provide
    Arbitrary<String> arithmeticSql() {
        // Use positive integers to avoid edge cases where Calcite
        // Interpreter evaluation may fail (e.g. division by zero)
        Arbitrary<Integer> ints = Arbitraries.integers().between(1, 999);
        Arbitrary<String> ops = Arbitraries.of("+", "-", "*");
        return Combinators.combine(ints, ops, ints)
                .as((a, op, b) -> "SELECT " + a + " " + op + " " + b);
    }

    @Provide
    Arbitrary<String> stringLiteralSql() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20)
                .map(s -> "SELECT '" + s + "'");
    }

    @Provide
    Arbitrary<String> booleanSql() {
        return Arbitraries.of("SELECT TRUE", "SELECT FALSE");
    }

    @Provide
    Arbitrary<String> multiColumnConstantSql() {
        Arbitrary<Integer> ints = Arbitraries.integers().between(1, 100);
        Arbitrary<String> strs = Arbitraries.strings().alpha()
                .ofMinLength(1).ofMaxLength(10);
        return Combinators.combine(ints, strs)
                .as((n, s) -> "SELECT " + n + ", '" + s + "', TRUE");
    }

    @Provide
    Arbitrary<String> allConstantSql() {
        return Arbitraries.oneOf(
                constantIntSql(),
                arithmeticSql(),
                stringLiteralSql(),
                booleanSql(),
                multiColumnConstantSql()
        );
    }

    /**
     * Filters generated SQL to only those that Calcite successfully folds.
     * Some arithmetic expressions may not fold due to Calcite Interpreter
     * limitations; we test the property only on foldable expressions.
     */
    @Provide
    Arbitrary<String> foldableConstantSql() {
        return allConstantSql().filter(sql -> {
            try {
                RelNode relNode = compiler.compileToRelNode(sql);
                return PangrovaPlanner.isFoldedConstant(relNode);
            } catch (Exception e) {
                return false;
            }
        });
    }

    // ── Property 7: Constant Folding path correctness ──

    /**
     * Property 7a: Planner detection — simple constant literals are recognized
     * as foldable by Calcite (LogicalValues with no inputs).
     */
    @Property(tries = 10)
    void simpleLiteralsDetectedAsFoldedConstant(
            @ForAll("constantIntSql") String sql) {
        RelNode relNode = compiler.compileToRelNode(sql);
        assertThat(PangrovaPlanner.isFoldedConstant(relNode))
                .as("Simple literal SQL '%s' should be detected as folded constant", sql)
                .isTrue();
    }

    /**
     * Property 7b: Local dispatch — foldable constant queries bypass Worker
     * and finish locally with state FINISHED.
     */
    @Property(tries = 10)
    void constantQueryBypassesWorkerAndFinishesLocally(
            @ForAll("foldableConstantSql") String sql) {
        QueryDispatcher dispatcher = buildDispatcherWithResultService();
        QueryStateMachine sm = new QueryStateMachine();
        String queryId = "cf-bypass-" + System.nanoTime();
        CoordinatorSessionContext session = new CoordinatorSessionContext(queryId, Map.of(), null, null);

        DispatchResult result = dispatcher.execute(sm, sql, session);

        assertThat(result).isNotNull();
        assertThat(result.workerHost())
                .as("Constant query should be dispatched locally, not to a worker")
                .isEqualTo("coordinator-local");
        assertThat(sm.getState())
                .as("Constant query should reach FINISHED without Worker involvement")
                .isEqualTo(QueryStateMachine.State.FINISHED);
    }

    /**
     * Property 7c: Result buffering — foldable constant query results are
     * buffered as non-empty Arrow IPC bytes.
     */
    @Property(tries = 10)
    void constantQueryResultIsBuffered(
            @ForAll("foldableConstantSql") String sql) {
        QueryDispatcher dispatcher = buildDispatcherWithResultService();
        String queryId = "cf-buf-" + System.nanoTime();
        CoordinatorSessionContext session = new CoordinatorSessionContext(queryId, Map.of(), null, null);

        dispatcher.execute(new QueryStateMachine(), sql, session);

        byte[] resultBytes = resultBuffer.get(queryId);
        assertThat(resultBytes)
                .as("Arrow IPC bytes should be buffered for query '%s'", sql)
                .isNotNull()
                .isNotEmpty();
    }

    /**
     * Property 7d: Arrow IPC validity — buffered bytes are readable Arrow IPC
     * with exactly 1 row and at least 1 column.
     */
    @Property(tries = 10)
    void bufferedResultBytesAreReadable(
            @ForAll("foldableConstantSql") String sql) throws Exception {
        QueryDispatcher dispatcher = buildDispatcherWithResultService();
        String queryId = "cf-ipc-" + System.nanoTime();
        CoordinatorSessionContext session = new CoordinatorSessionContext(queryId, Map.of(), null, null);

        dispatcher.execute(new QueryStateMachine(), sql, session);

        byte[] resultBytes = resultBuffer.get(queryId);
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(resultBytes), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(reader.loadNextBatch()).isTrue();
            assertThat(root.getRowCount())
                    .as("Constant query should produce exactly 1 row")
                    .isEqualTo(1);
            assertThat(root.getFieldVectors())
                    .as("Result should have at least one column")
                    .isNotEmpty();
        }
    }

    /**
     * Property 7e: Result endpoint — ticket matches queryId for transport retrieval.
     */
    @Property(tries = 10)
    void resultEndpointTicketMatchesQueryId(
            @ForAll("foldableConstantSql") String sql) {
        QueryDispatcher dispatcher = buildDispatcherWithResultService();
        String queryId = "cf-ticket-" + System.nanoTime();
        CoordinatorSessionContext session = new CoordinatorSessionContext(queryId, Map.of(), null, null);

        DispatchResult result = dispatcher.execute(new QueryStateMachine(), sql, session);

        assertThat(result.resultEndpoint().ticket()).isEqualTo(queryId);
        assertThat(result.resultEndpoint().host()).isEqualTo("localhost");
        assertThat(result.resultEndpoint().protocol()).isEqualTo("grpc");
    }

    // ── Helpers ──

    private QueryDispatcher buildDispatcherWithResultService() {
        CoordinatorResultWriter writer = new CoordinatorResultWriter(allocator);
        CoordinatorResultService resultService =
                new CoordinatorResultService(resultBuffer, writer, TRANSPORT_ENDPOINT);

        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(dummyWorker());
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());

        QueryDispatcherContext deps = QueryDispatcherContext.withDefaults(
                compiler, null, null, null,
                new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider()),
                scheduler, wm, new CoordinatorTaskTracker(),
                null, null, null, resultService, 0L);
        return new QueryDispatcher(deps, null, null);
    }

    private static WorkerInfo dummyWorker() {
        return TestWorkerInfoBuilder.active("dummy-w", "localhost", 1, 0);
    }
}
