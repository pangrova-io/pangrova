package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.result.CoordinatorResultWriter;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.PlannerContext;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.cache.CacheKey;
import org.pangrova.planner.cache.CacheEntryMetadata;
import org.pangrova.planner.cache.CachedResult;
import org.pangrova.planner.cache.InMemoryL2CacheStore;
import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.testing.TestObservability;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.jdbc.CalciteSchema;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration test verifying that cache hit returns a response with valid
 * {@code resultEndpoint} that clients can use to fetch results.
 *
 * <p>Validates: Requirements 1.7, 1.8, 2.7, 2.8</p>
 */
class CacheHitResultEndpointTest {

    private static final String CACHED_SQL = "SELECT 1";
    private static final byte[] CACHED_IPC_BYTES = {1, 2, 3, 4, 5};
    private static final ResultEndpoint TRANSPORT_ENDPOINT =
            new ResultEndpoint("localhost", 9091, "unused", "grpc");

    private ScheduledExecutorService executor;
    private QueryDispatcher dispatcher;
    private QueryResultCache queryResultCache;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();

        // Set up query result cache with a pre-populated entry
        // Cache key must match what QueryDispatcher constructs: normalized SQL + tenant + principal + props + policy
        queryResultCache = new QueryResultCache(new InMemoryL2CacheStore());
        CacheKey cachedKey = CacheKey.of(CACHED_SQL, null, "anonymous", Map.of(), null);
        queryResultCache.put(cachedKey, new CachedResult(CACHED_IPC_BYTES, Instant.now()),
                new CacheEntryMetadata(Set.of(), "anonymous", ""));

        // Set up planner with the cache wired in
        PlannerContext plannerDeps = new PlannerContext(null, null, queryResultCache, null, null, null, null);
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        PangrovaPlanner planner = new PangrovaPlanner(rootSchema, java.util.List.of(""), plannerDeps);
        SqlCompiler compiler = new SqlCompiler(planner, new SubstraitConverter(), new SubstraitPlanSerializer());

        // Set up coordinator result service
        CoordinatorResultBuffer buffer = new CoordinatorResultBuffer(executor, 60_000L);
        CoordinatorResultWriter writer = new CoordinatorResultWriter(new RootAllocator());
        CoordinatorResultService resultService = new CoordinatorResultService(buffer, writer, TRANSPORT_ENDPOINT);

        // Build dispatcher with cache and result service
        RemoteTaskDispatcher httpDispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        TaskScheduler taskScheduler = new TaskScheduler(new WorkerPerformanceScorer());
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());

        var deps = QueryDispatcherContext.withDefaults(
                compiler, null, null, null,
                httpDispatcher, taskScheduler, workerManager,
                new CoordinatorTaskTracker(), null, null, null,
                resultService, 0L);
        dispatcher = new QueryDispatcher(deps, null, null);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void cacheHit_returnsDispatchResultWithValidEndpoint() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext("cache-q1", Map.of(), null, null);

        DispatchResult result = dispatcher.execute(sm, CACHED_SQL, session);

        assertNotNull(result, "cache hit must return non-null DispatchResult");
        assertNotNull(result.resultEndpoint(), "cache hit must return non-null resultEndpoint");
        assertEquals("localhost", result.resultEndpoint().host());
        assertEquals(9091, result.resultEndpoint().port());
        assertEquals("cache-q1", result.resultEndpoint().ticket(),
                "resultEndpoint ticket should be the queryId");
        assertEquals("grpc", result.resultEndpoint().protocol());
    }

    @Test
    void cacheHit_transitionsStateMachineToFinished() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext("cache-q2", Map.of(), null, null);

        dispatcher.execute(sm, CACHED_SQL, session);

        assertEquals(QueryStateMachine.State.FINISHED, sm.getState(),
                "cache hit should transition state machine to FINISHED");
    }

    @Test
    void cacheHit_resultEndpointUsableByClient() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext("cache-q3", Map.of(), null, null);

        DispatchResult result = dispatcher.execute(sm, CACHED_SQL, session);

        // Verify the endpoint contains all fields needed for a client to fetch results
        ResultEndpoint endpoint = result.resultEndpoint();
        assertNotNull(endpoint.host(), "endpoint host must not be null");
        assertNotNull(endpoint.ticket(), "endpoint ticket must not be null");
        assertNotNull(endpoint.protocol(), "endpoint protocol must not be null");
        assertEquals("cache-q3", endpoint.ticket(),
                "ticket should match queryId so client can fetch the correct result");
    }

    @Test
    void cacheMiss_fallsThroughToNormalPath() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext("miss-q1", Map.of(), null, null);

        // "SELECT 1" is cached, but "SELECT 2" is not — should fall through to compile path.
        // With no workers registered, constant-fold path returns null (no resultService dispatch
        // for constants when workers are empty and resultService is present).
        DispatchResult result = dispatcher.execute(sm, "SELECT 2", session);

        // Constant query with resultService present dispatches locally
        assertNotNull(result, "constant query with resultService should dispatch locally");
        assertEquals(QueryStateMachine.State.FINISHED, sm.getState());
    }
}
