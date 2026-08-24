package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
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
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.RbacAccessControl;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.testing.TestObservability;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that cache hit cannot bypass security policy enforcement.
 * After the fix, {@code compileAndApplyPolicies()} executes BEFORE cache lookup,
 * so a user without permission is denied even when the query result is cached.
 *
 * <p>Validates: Requirements 1.9, 1.10, 2.9, 2.10</p>
 */
class CacheSecurityBypassTest {

    /** SQL referencing a real table so access control actually fires. */
    private static final String TABLE_SQL = "SELECT id, region FROM orders";
    private static final byte[] CACHED_IPC_BYTES = {10, 20, 30};
    private static final ResultEndpoint TRANSPORT_ENDPOINT =
            new ResultEndpoint("localhost", 9091, "unused", "grpc");

    private ScheduledExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    /**
     * Permitted user (admin group with SELECT grant) should still get cached results.
     * Preservation: security passes → cache hit → valid result returned.
     */
    @Test
    void permittedUser_cacheHit_returnsResult() {
        List<PrivilegeGrant> grants = List.of(
                PrivilegeGrant.of("admins", Privilege.SELECT, "TABLE", "*", "*", "*", null));
        QueryDispatcher dispatcher = buildDispatcher(grants);

        UserIdentity admin = new UserIdentity("admin-user", Set.of("admins"), Map.of(), Instant.now());
        CoordinatorSessionContext session = new CoordinatorSessionContext("perm-q1", Map.of(), admin, "tenant-a");
        QueryStateMachine sm = new QueryStateMachine();

        DispatchResult result = dispatcher.execute(sm, TABLE_SQL, session);

        assertNotNull(result, "permitted user should get cached result");
        assertNotNull(result.resultEndpoint(), "result endpoint must be valid");
        assertEquals(QueryStateMachine.State.FINISHED, sm.getState());
    }

    /**
     * Unpermitted user (no grants) submitting the same cached SQL must be denied.
     * Before the fix, cache lookup happened before security checks, so this would succeed.
     * After the fix, compileAndApplyPolicies() runs first and throws AccessDeniedException.
     */
    @Test
    void unpermittedUser_cannotRetrieveCachedResults() {
        List<PrivilegeGrant> grants = List.of(
                PrivilegeGrant.of("admins", Privilege.SELECT, "TABLE", "*", "*", "*", null));
        QueryDispatcher dispatcher = buildDispatcher(grants);

        UserIdentity unpermitted = new UserIdentity("reader-user", Set.of("readers"), Map.of(), Instant.now());
        CoordinatorSessionContext session = new CoordinatorSessionContext("deny-q1", Map.of(), unpermitted, "tenant-a");
        QueryStateMachine sm = new QueryStateMachine();

        assertThrows(AccessDeniedException.class,
                () -> dispatcher.execute(sm, TABLE_SQL, session),
                "unpermitted user must be denied even when result is cached");
    }

    /**
     * Anonymous user (null identity) submitting a cached SQL must be denied.
     */
    @Test
    void anonymousUser_cannotRetrieveCachedResults() {
        List<PrivilegeGrant> grants = List.of(
                PrivilegeGrant.of("admins", Privilege.SELECT, "TABLE", "*", "*", "*", null));
        QueryDispatcher dispatcher = buildDispatcher(grants);

        CoordinatorSessionContext session = new CoordinatorSessionContext("anon-q1", Map.of(), null, "tenant-a");
        QueryStateMachine sm = new QueryStateMachine();

        assertThrows(AccessDeniedException.class,
                () -> dispatcher.execute(sm, TABLE_SQL, session),
                "anonymous user must be denied even when result is cached");
    }

    private QueryDispatcher buildDispatcher(List<PrivilegeGrant> grants) {
        // Schema with an "orders" table
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add("orders", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("id", SqlTypeName.BIGINT)
                        .add("region", SqlTypeName.VARCHAR, 50)
                        .add("amount", SqlTypeName.DECIMAL, 10, 2)
                        .build();
            }
        });

        // Cache with pre-populated entry for the permitted user's query context
        QueryResultCache cache = new QueryResultCache(new InMemoryL2CacheStore());
        CacheKey permittedKey = CacheKey.of(TABLE_SQL, "tenant-a", "admin-user", Map.of(), null);
        cache.put(permittedKey, new CachedResult(CACHED_IPC_BYTES, Instant.now()),
                new CacheEntryMetadata(Set.of("orders"), "admin-user", ""));

        // Planner with schema + cache
        PlannerContext plannerDeps = new PlannerContext(null, null, cache, null, null, null, null);
        PangrovaPlanner planner = new PangrovaPlanner(rootSchema, List.of(""), plannerDeps);
        SqlCompiler compiler = new SqlCompiler(planner, new SubstraitConverter(), new SubstraitPlanSerializer());

        // Real RBAC access control
        RbacAccessControl accessControl = new RbacAccessControl(
                grants,
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
        AccessControlEnforcer enforcer = new AccessControlEnforcer(accessControl);

        // Result service
        CoordinatorResultBuffer buffer = new CoordinatorResultBuffer(executor, 60_000L);
        CoordinatorResultWriter writer = new CoordinatorResultWriter(new RootAllocator());
        CoordinatorResultService resultService = new CoordinatorResultService(buffer, writer, TRANSPORT_ENDPOINT);

        // Build dispatcher
        RemoteTaskDispatcher httpDispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        TaskScheduler taskScheduler = new TaskScheduler(new WorkerPerformanceScorer());
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());

        var deps = QueryDispatcherContext.withDefaults(
                compiler, enforcer, null, null,
                httpDispatcher, taskScheduler, workerManager,
                new CoordinatorTaskTracker(), null, null, null,
                resultService, 0L);
        return new QueryDispatcher(deps, null, null);
    }
}
