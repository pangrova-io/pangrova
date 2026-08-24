package org.pangrova.coordinator.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.index.IndexAwareSplitFetcher;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.query.cache.CacheWritebackConfig;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.task.catalog.CreateCatalogTask;
import org.pangrova.coordinator.query.execution.dispatch.task.catalog.CreateSchemaTask;
import org.pangrova.coordinator.query.execution.dispatch.task.catalog.DropCatalogTask;
import org.pangrova.coordinator.query.execution.dispatch.task.catalog.ShowCatalogsTask;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.bootstrap.module.DefaultModuleContext;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.speculation.SpeculativeStageHandler;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.parser.ast.SetSessionStatement;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;

/**
 * Creates execution-related components for the query subsystem: dispatch engine,
 * statement dispatcher, and write-complete callbacks. Extracted from QueryBootstrap
 * to keep bootstrap wiring under the 500-line quality gate.
 */
final class QueryExecutionWiring {

    private final CoordinatorBootstrapConfig config;
    private final TracingProvider tracingProvider;
    private final AccessControl accessControl;
    private final MetadataBootstrap metadataBootstrap;
    private final ModuleIntegrationWiring moduleWiring;
    private final StreamingBootstrap streamingBootstrap;
    private final HighAvailabilityCoordinationService coordinationService;
    private final org.pangrova.coordinator.query.task.NativeWriteCommitTracker writeCommitTracker;

    private RemoteTaskDispatcher remoteTaskDispatcher;

    QueryExecutionWiring(CoordinatorBootstrapConfig config, TracingProvider tracingProvider,
                         AccessControl accessControl, MetadataBootstrap metadataBootstrap,
                         ModuleIntegrationWiring moduleWiring, StreamingBootstrap streamingBootstrap,
                         HighAvailabilityCoordinationService coordinationService,
                         org.pangrova.coordinator.query.task.NativeWriteCommitTracker writeCommitTracker) {
        this.config = config;
        this.tracingProvider = tracingProvider;
        this.accessControl = accessControl;
        this.metadataBootstrap = metadataBootstrap;
        this.moduleWiring = moduleWiring;
        this.streamingBootstrap = streamingBootstrap;
        this.coordinationService = coordinationService;
        this.writeCommitTracker = writeCommitTracker;
    }

    QueryDispatcher buildQueryDispatcher(
            WorkerManager wm, CoordinatorTaskTracker tt, DefaultModuleContext mc,
            CircuitBreakerPolicy circuitBreaker, CoordinatorResultService resultService,
            SplitAssignmentManager splitAssignmentManager,
            SpeculativeStageHandler speculativeStageHandler,
            QueryStateJournal stateJournal) {
        SqlCompiler compiler = mc.require(SqlCompiler.class);
        CircuitBreakerPolicy cb = circuitBreaker;
        RemoteTaskDispatcher rtd = new RemoteTaskDispatcher(
                cb != null ? cb : CircuitBreakerPolicy.noOp(), tracingProvider,
                config.internalAuthSecret());
        this.remoteTaskDispatcher = rtd;
        CacheWritebackConfig wbConfig = config.cacheWritebackEnabled()
                ? new CacheWritebackConfig(true) : CacheWritebackConfig.DISABLED;
        ShuffleLifecycleManager shuffleLifecycleManager = buildShuffleLifecycleManager();
        var security = new org.pangrova.coordinator.query.execution.SecurityContextBundle(
                mc.optional(org.pangrova.coordinator.query.security.AccessControlEnforcer.class),
                mc.optional(org.pangrova.planner.security.ColumnMaskApplier.class),
                mc.optional(org.pangrova.planner.security.RowFilterApplier.class));
        var scheduling = new org.pangrova.coordinator.query.execution.SchedulingContextBundle(
                rtd, mc.optional(TaskScheduler.class), wm, tt,
                splitAssignmentManager, speculativeStageHandler,
                config.spillBudget(),
                null);
        var cache = new org.pangrova.coordinator.query.execution.CacheContextBundle(wbConfig);
        QueryDispatcherContext deps = new QueryDispatcherContext(compiler,
                security, scheduling, cache,
                mc.optional(CatalogRegistry.class),
                moduleWiring != null ? moduleWiring.getLineageEmitter() : null,
                resultService, config.effectiveTaskTimeoutSeconds(),
                mc.optional(org.pangrova.planner.runtimefilter.RuntimeFilterRegistry.class),
                new org.pangrova.planner.adaptive.AdaptiveQueryExecutor(),
                org.pangrova.planner.adaptive.AdaptiveQueryConfig.ENABLED,
                shuffleLifecycleManager, coordinationService,
                coordinationService != null ? config.coordinatorId() : null,
                stateJournal,
                mc.optional(IndexAwareSplitFetcher.class));
        // Late-binding reference: MERGE and INSERT SELECT dispatch functions need
        // QueryDispatcher, but QueryDispatcher needs StatementDispatcher. Resolve
        // via AtomicReference populated after QueryDispatcher construction.
        var queryDispatcherRef = new AtomicReference<QueryDispatcher>();
        var dispatcher = buildStatementDispatcher(
                compiler, tt, deps, queryDispatcherRef, resultService);
        if (dispatcher != null && metadataBootstrap != null) {
            dispatcher.setSystemTableQueryTask(
                    new org.pangrova.coordinator.query.execution.dispatch.task.SystemTableQueryTask(
                            metadataBootstrap.catalogRegistry()));
        }
        var qd = new QueryDispatcher(deps, dispatcher, tracingProvider);
        if (writeCommitTracker != null) {
            qd.setWriteCommitTracker(writeCommitTracker);
        }
        queryDispatcherRef.set(qd);
        return qd;
    }

    private org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher buildStatementDispatcher(
            SqlCompiler compiler, CoordinatorTaskTracker tt, QueryDispatcherContext deps,
            AtomicReference<QueryDispatcher> queryDispatcherRef,
            CoordinatorResultService resultService) {
        if (metadataBootstrap == null) {
            return null;
        }
        CatalogRegistry catalog = metadataBootstrap.catalogRegistry();
        IndexingBootstrap indexing = moduleWiring != null ? moduleWiring.getIndexingBootstrap() : null;
        Consumer<String> writeCallback = buildWriteCompleteCallback(
                moduleWiring != null ? moduleWiring.getMaintenanceScheduler() : null, indexing);

        // DML dispatch function: compiles statement SQL to RelNode, dispatches
        // through QueryDispatcher pipeline. Late-bound via queryDispatcherRef
        // because QueryDispatcher is constructed after StatementDispatcher.
        // Uses executeDmlStatement() to bypass StatementDispatchPhase and
        // convert AST → SqlNode → RelNode directly without SQL string round-trip.
        org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch =
                (sm, session, stmt) -> {
                    QueryDispatcher qd = queryDispatcherRef.get();
                    if (qd == null) {
                        throw new IllegalStateException(
                                "QueryDispatcher not initialized");
                    }
                    return qd.executeDmlStatement(sm, stmt, session);
                };

        var ddlExecutor = new org.pangrova.coordinator.query.statement.DdlExecutor(
                catalog, null,
                (Runnable) () -> {});
        // F-CHAIN-META-03: Unify the cache invalidation chain between SQL DdlExecutor
        // and REST CatalogRestOperations. Both paths flow through MetadataService's
        // invalidateTableCaches/invalidateCatalogCaches so the resolution cache, the
        // tiered MetadataCache, the Calcite LazyMetadataSchema (via localSchemaInvalidator),
        // and the cross-node invalidation log all stay in sync.
        // Cache invalidation is handled via MetadataService's built-in mechanisms
        var dmlExecutor = new org.pangrova.coordinator.query.statement.dml.DmlExecutor(
                catalog,
                new org.pangrova.coordinator.query.statement.dml.DmlExecutorConfig(
                        writeCallback, dmlDispatch,
                        new org.pangrova.coordinator.query.execution.CoordinatorMemoryGuard(),
                        org.pangrova.coordinator.query.statement.dml.DmlExecutorConfig.DEFAULT_MAX_CONCURRENT_DML,
                        org.pangrova.coordinator.query.statement.dml.DmlExecutorConfig.DEFAULT_DML_TIMEOUT_MS,
                        writeCommitTracker));
        var showDescribeExecutor = new org.pangrova.coordinator.query.statement.ShowDescribeExecutor(catalog);
        var maintenanceExecutor = new org.pangrova.coordinator.query.statement.MaintenanceExecutor(null);

        var securityApplier = new org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier(
                compiler, deps.accessControlEnforcer(),
                deps.columnMaskApplier(), deps.rowFilterApplier());

        // 2-Factory architecture: MetadataExecutionFactory (direct metadata ops)
        // + PlanExecutionFactory (compilation + DAG dispatch).
        var metadataFactory = buildMetadataFactory(
                ddlExecutor, dmlExecutor, showDescribeExecutor, maintenanceExecutor, catalog, dmlDispatch);
        if (indexing != null && indexing.getDdlExecutor() != null) {
            registerIndexTasks(metadataFactory, indexing.getDdlExecutor());
        }
        if (streamingBootstrap != null && streamingBootstrap.getDdlExecutor() != null) {
            registerStreamTasks(metadataFactory, streamingBootstrap.getDdlExecutor());
        }

        var planFactory = new org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactory(
                new org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryContext(
                        dmlDispatch, (java.util.function.Consumer<String>) null,
                        (sql, sess) -> securityApplier.compileAndApplyPolicies(sql, sess).toString(),
                        writeCommitTracker,
                        metadataBootstrap != null ? metadataBootstrap.catalogRegistry() : null,
                        deps.accessControlEnforcer()));

        var factories = new ArrayList<
                org.pangrova.coordinator.query.execution.dispatch.StatementExecutionFactory<?>>();
        factories.add(metadataFactory);
        factories.add(planFactory);

        var schemaListener = new org.pangrova.coordinator.query.execution.dispatch.listener.SchemaRefreshListener(
                () -> compiler, securityApplier::updateCompiler);
        var context = new org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext(
                compiler, resultService,
                List.copyOf(factories), List.of(schemaListener), accessControl);
        return new org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher(context);
    }

    /**
     * Builds a MetadataExecutionFactory with core DDL, DML (INSERT), SHOW/DESCRIBE,
     * and maintenance tasks. Index, clustering, stream, dynamic table, and tenant
     * tasks are registered dynamically via registerTask() after construction.
     */
    private static org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory buildMetadataFactory(
            org.pangrova.coordinator.query.statement.DdlExecutor ddlExecutor,
            org.pangrova.coordinator.query.statement.dml.DmlExecutor dmlExecutor,
            org.pangrova.coordinator.query.statement.ShowDescribeExecutor showDescribeExecutor,
            org.pangrova.coordinator.query.statement.MaintenanceExecutor maintenanceExecutor,
            CatalogRegistry catalogRegistry,
            org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch) {
        var tasks = new java.util.HashMap<Class<? extends org.pangrova.parser.ast.Statement>,
                MetadataTask<?>>();
        // DDL
        var createTableTask = new org.pangrova.coordinator.query.execution.dispatch.task.CreateTableTask(ddlExecutor);
        var dropTableTask = new org.pangrova.coordinator.query.execution.dispatch.task.DropTableTask(ddlExecutor);
        var alterTableTask = new org.pangrova.coordinator.query.execution.dispatch.task.AlterTableTask(ddlExecutor);
        var createSchemaTask = new CreateSchemaTask(ddlExecutor);
        var createCatalogTask = new CreateCatalogTask(ddlExecutor);
        var dropCatalogTask = new DropCatalogTask(ddlExecutor);
        tasks.put(org.pangrova.parser.ast.statement.CreateTableStatement.class, createTableTask);
        tasks.put(org.pangrova.parser.ast.statement.DropTableStatement.class, dropTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.AddColumn.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.DropColumn.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.RenameColumn.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.ChangeColumnType.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.SetPartitionSpec.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.CreateSchemaStatement.class, createSchemaTask);
        tasks.put(org.pangrova.parser.ast.CreateCatalogStatement.class, createCatalogTask);
        tasks.put(org.pangrova.parser.ast.statement.DropCatalogStatement.class, dropCatalogTask);
        // SET session property (no-op, returns empty result)
        tasks.put(SetSessionStatement.class,
                (MetadataTask<SetSessionStatement>)
                (stmt, ctx) -> new org.pangrova.coordinator.query.execution.dispatch.StatementResult(
                        ddlExecutor.executeSetSession(stmt), java.util.List.of(), null,
                        org.pangrova.coordinator.query.execution.dispatch.StatementCategory.DDL));
        // DML (INSERT VALUES, INSERT OVERWRITE only — DELETE/UPDATE/MERGE go to PlanExecutionFactory)
        tasks.put(org.pangrova.parser.ast.InsertStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.InsertValuesTask(dmlExecutor));
        tasks.put(org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.InsertOverwriteTask(dmlExecutor));
        // CTAS: create table from derived schema, then INSERT SELECT via native DML pipeline
        tasks.put(org.pangrova.parser.ast.statement.CreateTableAsSelectStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CtasTask(ddlExecutor, dmlDispatch));
        // SHOW / DESCRIBE
        tasks.put(org.pangrova.parser.ast.ShowSchemasStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.catalog.ShowSchemasTask(showDescribeExecutor));
        tasks.put(org.pangrova.parser.ast.statement.ShowTablesStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.ShowTablesTask(showDescribeExecutor));
        tasks.put(org.pangrova.parser.ast.statement.DescribeTableStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.DescribeTableTask(showDescribeExecutor));
        tasks.put(org.pangrova.parser.ast.statement.ShowCatalogsStatement.class,
                new ShowCatalogsTask(showDescribeExecutor));
        // Maintenance
        tasks.put(org.pangrova.parser.ast.statement.maintenance.CompactStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CompactTask(maintenanceExecutor));
        tasks.put(org.pangrova.parser.ast.statement.maintenance.VacuumStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.VacuumTask(maintenanceExecutor));
        tasks.put(org.pangrova.parser.ast.statement.maintenance.OptimizeStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.OptimizeTask(maintenanceExecutor));
        // CALL procedure
        tasks.put(org.pangrova.parser.ast.statement.maintenance.CallStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CallProcedureTask(catalogRegistry));
        // VIEW DDL
        tasks.put(org.pangrova.parser.ast.statement.CreateViewStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CreateViewTask(catalogRegistry));
        tasks.put(org.pangrova.parser.ast.statement.DropViewStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.DropViewTask(catalogRegistry));
        tasks.put(org.pangrova.parser.ast.statement.ShowViewsStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.ShowViewsTask(catalogRegistry));
        // Incremental read (CHANGES BETWEEN SNAPSHOT x AND y)
        tasks.put(org.pangrova.parser.ast.statement.IncrementalReadStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.IncrementalReadTask(catalogRegistry));
        return new org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory(tasks);
    }

    private static void registerIndexTasks(
            org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory factory,
            org.pangrova.coordinator.index.ddl.IndexDdlExecutor indexExecutor) {
        factory.registerTask(org.pangrova.parser.ast.statement.CreateIndexStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CreateIndexTask(indexExecutor));
        factory.registerTask(org.pangrova.parser.ast.statement.DropIndexStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.DropIndexTask(indexExecutor));
        factory.registerTask(org.pangrova.parser.ast.statement.ShowIndexesStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.ShowIndexesTask(indexExecutor));
        factory.registerTask(org.pangrova.parser.ast.statement.AlterIndexStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.AlterIndexTask(indexExecutor));
    }

    private static void registerStreamTasks(
            org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory factory,
            org.pangrova.coordinator.streaming.StreamDdlExecutor streamExecutor) {
        factory.registerTask(org.pangrova.parser.ast.statement.stream.CreateStreamStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.stream.CreateStreamTask(streamExecutor));
        factory.registerTask(org.pangrova.parser.ast.statement.stream.AlterStreamStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.stream.AlterStreamTask(streamExecutor));
        factory.registerTask(org.pangrova.parser.ast.statement.stream.DropStreamStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.stream.DropStreamTask(streamExecutor));
        factory.registerTask(org.pangrova.parser.ast.statement.stream.ShowStreamsStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.stream.ShowStreamsTask(streamExecutor));
        factory.registerTask(org.pangrova.parser.ast.statement.stream.DescribeStreamStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.stream.DescribeStreamTask(streamExecutor));
    }

    Consumer<String> buildWriteCompleteCallback(
            MaintenanceScheduler ms, IndexingBootstrap indexing) {
        Consumer<String> mCb = ms != null
                ? t -> ms.onWriteComplete(t, new org.pangrova.spi.connector.data.write.model.WriteResult(
                        t, 0, 0, 0, 0, java.time.Instant.now()))
                : null;
        Consumer<String> iCb = buildIndexAutoRebuildCallback(indexing);
        if (mCb == null && iCb == null) {
            return null;
        }
        final var fm = mCb;
        final var fi = iCb;
        return t -> {
            if (fm != null) {
                fm.accept(t);
            }
            if (fi != null) {
                fi.accept(t);
            }
        };
    }

    private Consumer<String> buildIndexAutoRebuildCallback(IndexingBootstrap indexing) {
        if (indexing == null || indexing.getBuildCoordinator() == null || indexing.getMetadataStore() == null) {
            return null;
        }
        return new org.pangrova.coordinator.index.lifecycle.IndexAutoRebuildTrigger(
                indexing.getMetadataStore(), indexing.getBuildCoordinator());
    }

    /**
     * Resolves the shuffle provider via auto-selection (G-HA-07) and returns its
     * lifecycle manager. When no explicit {@code shuffle.manager} is configured,
     * {@link org.pangrova.coordinator.query.execution.dispatch.shuffle.ShuffleProviderResolver#resolveWithDurableDefault}
     * selects the highest-durability provider available, making durable exchange the
     * default FTE mode when a durable provider is on the classpath.
     *
     * <p>Returns null if the resolved provider has no coordinator-side lifecycle
     * (e.g., native data exchange shuffle manages lifecycle in-process on workers).</p>
     */
    private ShuffleLifecycleManager buildShuffleLifecycleManager() {
        String shuffleManagerName = System.getProperty("pangrova.shuffle.manager");
        try {
            var provider = org.pangrova.coordinator.query.execution.dispatch.shuffle
                    .ShuffleProviderResolver.resolveWithDurableDefault(
                            shuffleManagerName, java.util.Map.of());
            return provider.lifecycleManager();
        } catch (Exception e) {
            org.slf4j.LoggerFactory.getLogger(QueryExecutionWiring.class)
                    .warn("event=shuffle_provider_resolution_failed error={}; falling back to null lifecycle",
                            e.getMessage());
            return null;
        }
    }

    RemoteTaskDispatcher remoteTaskDispatcher() { return remoteTaskDispatcher; }
}
