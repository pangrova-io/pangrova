package org.pangrova.coordinator.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.pangrova.config.ShuffleMode;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.index.IndexAwareSplitFetcher;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.CopySourceTaskBinder;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.CoordinatorShuffleLifecycleManager;
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
import org.pangrova.shuffle.ShuffleServiceAddress;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates execution-related components for the query subsystem: dispatch engine,
 * statement dispatcher, and write-complete callbacks. Extracted from QueryBootstrap
 * to keep bootstrap wiring under the 500-line quality gate.
 */
final class QueryExecutionBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutionBootstrap.class);

    private final CoordinatorBootstrapConfig config;
    private final TracingProvider tracingProvider;
    private final AccessControl accessControl;
    private final MetadataBootstrap metadataBootstrap;
    private final ModuleIntegrationBootstrap moduleWiring;
    private final StreamingBootstrap streamingBootstrap;
    private final HighAvailabilityCoordinationService coordinationService;
    private final org.pangrova.shuffle.ShuffleExecutionOwnerResolver shuffleOwnerResolver;
    private final javax.sql.DataSource metadataDataSource;
    private final org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker writeCommitTracker;
    private final org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator
            generatedValueMaterialization;
    private final org.pangrova.coordinator.query.execution.GeneratedWriteAuthoritySupplier writeAuthoritySupplier;
    private final org.pangrova.metadata.semantic.SemanticViewStore semanticViewStore;
    private final java.util.function.Function<org.pangrova.coordinator.feature.FeatureSourceCompiler,
            org.pangrova.coordinator.feature.FeatureViewStatementService> featureViewServiceFactory;
    private final org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry
            embeddingModelRegistry;
    private final org.pangrova.security.audit.AuditEventEmitter auditEmitter;

    private RemoteTaskDispatcher remoteTaskDispatcher;
    private WorkerManager workerManager;
    private org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory metadataFactory;
    private SqlCompiler compiler;
    private CoordinatorResultService resultService;
    private org.pangrova.coordinator.materialization.MaterializationKernelFactory
            materializationKernels;

    // This internal composition root binds the exact optional execution owners.
    @SuppressWarnings("checkstyle:ParameterNumber")
    QueryExecutionBootstrap(CoordinatorBootstrapConfig config, TracingProvider tracingProvider,
                         AccessControl accessControl, MetadataBootstrap metadataBootstrap,
                         ModuleIntegrationBootstrap moduleWiring, StreamingBootstrap streamingBootstrap,
                         HighAvailabilityCoordinationService coordinationService,
                         org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker writeCommitTracker,
                         org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator
                                 generatedValueMaterialization,
                         org.pangrova.metadata.semantic.SemanticViewStore semanticViewStore,
                         java.util.function.Function<org.pangrova.coordinator.feature.FeatureSourceCompiler,
                                 org.pangrova.coordinator.feature.FeatureViewStatementService>
                                 featureViewServiceFactory,
                         org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry
                                 embeddingModelRegistry,
                         org.pangrova.shuffle.ShuffleExecutionOwnerResolver shuffleOwnerResolver,
                         javax.sql.DataSource metadataDataSource,
                         org.pangrova.security.audit.AuditEventEmitter auditEmitter) {
        this.config = config;
        this.tracingProvider = tracingProvider;
        this.accessControl = accessControl;
        this.metadataBootstrap = metadataBootstrap;
        this.moduleWiring = moduleWiring;
        this.streamingBootstrap = streamingBootstrap;
        this.coordinationService = coordinationService;
        this.shuffleOwnerResolver = shuffleOwnerResolver;
        this.metadataDataSource = metadataDataSource;
        this.writeCommitTracker = writeCommitTracker;
        this.generatedValueMaterialization = generatedValueMaterialization;
        var catalogs = metadataBootstrap == null ? null : metadataBootstrap.catalogRegistry();
        this.writeAuthoritySupplier = catalogs == null || metadataDataSource == null ? null
                : org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator
                        .writeAuthoritySupplier(catalogs, metadataDataSource, generatedValueMaterialization);
        this.semanticViewStore = semanticViewStore;
        this.featureViewServiceFactory = featureViewServiceFactory;
        this.embeddingModelRegistry = embeddingModelRegistry;
        this.auditEmitter = auditEmitter;
    }

    QueryDispatcher buildQueryDispatcher(
            WorkerManager wm, CoordinatorTaskTracker tt, DefaultModuleContext mc,
            CircuitBreakerPolicy circuitBreaker, CoordinatorResultService resultSvc,
            SplitAssignmentManager splitAssignmentManager,
            SpeculativeStageHandler speculativeStageHandler,
            QueryStateJournal stateJournal) {
        this.workerManager = java.util.Objects.requireNonNull(wm, "workerManager must not be null");
        this.resultService = java.util.Objects.requireNonNull(resultSvc, "resultService must not be null");
        SqlCompiler scopedCompiler = mc.require(SqlCompiler.class).withNativeCapabilities(() -> {
            var inventories = wm.getActiveWorkers().stream().map(
                    org.pangrova.coordinator.worker.WorkerInfo::capabilityInventory).toList();
            if (inventories.isEmpty()) {
                throw new org.pangrova.coordinator.scheduler.failure.NoAvailableWorkerException();
            }
            return org.pangrova.planner.validation.NativeCapabilityRegistry.fromInventories(inventories);
        });
        mc.bind(SqlCompiler.class, scopedCompiler);
        this.compiler = scopedCompiler;
        CircuitBreakerPolicy cb = circuitBreaker;
        RemoteTaskDispatcher rtd = new RemoteTaskDispatcher(
                cb != null ? cb : CircuitBreakerPolicy.noOp(), tracingProvider,
                config.internalAuthSecret());
        rtd.setRecoveryWorkers(wm::getActiveWorkers);
        if (config.shuffleTlsConfig() != null) {
            rtd.configureCopyTaskTls(config.shuffleTlsConfig());
        }
        if (coordinationService != null) {
            rtd.setControlEpochSupplier(coordinationService::getCurrentEpoch);
            rtd.setExecutionOwnerResolver(executionId -> {
                var anchor = coordinationService.requireExecutionAnchor(executionId);
                return new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                        anchor.ownerId(), anchor.ownerIncarnation(),
                        anchor.fencingToken());
            });
        }
        if (metadataDataSource != null && accessControl != null && coordinationService != null) {
            rtd.setCopySourceBinder(new CopySourceTaskBinder(
                    new org.pangrova.metadata.storage.JdbcCopySourceReadAdmissions(metadataDataSource, accessControl),
                    coordinationService::requireExecutionAnchor));
        }
        this.remoteTaskDispatcher = rtd;
        ShuffleLifecycleManager shuffleLifecycleManager = buildShuffleLifecycleManager();
        var security = new org.pangrova.coordinator.query.execution.SecurityContextBundle(
                mc.optional(org.pangrova.coordinator.query.security.AccessControlEnforcer.class),
                mc.optional(org.pangrova.planner.security.ColumnMaskApplier.class),
                mc.optional(org.pangrova.planner.security.RowFilterApplier.class));
        var scheduling = new org.pangrova.coordinator.query.execution.SchedulingContextBundle(
                rtd, mc.optional(TaskScheduler.class), wm, tt,
                splitAssignmentManager, speculativeStageHandler,
                config.spillBudget(),
                moduleWiring != null ? moduleWiring.getResilienceRegistry() : null);
        QueryDispatcherContext deps = new QueryDispatcherContext(scopedCompiler,
                security, scheduling,
                mc.optional(CatalogRegistry.class),
                moduleWiring != null ? moduleWiring.getLineageEmitter() : null,
                config.effectiveTaskTimeoutSeconds(),
                mc.optional(org.pangrova.planner.runtimefilter.RuntimeFilterRegistry.class),
                new org.pangrova.planner.adaptive.AdaptiveQueryExecutor(),
                org.pangrova.planner.adaptive.AdaptiveQueryConfig.ENABLED,
                shuffleLifecycleManager, coordinationService,
                coordinationService != null ? config.coordinatorId() : null,
                stateJournal,
                mc.optional(IndexAwareSplitFetcher.class),
                embeddingModelRegistry);
        // Late-binding reference: MERGE and INSERT SELECT dispatch functions need
        // QueryDispatcher, but QueryDispatcher needs StatementDispatcher. Resolve
        // via AtomicReference populated after QueryDispatcher construction.
        var queryDispatcherRef = new AtomicReference<QueryDispatcher>();
        if (generatedValueMaterialization != null) {
            generatedValueMaterialization.bindSourceCompiler((sql, actor) -> {
                QueryDispatcher dispatcher = queryDispatcherRef.get();
                if (dispatcher == null) {
                    throw new IllegalStateException("QueryDispatcher not initialized for generated input compilation");
                }
                return dispatcher.compileAuthorizedRelation(sql, actor);
            });
        }
        var dispatcher = buildStatementDispatcher(
                scopedCompiler, tt, deps, queryDispatcherRef, this.resultService);
        var qd = new QueryDispatcher(deps, dispatcher, tracingProvider);
        if (writeAuthoritySupplier != null) {
            qd.setGeneratedWriteAuthoritySupplier(writeAuthoritySupplier);
        }
        if (writeCommitTracker != null) {
            qd.setWriteCommitTracker(writeCommitTracker);
        }
        queryDispatcherRef.set(qd);
        return qd;
    }

    private org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher buildStatementDispatcher(
            SqlCompiler compilerSnapshot, CoordinatorTaskTracker tt, QueryDispatcherContext deps,
            AtomicReference<QueryDispatcher> queryDispatcherRef,
            CoordinatorResultService resultSvc) {
        if (metadataBootstrap == null) {
            return null;
        }
        CatalogRegistry catalog = metadataBootstrap.catalogRegistry();
        IndexingBootstrap indexing = moduleWiring != null ? moduleWiring.getIndexingBootstrap() : null;
        Consumer<String> writeCallback = buildWriteCompleteCallback(
                moduleWiring != null ? moduleWiring.getMaintenanceScheduler() : null);

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
                org.pangrova.coordinator.metadata.read.MetadataSchemaBuilder::invalidateTableCaches);
        ddlExecutor.setMetadataService(metadataBootstrap.metadataService());
        if (metadataDataSource != null
                && (config.securityEnabled() || config.governanceEnabled())) {
            ddlExecutor.setColumnMaskStore(
                    new org.pangrova.security.store.PersistentColumnMaskStore(metadataDataSource));
        }
        var showDescribeExecutor = new org.pangrova.coordinator.query.statement.ShowDescribeExecutor(catalog);
        var maintenancePolicyService =
                new org.pangrova.metadata.control.TableControlMaintenanceService(
                        metadataDataSource != null
                                ? new org.pangrova.metadata.control
                                        .JdbcTableControlMaintenanceStore(metadataDataSource)
                                : new org.pangrova.metadata.control
                                        .InMemoryTableControlMaintenanceStore());
        var controlledMaintenance =
                new org.pangrova.coordinator.query.statement.ControlledTableMaintenance(
                        catalog, maintenancePolicyService,
                        moduleWiring != null ? moduleWiring.getDistributedLease() : null,
                        config.coordinatorId());
        var maintenanceExecutor = new org.pangrova.coordinator.query.statement.MaintenanceExecutor(
                controlledMaintenance);
        ddlExecutor.setMaintenancePolicyService(maintenancePolicyService);
        ddlExecutor.setPolicyBinder(new org.pangrova.metadata.control.TableControlPolicyBinder(
                catalog.tableControl(),
                metadataDataSource != null
                        ? new org.pangrova.metadata.control
                                .JdbcTableControlGovernanceBindingStore(metadataDataSource)
                        : new org.pangrova.metadata.control
                                .InMemoryTableControlGovernanceBindingStore()));

        if (auditEmitter != null) {
            var tableControlAudit =
                    new org.pangrova.coordinator.query.audit.TableControlAuditEmitter(auditEmitter);
            ddlExecutor.setTableControlAudit(tableControlAudit);
            controlledMaintenance.setTableControlAudit(tableControlAudit);
            metadataBootstrap.addMetadataListener(tableControlAudit);
        }

        var securityApplier = new org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier(
                compilerSnapshot, deps.accessControlEnforcer(),
                deps.columnMaskApplier(), deps.rowFilterApplier());

        // 2-Factory architecture: MetadataExecutionFactory (direct metadata ops)
        // + PlanExecutionFactory (compilation + DAG dispatch).
        this.metadataFactory = buildMetadataFactory(
                ddlExecutor, showDescribeExecutor, maintenanceExecutor, catalog, dmlDispatch);
        registerOptionalMetadataTasks(indexing, dmlDispatch, ddlExecutor, queryDispatcherRef);
        registerBranchTasks(metadataFactory, catalog);
        registerStorageProfileTasks(metadataFactory, accessControl, metadataDataSource);
        registerFileSpaceTasks(metadataFactory, accessControl, metadataDataSource, auditEmitter);

        var planFactory = new org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactory(
                new org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryContext(
                        dmlDispatch, writeCallback,
                        (sql, sess) -> {
                            boolean timeTravel = org.pangrova.parser.TimeTravelSqlClauses
                                    .extract(sql).references().stream()
                                    .anyMatch(ref -> ref.table().timeTravelSpec().isPresent());
                            var explained = securityApplier.compileForExplain(sql, sess, timeTravel);
                            return explained.plan().planExplanation() + System.lineSeparator()
                                    + explained.rewrite().describe();
                        },
                        catalog,
                        generatedValueMaterialization));

        var factories = new ArrayList<
                org.pangrova.coordinator.query.execution.dispatch.StatementExecutionFactory<?>>();
        factories.add(metadataFactory);
        factories.add(planFactory);

        var schemaListener = new org.pangrova.coordinator.query.execution.dispatch.listener.SchemaRefreshListener(
                () -> compilerSnapshot, securityApplier::updateCompiler);

        // DDL leader routing guard: only instantiated when explicitly enabled via config.
        // Default is OFF — DDL executes on any coordinator, relying on connector-level
        // concurrency control and cross-node invalidation propagation for consistency.
        org.pangrova.coordinator.query.execution.dispatch.DdlLeadershipGuard ddlGuard = null;
        if (config.ddlLeaderRoutingEnabled() && coordinationService != null) {
            ddlGuard = new org.pangrova.coordinator.query.execution.dispatch.DdlLeadershipGuard(
                    coordinationService, config.coordinatorId());
            LOG.info("event=ddl_leader_routing_enabled coordinatorId={}", config.coordinatorId());
        }

        var context = new org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext(
                compilerSnapshot, resultSvc,
                List.copyOf(factories), List.of(schemaListener), accessControl, ddlGuard);
        return new org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher(context);
    }

    /** Registers metadata tasks whose wiring depends on optional modules. */
    private void registerOptionalMetadataTasks(IndexingBootstrap indexing,
            org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch,
            org.pangrova.coordinator.query.statement.DdlExecutor ddlExecutor,
            AtomicReference<QueryDispatcher> queryDispatcherRef) {
        if (semanticViewStore != null) {
            registerSemanticViewTasks(metadataFactory, semanticViewStore);
        }
        if (featureViewServiceFactory != null) {
            var featureViewService = featureViewServiceFactory.apply((sql, actor) -> {
                QueryDispatcher dispatcher = queryDispatcherRef.get();
                if (dispatcher == null) {
                    throw new IllegalStateException(
                            "QueryDispatcher not initialized for authenticated Feature compilation");
                }
                return dispatcher.compileAuthorizedRelation(sql, actor);
            });
            org.pangrova.coordinator.feature.FeatureViewMetadataTasks.register(
                    metadataFactory, featureViewService);
        }
        if (indexing != null && indexing.getDdlExecutor() != null) {
            registerIndexTasks(metadataFactory, indexing.getDdlExecutor());
        }
        if (streamingBootstrap != null && streamingBootstrap.getDdlExecutor() != null) {
            registerStreamTasks(metadataFactory, streamingBootstrap.getDdlExecutor());
        }
        if (metadataBootstrap.materializationRepository() != null
                && coordinationService != null && metadataDataSource != null) {
            registerDynamicTableTasks(metadataFactory, ddlExecutor, dmlDispatch);
        }
    }

    /**
     * Builds a MetadataExecutionFactory with control-plane DDL, SHOW/DESCRIBE,
     * and maintenance tasks. Index, clustering, stream, dynamic table, and tenant
     * tasks are registered dynamically via registerTask() after construction.
     */
    private static org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory buildMetadataFactory(
            org.pangrova.coordinator.query.statement.DdlExecutor ddlExecutor,
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
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.RenameTable.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.SetPartitionSpec.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.CreateBranch.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.DropBranch.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.CreateTag.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.DropTag.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.SetColumnTag.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.UnsetColumnTag.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.Adopt.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.AlterTableStatement.Unlink.class, alterTableTask);
        tasks.put(org.pangrova.parser.ast.statement.CreateSchemaStatement.class, createSchemaTask);
        tasks.put(org.pangrova.parser.ast.CreateCatalogStatement.class, createCatalogTask);
        tasks.put(org.pangrova.parser.ast.statement.DropCatalogStatement.class, dropCatalogTask);
        // SET session property (no-op, returns empty result)
        tasks.put(SetSessionStatement.class,
                (MetadataTask<SetSessionStatement>)
                (stmt, ctx) -> new org.pangrova.coordinator.query.execution.dispatch.StatementResult(
                        ddlExecutor.executeSetSession(stmt), java.util.List.of(), null,
                        org.pangrova.coordinator.query.execution.dispatch.StatementCategory.DDL));
        // CTAS: create table from derived schema, then INSERT SELECT via native DML pipeline
        tasks.put(org.pangrova.parser.ast.statement.CreateTableAsSelectStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CtasTask(ddlExecutor, dmlDispatch));
        // SHOW / DESCRIBE
        tasks.put(org.pangrova.parser.ast.ShowSchemasStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.catalog.ShowSchemasTask(
                        showDescribeExecutor));
        tasks.put(org.pangrova.parser.ast.statement.ShowTablesStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.ShowTablesTask(showDescribeExecutor));
        tasks.put(org.pangrova.parser.ast.statement.ShowTableControlsStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.ShowTableControlsTask(
                        showDescribeExecutor));
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
        tasks.put(org.pangrova.parser.ast.statement.maintenance.AnalyzeStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.AnalyzeTask(
                        new org.pangrova.coordinator.query.statement.AnalyzeExecutor(
                                catalogRegistry)));
        // CALL procedure
        tasks.put(org.pangrova.parser.ast.statement.maintenance.CallStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CallProcedureTask(catalogRegistry));
        // Views
        tasks.put(org.pangrova.parser.ast.statement.CreateViewStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CreateViewTask(catalogRegistry));
        tasks.put(org.pangrova.parser.ast.statement.DropViewStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.DropViewTask(catalogRegistry));
        tasks.put(org.pangrova.parser.ast.statement.ShowViewsStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.ShowViewsTask(catalogRegistry));
        // Incremental read (CHANGES BETWEEN SNAPSHOT x AND y)
        tasks.put(org.pangrova.parser.ast.statement.IncrementalReadStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.IncrementalReadTask(catalogRegistry));
        // SHOW metadata (redirects to system table queries)
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

    private void registerDynamicTableTasks(
            org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory factory,
            org.pangrova.coordinator.query.statement.DdlExecutor ddlExecutor,
            org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch) {
        org.pangrova.coordinator.materialization.MaterializationKernelFactory kernels =
                org.pangrova.coordinator.materialization.MaterializationDdlRegistration.register(
                        factory,
                        metadataBootstrap.materializationRepository(),
                        metadataBootstrap.materializationCatalog(),
                        metadataBootstrap.catalogRegistry(),
                        Objects.requireNonNull(coordinationService,
                                "Materialization execution authority"),
                        ddlExecutor,
                        dmlDispatch,
                        metadataDataSource,
                        accessControl);
        this.materializationKernels = kernels;
    }

    org.pangrova.coordinator.materialization.MaterializationKernelFactory materializationKernels() {
        return materializationKernels;
    }

    SqlCompiler compiler() {
        return compiler;
    }

    CoordinatorResultService resultService() {
        return resultService;
    }

    private static void registerBranchTasks(
            org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory factory,
            CatalogRegistry catalogRegistry) {
        var branchDeps = new org.pangrova.coordinator.ai.branch.BranchDdlContext(catalogRegistry, null);
        var branchExecutor = new org.pangrova.coordinator.ai.branch.BranchDdlExecutor(branchDeps);
        factory.registerTask(org.pangrova.parser.ast.statement.CreateBranchStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.CreateBranchTask(branchExecutor));
        factory.registerTask(org.pangrova.parser.ast.statement.DropBranchStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.DropBranchTask(branchExecutor));
        factory.registerTask(org.pangrova.parser.ast.statement.CreateTagStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.CreateTagTask(branchExecutor));
        factory.registerTask(org.pangrova.parser.ast.statement.DropTagStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.DropTagTask(branchExecutor));

    }

    private static void registerStorageProfileTasks(
            org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory factory,
            AccessControl accessControl, javax.sql.DataSource metadataDataSource) {
        if (metadataDataSource == null) {
            return;
        }
        var registry = new org.pangrova.metadata.storage.JdbcStorageProfileRegistry(metadataDataSource);
        factory.registerTask(org.pangrova.parser.ast.statement.CreateStorageProfileStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.storage.CreateStorageProfileTask(registry));
        factory.registerTask(org.pangrova.parser.ast.statement.DropStorageProfileStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.storage.DropStorageProfileTask(registry));
        factory.registerTask(org.pangrova.parser.ast.statement.ShowStorageProfilesStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.storage.ShowStorageProfilesTask(
                        registry, accessControl));
    }

    private void registerFileSpaceTasks(
            org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory factory,
            AccessControl authorization, javax.sql.DataSource source,
            org.pangrova.security.audit.AuditEventEmitter auditOutput) {
        if (source == null) {
            return;
        }
        var registry = new org.pangrova.metadata.file.JdbcFileSpaceRegistry(source);
        var audit = new org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter(auditOutput);
        org.pangrova.coordinator.query.execution.dispatch.task.file.VolumeTasks.register(
                factory, registry, authorization, audit);
        var access = fileSpaceAccessService(registry, authorization, source, audit);
        var client = access == null ? null
                : org.pangrova.coordinator.file.WorkerFileClient
                        .create(config.internalAuthSecret());
        var refresh = access == null ? null
                : new org.pangrova.coordinator.file.StageDirectoryRefreshService(
                        new org.pangrova.metadata.file.JdbcFileDirectoryIndex(source),
                        access, client, authorization);
        org.pangrova.coordinator.query.execution.dispatch.task.file.StageTasks.register(
                factory, registry, authorization, audit, refresh);
        factory.registerTask(org.pangrova.parser.ast.statement.ListStageStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.file.ListStageTask(
                        access, client));
        factory.registerTask(org.pangrova.parser.ast.statement.RemoveStageStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.file.RemoveStageTask(
                        access, client, client));
    }

    private org.pangrova.coordinator.file.FileSpaceAccessService fileSpaceAccessService(
            org.pangrova.spi.storage.file.FileSpaceRegistry registry, AccessControl authorization,
            javax.sql.DataSource source,
            org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter audit) {
        if (authorization == null || config.internalAuthSecret() == null
                || config.internalAuthSecret().isBlank()) {
            return null;
        }
        var context = org.pangrova.coordinator.file.FileSpaceAccessService.FileSpaceAccessContext
                .of(System.getProperty("pangrova.files.managed-storage-profile", ""),
                        () -> workerManager == null ? java.util.List.of()
                                : workerManager.getActiveWorkers().stream()
                                        .map(worker -> "http://" + worker.host() + ":"
                                                + worker.httpPort())
                                        .toList(),
                        audit, config.internalAuthSecret());
        return new org.pangrova.coordinator.file.FileSpaceAccessService(registry,
                new org.pangrova.metadata.storage.JdbcStorageProfileRegistry(source),
                authorization, context);
    }

    Consumer<String> buildWriteCompleteCallback(MaintenanceScheduler ms) {
        if (ms == null) {
            return null;
        }
        return table -> ms.onWriteComplete(
                table, new org.pangrova.spi.connector.data.write.model.WriteResult(
                        table, 0, 0, 0, 0, java.time.Instant.now()));
    }

    private ShuffleLifecycleManager buildShuffleLifecycleManager() {
        ShuffleMode mode = config.shuffleMode();
        if (mode == ShuffleMode.REMOTE) {
            List<String> rawEndpoints = config.shuffleEndpoints();
            if (rawEndpoints == null || rawEndpoints.isEmpty()) {
                throw new IllegalStateException(
                        "shuffleMode=REMOTE but shuffleEndpoints is empty; "
                                + "configure at least one shuffle server endpoint");
            }
            String tlsIdentity = config.shuffleTlsConfig() == null
                    ? "" : config.shuffleTlsConfig().serverIdentity();
            List<ShuffleServiceAddress> endpoints = rawEndpoints.stream()
                    .map(endpoint -> parseEndpoint(endpoint, tlsIdentity))
                    .toList();
            if (shuffleOwnerResolver == null) {
                throw new IllegalStateException(
                        "shuffleMode=REMOTE requires durable execution ownership");
            }
            return new CoordinatorShuffleLifecycleManager(
                    endpoints, shuffleOwnerResolver, config.shuffleTlsConfig());
        }
        return null;
    }

    private static ShuffleServiceAddress parseEndpoint(
            String hostPort, String tlsIdentity) {
        int colonIdx = hostPort.lastIndexOf(':');
        if (colonIdx <= 0 || colonIdx == hostPort.length() - 1) {
            throw new IllegalArgumentException(
                    "Invalid shuffle endpoint format (expected host:port): " + hostPort);
        }
        String host = hostPort.substring(0, colonIdx);
        int port = Integer.parseInt(hostPort.substring(colonIdx + 1));
        return new ShuffleServiceAddress(host, port, tlsIdentity);
    }

    RemoteTaskDispatcher remoteTaskDispatcher() { return remoteTaskDispatcher; }

    /**
     * Registers embedding model DDL tasks (CREATE/ALTER/DROP/SHOW EMBEDDING MODEL) into the
     * metadata execution factory. Called after the embedding model registry is initialized.
     */
    void registerEmbeddingModelTasks(
            org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry registry) {
        if (metadataFactory == null || registry == null) {
            return;
        }
        org.pangrova.coordinator.ai.embedding.EmbeddingModelTaskRegistration.register(
                metadataFactory, registry, requireWorkerManager());
    }

    private WorkerManager requireWorkerManager() {
        return java.util.Objects.requireNonNull(
                workerManager, "query execution WorkerManager is not initialized");
    }

    /** Registers the metadata-only Semantic View control-plane statements. */
    private void registerSemanticViewTasks(
            org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory factory,
            org.pangrova.metadata.semantic.SemanticViewStore store) {
        var service = new org.pangrova.coordinator.semantic.SemanticViewStatementService(
                java.util.Objects.requireNonNull(store, "store must not be null"), accessControl);
        factory.registerTask(
                org.pangrova.parser.ast.statement.CreateSemanticViewStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.semantic.CreateSemanticViewTask(
                        service));
        factory.registerTask(
                org.pangrova.parser.ast.statement.PublishSemanticViewStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.semantic.PublishSemanticViewTask(
                        service));
        factory.registerTask(
                org.pangrova.parser.ast.statement.ShowSemanticViewsStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.semantic.ShowSemanticViewsTask(
                        service));
    }
}
