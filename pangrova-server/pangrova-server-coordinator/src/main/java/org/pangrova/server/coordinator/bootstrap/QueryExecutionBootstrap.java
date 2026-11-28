package org.pangrova.server.coordinator.bootstrap;
import org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.persistence.security.postgresql.tenant.PostgresDeferredTenantAuthority;

import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.indexing.IndexAwareSplitFetcher;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.CopySourceTaskBinder;
import org.pangrova.coordinator.query.execution.dispatch.task.catalog.CatalogStatementTasks;
import org.pangrova.coordinator.query.execution.dispatch.task.catalog.CreateSchemaTask;
import org.pangrova.coordinator.query.execution.dispatch.task.catalog.ShowCatalogsTask;
import org.pangrova.coordinator.scheduler.placement.CacheAffinityAssigner;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.scheduler.speculation.SpeculativeStageHandler;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.parser.api.ast.SetSessionStatement;
import org.pangrova.coordinator.query.execution.dispatch.task.SetSessionTask;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.shuffle.api.coordinator.ShuffleCoordinatorSession;
import org.pangrova.observability.api.tracing.TracingProvider;
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
    private final org.pangrova.shuffle.api.ShuffleExecutionOwnerResolver shuffleOwnerResolver;
    private final javax.sql.DataSource metadataDataSource;
    private final org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker writeCommitTracker;
    private final org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator
            generatedValueMaterialization;
    private final org.pangrova.query.api.write.WriteAuthorityResolver writeAuthoritySupplier;
    private final org.pangrova.semantic.api.version.SemanticViewStore semanticViewStore;
    private final java.util.function.Function<org.pangrova.coordinator.feature.FeatureSourceCompiler,
            org.pangrova.coordinator.feature.FeatureViewStatementService> featureViewServiceFactory;
    private final org.pangrova.ai.control.embedding.EmbeddingModelRegistry
            embeddingModelRegistry;
    private final org.pangrova.security.audit.AuditEventEmitter auditEmitter;

    private RemoteTaskDispatcher remoteTaskDispatcher;
    private ShuffleCoordinatorSession shuffleSession;
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
                         org.pangrova.semantic.api.version.SemanticViewStore semanticViewStore,
                         java.util.function.Function<org.pangrova.coordinator.feature.FeatureSourceCompiler,
                                 org.pangrova.coordinator.feature.FeatureViewStatementService>
                                 featureViewServiceFactory,
                         org.pangrova.ai.control.embedding.EmbeddingModelRegistry
                                 embeddingModelRegistry,
                         org.pangrova.shuffle.api.ShuffleExecutionOwnerResolver shuffleOwnerResolver,
                         javax.sql.DataSource metadataDataSource,
                         org.pangrova.security.audit.AuditEventEmitter auditEmitter,
                         CurrentModelAuthority modelAuthority) {
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
                        .writeAuthoritySupplier(catalogs, metadataDataSource, generatedValueMaterialization,
                                modelAuthority,
                                new PostgresDeferredTenantAuthority(),
                                AuthorizationTransactionGate::lockPublication, CoordinatorTableWriteAuthority.create());
        this.semanticViewStore = semanticViewStore;
        this.featureViewServiceFactory = featureViewServiceFactory;
        this.embeddingModelRegistry = embeddingModelRegistry;
        this.auditEmitter = auditEmitter;
    }

    QueryDispatcher buildQueryDispatcher(
            WorkerManager wm, CoordinatorTaskTracker tt, SqlCompiler compilerTemplate,
            TaskScheduler taskScheduler, IndexAwareSplitFetcher indexedReads,
            CircuitBreakerPolicy circuitBreaker, CoordinatorResultService resultSvc,
            CacheAffinityAssigner cacheAffinityAssigner,
            SpeculativeStageHandler speculativeStageHandler,
            QueryStateJournal stateJournal,
            org.pangrova.generation.publication.read.PublishedGenerationBindingResolver publishedReads,
            org.pangrova.coordinator.generation.read.GeneratedValueReadResolver generatedValueReads) {
        this.workerManager = java.util.Objects.requireNonNull(wm, "workerManager must not be null");
        this.resultService = java.util.Objects.requireNonNull(resultSvc, "resultService must not be null");
        var nativeCapabilities = org.pangrova.planner.validation.NativeCapabilityRegistry.live(() -> {
            var inventories = wm.getActiveWorkers().stream().map(
                    org.pangrova.coordinator.worker.WorkerInfo::capabilityInventory).toList();
            if (inventories.isEmpty()) {
                throw new org.pangrova.scheduler.api.exception.NoAvailableWorkerException();
            }
            return inventories;
        });
        SqlCompiler scopedCompiler = compilerTemplate.withNativeCapabilities(nativeCapabilities);
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
                    new org.pangrova.persistence.file.postgresql.copy.JdbcCopySourceReadAdmissions(metadataDataSource,
                            accessControl,
                            new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionTransactionFence()),
                    coordinationService::requireExecutionAnchor));
        }
        this.remoteTaskDispatcher = rtd;
        shuffleSession = CoordinatorShuffleProviders.open(config, shuffleOwnerResolver);
        var security = new org.pangrova.coordinator.query.execution.SecurityContextBundle(
                accessControl == null
                        ? null
                        : new org.pangrova.coordinator.query.security.AccessControlEnforcer(accessControl),
                accessControl == null ? null : new org.pangrova.planner.security.ColumnMaskApplier(accessControl),
                accessControl == null ? null : new org.pangrova.planner.security.RowFilterApplier(accessControl));
        var scheduling = new org.pangrova.coordinator.query.execution.SchedulingContextBundle(
                rtd, taskScheduler, wm, tt,
                cacheAffinityAssigner, speculativeStageHandler,
                config.spillBudget(),
                moduleWiring != null ? moduleWiring.getResilienceRegistry() : null);
        QueryDispatcherContext deps = new QueryDispatcherContext(
                scopedCompiler,
                security, scheduling,
                metadataBootstrap == null ? null : metadataBootstrap.catalogRegistry(),
                moduleWiring != null ? moduleWiring.getLineageEmitter() : null,
                config.effectiveTaskTimeoutSeconds(),
                org.pangrova.planner.adaptive.AdaptiveQueryConfig.ENABLED,
                shuffleSession, coordinationService,
                coordinationService != null ? config.coordinatorId() : null,
                stateJournal,
                indexedReads,
                embeddingModelRegistry, publishedReads, generatedValueReads);
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
        var qd = new QueryDispatcher(deps, dispatcher, tracingProvider, writeAuthoritySupplier);
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
        // Uses startDmlStatement() to bypass StatementDispatchPhase and
        // convert AST → SqlNode → RelNode directly without SQL string round-trip.
        org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch =
                (sm, session, stmt) -> {
                    QueryDispatcher qd = queryDispatcherRef.get();
                    if (qd == null) {
                        throw new IllegalStateException(
                                "QueryDispatcher not initialized");
                    }
                    return qd.startDmlStatement(sm, stmt, session);
                };

        var ddlExecutor = new org.pangrova.coordinator.query.statement.DdlExecutor(
                catalog, null,
                org.pangrova.planner.metadata.catalog.MetadataSchemaBuilder::invalidateTableCaches);
        ddlExecutor.setMetadataService(metadataBootstrap.metadataService());
        if (indexing != null && indexing.getSchemaEvolutionHandler() != null) {
            ddlExecutor.setSchemaEvolutionHandler(indexing.getSchemaEvolutionHandler());
        }
        if (metadataDataSource != null
                && (config.securityEnabled() || config.governanceEnabled())) {
            ddlExecutor.setColumnMaskStore(
                    new org.pangrova.metadata.security.TableControlColumnMaskStore(
                            catalog.tableGovernance(), new JdbcColumnMaskStore(metadataDataSource)));
        }
        var showDescribeExecutor = new org.pangrova.coordinator.query.statement.ShowDescribeExecutor(catalog);
        var maintenancePolicyService =
                catalog.tableMaintenance();
        var controlledMaintenance =
                new org.pangrova.coordinator.query.statement.ControlledTableMaintenance(
                        catalog, maintenancePolicyService,
                        moduleWiring != null ? moduleWiring.getDistributedLease() : null,
                        config.coordinatorId(), new OtelExecutionTraceContextFactory());
        if (config.internalAuthSecret() != null && !config.internalAuthSecret().isBlank()) {
            controlledMaintenance.setReclusterDispatcher(
                    new org.pangrova.coordinator.clustering.WorkerReclusterDispatcher(
                            requireWorkerManager()::getActiveWorkers, config.internalAuthSecret()));
        }
        var maintenanceExecutor = new org.pangrova.coordinator.query.statement.MaintenanceExecutor(
                controlledMaintenance);
        ddlExecutor.setMaintenancePolicyService(maintenancePolicyService);

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
                ddlExecutor, showDescribeExecutor, maintenanceExecutor, controlledMaintenance, catalog, dmlDispatch);
        registerOptionalMetadataTasks(indexing, dmlDispatch, ddlExecutor, queryDispatcherRef);
        registerBranchTasks(metadataFactory, catalog, accessControl);
        SecurityAdministrationBootstrap.register(metadataFactory, config, metadataDataSource);
        registerStorageProfileTasks(metadataFactory, accessControl, metadataDataSource);
        registerFileSpaceTasks(metadataFactory, accessControl, metadataDataSource, auditEmitter);

        var planFactory = new org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactory(
                new org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryContext(
                        dmlDispatch, writeCallback,
                        (sql, sess) -> {
                            boolean timeTravel = org.pangrova.parser.parse.TimeTravelSqlClauses
                                    .extract(sql).references().stream()
                                    .anyMatch(ref -> ref.table().timeTravelSpec().isPresent());
                            var explained = securityApplier.compileForExplain(sql, sess, timeTravel);
                            return explained.plan().planExplanation() + System.lineSeparator()
                                    + explained.rewrite().describe();
                        },
                        catalog,
                        generatedValueMaterialization));

        var factories = new ArrayList<
                org.pangrova.query.dispatch.StatementExecutionFactory<?>>();
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
                resultSvc,
                List.copyOf(factories), List.of(schemaListener), accessControl, ddlGuard);
        return new org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher(context,
                metadataDataSource == null ? null : deps.stateJournal()::reserveCoordinatorStatement);
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
        if (metadataBootstrap.materializationRepository() != null && accessControl != null
                && coordinationService != null && metadataDataSource != null) {
            registerDynamicTableTasks(metadataFactory, ddlExecutor, dmlDispatch);
        }
    }

    /**
     * Builds a MetadataExecutionFactory with control-plane DDL, SHOW/DESCRIBE,
     * and maintenance tasks. Index, stream, dynamic table, and tenant
     * tasks are registered dynamically via registerTask() after construction.
     */
    private org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory buildMetadataFactory(
            org.pangrova.coordinator.query.statement.DdlExecutor ddlExecutor,
            org.pangrova.coordinator.query.statement.ShowDescribeExecutor showDescribeExecutor,
            org.pangrova.coordinator.query.statement.MaintenanceExecutor maintenanceExecutor,
            org.pangrova.coordinator.query.statement.ControlledTableMaintenance controlledMaintenance,
            CatalogRegistry catalogRegistry,
            org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch) {
        var tasks = new java.util.HashMap<Class<? extends org.pangrova.parser.api.ast.Statement>,
                MetadataTask<?>>();
        // DDL
        var createTableTask = new org.pangrova.coordinator.query.execution.dispatch.task.CreateTableTask(ddlExecutor);
        var dropTableTask = new org.pangrova.coordinator.query.execution.dispatch.task.DropTableTask(ddlExecutor);
        var alterTableTask = new org.pangrova.coordinator.query.execution.dispatch.task.AlterTableTask(ddlExecutor);
        var createSchemaTask = new CreateSchemaTask(ddlExecutor);
        tasks.put(org.pangrova.parser.api.ast.statement.CreateTableStatement.class, createTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.DropTableStatement.class, dropTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.AddColumn.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.DropColumn.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.RenameColumn.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.ChangeColumnType.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.RenameTable.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.SetPartitionSpec.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.CreateBranch.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.DropBranch.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.CreateTag.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.DropTag.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.SetColumnTag.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.UnsetColumnTag.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.Adopt.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.AlterTableStatement.Unlink.class, alterTableTask);
        tasks.put(org.pangrova.parser.api.ast.statement.CreateSchemaStatement.class, createSchemaTask);
        CatalogStatementTasks.register(tasks, ddlExecutor, showDescribeExecutor);
        tasks.put(SetSessionStatement.class, new SetSessionTask());
        // CTAS: create table from derived schema, then INSERT SELECT via native DML pipeline
        tasks.put(org.pangrova.parser.api.ast.statement.CreateTableAsSelectStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CtasTask(
                        ddlExecutor, dmlDispatch, this::compiler));
        // SHOW / DESCRIBE
        tasks.put(org.pangrova.parser.api.ast.ShowSchemasStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.catalog.ShowSchemasTask(
                        showDescribeExecutor));
        tasks.put(org.pangrova.parser.api.ast.statement.ShowTablesStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.ShowTablesTask(showDescribeExecutor));
        tasks.put(org.pangrova.parser.api.ast.statement.ShowTableControlsStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.ShowTableControlsTask(
                        showDescribeExecutor));
        tasks.put(org.pangrova.parser.api.ast.statement.DescribeTableStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.DescribeTableTask(showDescribeExecutor));
        tasks.put(org.pangrova.parser.api.ast.statement.ShowCatalogsStatement.class,
                new ShowCatalogsTask(showDescribeExecutor));
        // Maintenance
        tasks.put(org.pangrova.parser.api.ast.statement.maintenance.CompactStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CompactTask(maintenanceExecutor));
        tasks.put(org.pangrova.parser.api.ast.statement.maintenance.VacuumStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.VacuumTask(maintenanceExecutor));
        tasks.put(org.pangrova.parser.api.ast.statement.maintenance.OptimizeStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.OptimizeTask(maintenanceExecutor));
        tasks.put(org.pangrova.parser.api.ast.statement.maintenance.AnalyzeStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.AnalyzeTask(
                        new org.pangrova.coordinator.query.statement.AnalyzeExecutor(
                                catalogRegistry)));
        org.pangrova.coordinator.query.execution.dispatch.task.ClusteringStatementTasks.register(
                tasks, controlledMaintenance);
        // CALL procedure
        tasks.put(org.pangrova.parser.api.ast.statement.maintenance.CallStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CallProcedureTask(
                        catalogRegistry, controlledMaintenance));
        // Views
        tasks.put(org.pangrova.parser.api.ast.statement.CreateViewStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CreateViewTask(catalogRegistry));
        tasks.put(org.pangrova.parser.api.ast.statement.DropViewStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.DropViewTask(catalogRegistry));
        tasks.put(org.pangrova.parser.api.ast.statement.ShowViewsStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.ShowViewsTask(catalogRegistry));
        // SHOW metadata (redirects to system table queries)
        return new org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory(tasks);
    }

    private static void registerIndexTasks(
            org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory factory,
            org.pangrova.coordinator.indexing.ddl.IndexDdlExecutor indexExecutor) {
        factory.registerTask(org.pangrova.parser.api.ast.statement.CreateIndexStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.CreateIndexTask(indexExecutor));
        factory.registerTask(org.pangrova.parser.api.ast.statement.DropIndexStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.DropIndexTask(indexExecutor));
        factory.registerTask(org.pangrova.parser.api.ast.statement.ShowIndexesStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.ShowIndexesTask(indexExecutor));
        factory.registerTask(org.pangrova.parser.api.ast.statement.AlterIndexStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.AlterIndexTask(indexExecutor));
    }

    private static void registerStreamTasks(
            org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory factory,
            org.pangrova.streaming.coordinator.StreamDdlExecutor streamExecutor) {
        factory.registerTask(org.pangrova.parser.api.ast.statement.stream.CreateStreamStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.stream.CreateStreamTask(streamExecutor));
        factory.registerTask(org.pangrova.parser.api.ast.statement.stream.AlterStreamStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.stream.AlterStreamTask(streamExecutor));
        factory.registerTask(org.pangrova.parser.api.ast.statement.stream.DropStreamStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.stream.DropStreamTask(streamExecutor));
        factory.registerTask(org.pangrova.parser.api.ast.statement.stream.ShowStreamsStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.stream.ShowStreamsTask(streamExecutor));
        factory.registerTask(org.pangrova.parser.api.ast.statement.stream.DescribeStreamStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.stream.DescribeStreamTask(streamExecutor));
    }

    private void registerDynamicTableTasks(
            org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory factory,
            org.pangrova.coordinator.query.statement.DdlExecutor ddlExecutor,
            org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction dmlDispatch) {
        org.pangrova.coordinator.materialization.MaterializationKernelFactory kernels =
                org.pangrova.server.coordinator.assembly.materialization.MaterializationDdlRegistration.register(
                        factory,
                        metadataBootstrap.materializationRepository(),
                        metadataBootstrap.materializationCatalog(),
                        metadataBootstrap.catalogRegistry(),
                        Objects.requireNonNull(coordinationService,
                                "Materialization execution authority"),
                        ddlExecutor,
                        dmlDispatch,
                        this::compiler,
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
            CatalogRegistry catalogRegistry, AccessControl accessControl) {
        var branchDeps = new org.pangrova.coordinator.ai.branch.BranchDdlContext(catalogRegistry, accessControl);
        var branchExecutor = new org.pangrova.coordinator.ai.branch.BranchDdlExecutor(branchDeps);
        factory.registerTask(org.pangrova.parser.api.ast.statement.UseBranchStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.UseBranchTask(branchExecutor));
        factory.registerTask(org.pangrova.parser.api.ast.statement.CreateBranchStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.CreateBranchTask(branchExecutor));
        factory.registerTask(org.pangrova.parser.api.ast.statement.DropBranchStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.DropBranchTask(branchExecutor));
        factory.registerTask(org.pangrova.parser.api.ast.statement.CreateTagStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.CreateTagTask(branchExecutor));
        factory.registerTask(org.pangrova.parser.api.ast.statement.DropTagStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.branch.DropTagTask(branchExecutor));

    }

    private static void registerStorageProfileTasks(
            org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory factory,
            AccessControl accessControl, javax.sql.DataSource metadataDataSource) {
        if (metadataDataSource == null) {
            return;
        }
        var registry = new JdbcStorageProfileRegistry(metadataDataSource);
        factory.registerTask(org.pangrova.parser.api.ast.statement.CreateStorageProfileStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.storage.CreateStorageProfileTask(registry));
        factory.registerTask(org.pangrova.parser.api.ast.statement.DropStorageProfileStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.storage.DropStorageProfileTask(registry));
        factory.registerTask(org.pangrova.parser.api.ast.statement.ShowStorageProfilesStatement.class,
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
        var registry = new org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry(source);
        var audit = new org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter(auditOutput::emit);
        org.pangrova.coordinator.query.execution.dispatch.task.file.VolumeTasks.register(
                factory, registry, authorization, audit);
        var access = fileSpaceAccessService(registry, authorization, source, auditOutput);
        var client = access == null ? null
                : org.pangrova.file.worker.WorkerFileClient
                        .create(config.internalAuthSecret());
        var refresh = access == null ? null
                : new org.pangrova.file.directory.StageDirectoryRefreshService(
                        new org.pangrova.persistence.file.postgresql.directory.JdbcFileDirectoryIndex(source),
                        access, client, authorization);
        org.pangrova.coordinator.query.execution.dispatch.task.file.StageTasks.register(
                factory, registry, authorization, audit, refresh);
        factory.registerTask(org.pangrova.parser.api.ast.statement.ListStageStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.file.ListStageTask(
                        access, client));
        factory.registerTask(org.pangrova.parser.api.ast.statement.RemoveStageStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.file.RemoveStageTask(
                        access, client, client));
    }

    private org.pangrova.file.access.FileSpaceAccessService fileSpaceAccessService(
            org.pangrova.persistence.file.api.space.FileSpaceRegistry registry, AccessControl authorization,
            javax.sql.DataSource source,
            org.pangrova.security.audit.AuditEventEmitter auditOutput) {
        if (authorization == null || config.internalAuthSecret() == null
                || config.internalAuthSecret().isBlank()) {
            return null;
        }
        var context = org.pangrova.file.access.FileSpaceAccessService.FileSpaceAccessContext
                .of(System.getProperty("pangrova.files.managed-storage-profile", ""),
                        () -> workerManager == null ? java.util.List.of()
                                : workerManager.getActiveWorkers().stream()
                                        .map(worker -> "http://" + worker.host() + ":"
                                                + worker.httpPort())
                                        .toList(),
                        new org.pangrova.file.access.FileGrantAudit(auditOutput::emit), config.internalAuthSecret(),
                        new org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceAuthority(source,
                                new org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore(source)));
        return new org.pangrova.file.access.FileSpaceAccessService(registry,
                new JdbcStorageProfileRegistry(source),
                authorization, context);
    }

    Consumer<String> buildWriteCompleteCallback(MaintenanceScheduler ms) {
        if (ms == null) {
            return null;
        }
        return table -> ms.onWriteComplete(
                table, new org.pangrova.connector.api.write.WriteResult(
                        table, 0, 0, 0, 0, java.time.Instant.now()));
    }

    void closeShuffleSession() {
        if (shuffleSession != null) {
            shuffleSession.close();
            shuffleSession = null;
        }
    }

    RemoteTaskDispatcher remoteTaskDispatcher() { return remoteTaskDispatcher; }

    /**
     * Registers embedding model DDL tasks (CREATE/ALTER/DROP/SHOW EMBEDDING MODEL) into the
     * metadata execution factory. Called after the embedding model registry is initialized.
     */
    void registerEmbeddingModelTasks(
            org.pangrova.ai.control.embedding.EmbeddingModelRegistry registry) {
        if (metadataFactory == null || registry == null) {
            return;
        }
        EmbeddingModelTaskRegistration.register(
                metadataFactory, registry, requireWorkerManager());
    }

    private WorkerManager requireWorkerManager() {
        return java.util.Objects.requireNonNull(
                workerManager, "query execution WorkerManager is not initialized");
    }

    /** Registers the metadata-only Semantic View control-plane statements. */
    private void registerSemanticViewTasks(
            org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory factory,
            org.pangrova.semantic.api.version.SemanticViewStore store) {
        var service = new org.pangrova.semantic.version.SemanticViewService(
                java.util.Objects.requireNonNull(store, "store must not be null"), accessControl);
        factory.registerTask(
                org.pangrova.parser.api.ast.statement.CreateSemanticViewStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.semantic.CreateSemanticViewTask(
                        service));
        factory.registerTask(
                org.pangrova.parser.api.ast.statement.PublishSemanticViewStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.semantic.PublishSemanticViewTask(
                        service));
        factory.registerTask(
                org.pangrova.parser.api.ast.statement.ShowSemanticViewsStatement.class,
                new org.pangrova.coordinator.query.execution.dispatch.task.semantic.ShowSemanticViewsTask(
                        service));
    }
}
