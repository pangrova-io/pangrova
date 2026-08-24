/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.query.execution;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.index.IndexAwareSplitFetcher;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGQueryContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.DefaultWorkerSelector;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.RuntimeFilterStageHandler;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.task.NativeWriteCommitTracker;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.runtimefilter.RuntimeFilterDescriptor;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles query scheduling and dispatch to native workers: local dispatch for
 * constant queries, DAG-based distributed dispatch for multi-stage queries,
 * vector search enrichment, and DML task completion awaiting.
 *
 * <p>Thread safety: all fields are final or volatile. DAGDispatchEngine and
 * WorkerManager are internally thread-safe.</p>
 */
@org.pangrova.common.concurrent.ThreadSafe
final class QuerySchedulingEngine {

    private static final Logger LOG = LoggerFactory.getLogger(
            QuerySchedulingEngine.class);

    private final SchedulingDependencies deps;
    private final DispatchContextEnricher enricher;
    private final QueryVectorResultMerger vectorResultMerger;
    private volatile NativeWriteCommitTracker writeCommitTracker;

    /**
     * Groups scheduling dependencies into a single parameter object.
     */
    record SchedulingDependencies(
            DAGDispatchEngine dagEngine,
            DefaultWorkerSelector workerSelector,
            WorkerManager workerManager,
            CoordinatorTaskTracker taskTracker,
            CoordinatorResultService resultService,
            CatalogRegistry catalogRegistry,
            IndexAwareSplitFetcher indexAwareSplitFetcher,
            RuntimeFilterStageHandler runtimeFilterStageHandler,
            long dagDispatchTimeoutMs) {}

    QuerySchedulingEngine(SchedulingDependencies deps) {
        this.deps = deps;
        this.enricher = new DispatchContextEnricher(deps.indexAwareSplitFetcher());
        this.vectorResultMerger = new QueryVectorResultMerger(deps.workerManager());
    }

    void setWriteCommitTracker(NativeWriteCommitTracker tracker) {
        this.writeCommitTracker = tracker;
    }

    DAGDispatchEngine dagEngine() {
        return deps.dagEngine();
    }

    // ── Main dispatch entry point ──

    DispatchResult dispatchQuery(QueryStateMachine sm,
                                 CoordinatorSessionContext session,
                                 PlannedQuery plannedQuery,
                                 DispatchQueryContext ctx) {
        LOG.info("event=dispatch_query queryId={} user={} sql_preview={}",
                session.queryId(),
                session.userIdentity() != null
                        ? session.userIdentity() : "anonymous",
                ctx.originalSql() != null && ctx.originalSql().length() > 80
                        ? ctx.originalSql().substring(0, 80) + "..."
                        : ctx.originalSql());
        DispatchResult localResult = tryLocalDispatch(sm, session, plannedQuery);
        if (localResult != null) {
            return localResult;
        }
        DispatchQueryContext effectiveCtx = enricher.enrichWithVectorSearch(
                plannedQuery, ctx, session.queryId());
        effectiveCtx = enricher.enrichWithReranker(effectiveCtx, session.queryId());
        return dispatchToWorker(sm, session, plannedQuery, effectiveCtx);
    }

    // ── Local dispatch (constant queries) ──

    private DispatchResult tryLocalDispatch(QueryStateMachine sm,
                                            CoordinatorSessionContext session,
                                            PlannedQuery plannedQuery) {
        if (plannedQuery.isConstant()) {
            return dispatchFoldedConstant(sm, session, plannedQuery);
        }
        if (plannedQuery.isZeroFetchSort() && deps.resultService() != null) {
            return DispatchResult.fromLocal(
                    deps.resultService().dispatchLocalFromTypedRows(
                            session.queryId(), List.of(),
                            plannedQuery.columnNames(),
                            sm.toFinisher()));
        }
        if (plannedQuery.isTablelessConstant() && deps.resultService() != null) {
            var evaluated = plannedQuery.evaluateConstant();
            return DispatchResult.fromLocal(
                    deps.resultService().dispatchLocalFromTypedRows(
                            session.queryId(), evaluated.rows(),
                            evaluated.columnNames(), sm.toFinisher()));
        }
        // UNNEST flows through the normal Substrait→Native path.
        // PlanNodeConverter encodes UNNEST as ExtensionSingleRel → Velox UnnestNode.
        return null;
    }

    private DispatchResult dispatchFoldedConstant(
            QueryStateMachine sm,
            CoordinatorSessionContext session,
            PlannedQuery plannedQuery) {
        if (deps.resultService() != null) {
            var evaluated = plannedQuery.evaluateConstant();
            return DispatchResult.fromLocal(
                    deps.resultService().dispatchLocalFromTypedRows(
                            session.queryId(), evaluated.rows(),
                            evaluated.columnNames(), sm.toFinisher()));
        }
        if (!deps.workerManager().getActiveWorkers().isEmpty()) {
            return null;
        }
        LOG.debug("event=folded_constant_local_complete queryId={} "
                + "reason=no_result_service_no_workers", session.queryId());
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        sm.tryTransition(QueryStateMachine.State.FINISHING);
        sm.tryTransition(QueryStateMachine.State.FINISHED);
        return new DispatchResult("localhost",
                "local-" + session.queryId(),
                new ResultEndpoint("localhost", 0,
                        "local-" + session.queryId(), "local"));
    }

    // ── Worker dispatch ──

    private DispatchResult dispatchToWorker(QueryStateMachine sm,
                                            CoordinatorSessionContext session,
                                            PlannedQuery plannedQuery,
                                            DispatchQueryContext ctx) {
        if (!sm.tryTransition(QueryStateMachine.State.SCHEDULING)) {
            return null;
        }
        try (var connectorScope = new ConnectorQueryScope(
                session.queryId(), deps.catalogRegistry())) {
            if (ctx.dmlWriteContext().isPresent()) {
                String writeMode = ctx.dmlWriteContext().get().writeMode();
                boolean skipSplitCheck = "MERGE".equals(writeMode)
                        || "OVERWRITE".equals(writeMode)
                        || "UPDATE".equals(writeMode)
                        || plannedQuery.isMergeOperation();
                if (!skipSplitCheck) {
                    var splits = fetchAndFilterSplits(
                            plannedQuery, ctx.snapshotOverrides(), session.queryId());
                    if (splits.isEmpty()) {
                        return handleZeroSplitsDml(sm, session, ctx);
                    }
                }
                ensureWriteContextRegistered(session.queryId(), ctx.dmlWriteContext().get());
            }
            CoordinatorSessionContext effectiveSession =
                    ctx.vectorSearchNode().isPresent()
                            ? DispatchContextEnricher.injectVectorSearchProperties(
                                    session, ctx.vectorSearchNode().get())
                            : session;
            if (ctx.rerankerPlan().isPresent()) {
                effectiveSession = DispatchContextEnricher.injectRerankerProperties(
                        effectiveSession, ctx.rerankerPlan().get());
            }
            DispatchResult result = dispatchByTopology(sm, effectiveSession, plannedQuery, ctx);
            result = vectorResultMerger.finalizeDispatchResult(
                    result, ctx, session.queryId());
            if (ctx.dmlWriteContext().isPresent()) {
                awaitTaskCompletion(session.queryId(), result.taskId());
            }
            return result;
        } catch (Exception e) {
            LOG.warn("event=dispatch_to_worker_failed queryId={} error={}",
                    session.queryId(), e.getMessage());
            throw e;
        }
    }

    private DispatchResult handleZeroSplitsDml(
            QueryStateMachine sm,
            CoordinatorSessionContext session,
            DispatchQueryContext ctx) {
        LOG.info("event=dml_write_noop queryId={} reason=zero_splits table={}",
                session.queryId(),
                ctx.dmlWriteContext().get().targetTable());
        if (writeCommitTracker != null) {
            writeCommitTracker.clearWriteState(session.queryId());
        }
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        sm.tryTransition(QueryStateMachine.State.FINISHING);
        sm.tryTransition(QueryStateMachine.State.FINISHED);
        return new DispatchResult("localhost",
                "noop-" + session.queryId(),
                new ResultEndpoint("localhost", 0,
                        "noop-" + session.queryId(), "local"));
    }

    // ── DAG topology dispatch ──

    /**
     * Ensures a write context is registered with the commit tracker before native
     * DML dispatch. Idempotent: no-op if already registered (e.g., by
     * PlanExecutionFactory or MergeDecomposer). This catches code paths like CTAS
     * that call executeDmlStatement() directly without prior registration.
     */
    private void ensureWriteContextRegistered(String queryId, DmlWriteContext dmlCtx) {
        if (writeCommitTracker == null || deps.catalogRegistry() == null) {
            return;
        }
        if (writeCommitTracker.hasWriteContext(queryId)) {
            return;
        }
        String targetTable = dmlCtx.targetTable();
        String[] parts = targetTable.split("\\.");
        if (parts.length < 3) {
            LOG.warn("event=write_context_skip reason=unparseable_table queryId={} table={}",
                    queryId, targetTable);
            return;
        }
        String catalog = parts[0];
        String schema = parts[1];
        String table = parts[2];
        try {
            var catalogHandle = deps.catalogRegistry().getCatalog(catalog);
            if (catalogHandle == null) {
                LOG.warn("event=write_context_skip reason=catalog_not_found queryId={} catalog={}",
                        queryId, catalog);
                return;
            }
            var connectorSession = org.pangrova.coordinator.query.statement.TableResolver.buildSession(catalog);
            var metadata = catalogHandle.getMetadata(connectorSession);
            var qualifiedName = new org.pangrova.spi.connector.metadata.table.QualifiedTableName(
                    catalog, schema, table);
            var tableHandle = org.pangrova.coordinator.query.statement.TableResolver
                    .requireTableHandle(metadata, connectorSession, qualifiedName);
            boolean useInsert = "INSERT".equals(dmlCtx.writeMode());
            var outputHandle = useInsert
                    ? metadata.beginInsert(connectorSession, tableHandle)
                    : metadata.beginOverwrite(connectorSession, tableHandle);
            writeCommitTracker.registerWriteContext(queryId, catalog, outputHandle, connectorSession);
            LOG.info("event=write_context_auto_registered queryId={} table={} writeMode={}",
                    queryId, targetTable, dmlCtx.writeMode());
        } catch (Exception e) {
            LOG.warn("event=write_context_registration_failed queryId={} table={} error={}",
                    queryId, targetTable, e.getMessage());
        }
    }

    private DispatchResult dispatchByTopology(
            QueryStateMachine sm,
            CoordinatorSessionContext session,
            PlannedQuery plannedQuery,
            DispatchQueryContext ctx) {
        StageDAG rawDag = StageDAG.fromFragmentedPlan(plannedQuery.fragment());
        StageDAG dag = rawDag.withStages(session.queryId());
        registerStageListeners(dag, sm);
        registerRuntimeFilterTargets(
                ctx.runtimeFilterDescriptors(), dag, session.queryId());

        WriteConfig writeConfig = ctx.dmlWriteContext()
                .map(d -> new WriteConfig(d.targetTable(), d.writeMode(),
                        List.of(), "parquet", 0))
                .orElse(null);
        var dagQueryContext = new DAGQueryContext(
                ctx.snapshotOverrides(), ctx.originalSql(),
                ctx.preOptColumnNames(), writeConfig);

        DispatchResult result = deps.dagEngine().dispatch(
                session, dag, ctx.compiler(), deps.workerSelector(),
                deps.dagDispatchTimeoutMs(), dagQueryContext);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        return result;
    }

    // ── Task completion ──

    private void awaitTaskCompletion(String queryId, String taskId) {
        long timeoutSec = TimeUnit.MILLISECONDS.toSeconds(
                deps.dagDispatchTimeoutMs());
        try {
            deps.taskTracker().awaitTaskCompletion(queryId, taskId)
                    .get(timeoutSec, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new PangrovaException(
                    PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    "Native DML task timed out after " + timeoutSec
                            + "s: queryId=" + queryId
                            + " taskId=" + taskId);
        } catch (ExecutionException e) {
            throw new PangrovaException(
                    PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    "Native DML task failed: queryId=" + queryId
                            + " taskId=" + taskId
                            + " error=" + e.getCause().getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PangrovaException(
                    PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    "Interrupted waiting for DML task: queryId="
                            + queryId);
        }
    }

    // ── Split and stage infrastructure ──

    List<SplitFetcher.SplitWithPaths> fetchAndFilterSplits(
            PlannedQuery plannedQuery, Map<String, Long> snapshotOverrides,
            String queryId) {
        if (deps.indexAwareSplitFetcher() != null) {
            return deps.indexAwareSplitFetcher().fetchAndFilterSplits(
                    plannedQuery, snapshotOverrides, queryId);
        }
        return SplitFetcher.fetchSplitsWithFilePaths(
                plannedQuery, deps.catalogRegistry(), snapshotOverrides);
    }

    void registerStageListeners(StageDAG dag, QueryStateMachine querySm) {
        for (Stage stage : dag.stageEntities()) {
            stage.stateMachine().addStateChangeListener(
                    (stageId, oldState, newState) ->
                            querySm.updateStageState(stageId, newState));
        }
    }

    private void registerRuntimeFilterTargets(
            List<RuntimeFilterDescriptor> descriptors,
            StageDAG dag, String queryId) {
        if (deps.runtimeFilterStageHandler() == null
                || descriptors.isEmpty()) {
            return;
        }
        var roots = dag.leafStages();
        if (roots.size() < 2) {
            return;
        }
        String buildStageId = String.valueOf(roots.get(0).stageId());
        String probeStageId = String.valueOf(roots.get(1).stageId());
        for (var descriptor : descriptors) {
            deps.runtimeFilterStageHandler().registerFilterTarget(
                    buildStageId, probeStageId,
                    descriptor.buildColumn(), descriptor.probeColumn(),
                    descriptor.buildColumnType());
            LOG.info("event=runtime_filter_target_wired queryId={} "
                            + "buildStageId={} probeStageId={} "
                            + "buildColumn={} probeColumn={}",
                    queryId, buildStageId, probeStageId,
                    descriptor.buildColumn(), descriptor.probeColumn());
        }
    }
}
