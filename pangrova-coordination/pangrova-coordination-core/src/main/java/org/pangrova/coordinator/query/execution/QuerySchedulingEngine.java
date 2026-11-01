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
import org.pangrova.coordinator.indexing.IndexAwareSplitFetcher;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.security.SecurityPolicySignatureProvider;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGQueryContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RerankIntent;
import org.pangrova.coordinator.query.execution.retrieval.RetrievalIntentCollector;
import org.pangrova.coordinator.query.execution.dispatch.dag.DefaultWorkerSelector;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.RuntimeFilterStageHandler;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;
import org.pangrova.coordinator.query.task.write.WriteAllocationIntent;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.runtimefilter.RuntimeFilterDescriptor;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles query scheduling and dispatch to native workers. Every relational
 * query, including constants and zero-row plans, follows the same DAG/Native
 * execution path. The coordinator never evaluates or buffers query rows.
 *
 * <p>Thread safety: all fields are final or volatile. DAGDispatchEngine and
 * WorkerManager are internally thread-safe.</p>
 */
@org.pangrova.common.concurrent.ThreadSafe
final class QuerySchedulingEngine {

    private static final Logger LOG = LoggerFactory.getLogger(
            QuerySchedulingEngine.class);
    private static final String POLICY_CONTEXT_PROPERTY =
            "pangrova.policy_context_hash";
    private static final int SQL_PREVIEW_LIMIT = 80;

    private final SchedulingDependencies deps;
    private final DispatchContextEnricher enricher;
    private final RetrievalIntentCollector retrievalIntents;
    private volatile NativeWriteCommitTracker writeCommitTracker;
    private volatile GeneratedWriteAuthoritySupplier generatedWriteAuthoritySupplier;

    /**
     * Groups scheduling dependencies into a single parameter object.
     */
    record SchedulingDependencies(
            DAGDispatchEngine dagEngine,
            DefaultWorkerSelector workerSelector,
            WorkerManager workerManager,
            CoordinatorTaskTracker taskTracker,
            CatalogRegistry catalogRegistry,
            IndexAwareSplitFetcher indexAwareSplitFetcher,
            RuntimeFilterStageHandler runtimeFilterStageHandler,
            long dagDispatchTimeoutMs) {}

    QuerySchedulingEngine(SchedulingDependencies deps) {
        this.deps = deps;
        this.enricher = new DispatchContextEnricher(deps.indexAwareSplitFetcher());
        this.retrievalIntents = new RetrievalIntentCollector(deps.workerManager());
    }

    void setWriteCommitTracker(NativeWriteCommitTracker tracker) {
        this.writeCommitTracker = tracker;
    }

    void setGeneratedWriteAuthoritySupplier(GeneratedWriteAuthoritySupplier supplier) {
        this.generatedWriteAuthoritySupplier = java.util.Objects.requireNonNull(supplier);
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
                ctx.originalSql() != null && ctx.originalSql().length() > SQL_PREVIEW_LIMIT
                        ? ctx.originalSql().substring(0, SQL_PREVIEW_LIMIT) + "..."
                        : ctx.originalSql());
        DispatchQueryContext effectiveCtx = enricher.enrichWithVectorSearch(
                plannedQuery, ctx, session.queryId(), session.tenantId());
        effectiveCtx = enricher.enrichWithReranker(
                effectiveCtx, session.queryId(), session.tenantId());
        return dispatchToWorker(
                sm, withPolicyContext(session, plannedQuery), plannedQuery, effectiveCtx);
    }

    private static CoordinatorSessionContext withPolicyContext(
            CoordinatorSessionContext session, PlannedQuery securedPlan) {
        String signature = SecurityPolicySignatureProvider.computeEffectiveSignature(
                session.userIdentity(), securedPlan);
        Map<String, String> properties = new java.util.HashMap<>(
                session.sessionProperties());
        properties.put(POLICY_CONTEXT_PROPERTY, signature);
        return session.withSessionProperties(properties);
    }

    // ── Worker dispatch ──

    private DispatchResult dispatchToWorker(QueryStateMachine sm,
                                            CoordinatorSessionContext session,
                                            PlannedQuery plannedQuery,
                                            DispatchQueryContext ctx) {
        if (!sm.tryTransition(QueryStateMachine.State.SCHEDULING)) {
            return null;
        }
        try {
            if (ctx.dmlWriteContext().isPresent()) {
                ensureWriteContextRegistered(session, ctx.dmlWriteContext().get());
            }
            DispatchResult result = dispatchByTopology(sm, session, plannedQuery, ctx);
            if (ctx.dmlWriteContext().isPresent()) {
                awaitWriteCompletion(sm, session.queryId(), result.taskId(), deps.dagDispatchTimeoutMs());
            }
            return result;
        } catch (Exception e) {
            LOG.warn("event=dispatch_to_worker_failed queryId={} error={}",
                    session.queryId(), e.getMessage());
            throw e;
        }
    }

    // ── DAG topology dispatch ──

    /**
     * Ensures a write context is registered with the commit tracker before native
     * DML dispatch, after the single protected compilation. This is the sole
     * allocation path, including CTAS and pre-parsed statement entry points.
     */
    void ensureWriteContextRegistered(
            CoordinatorSessionContext session, DmlWriteContext dmlCtx) {
        String queryId = session.queryId();
        requireWritePrerequisites(session);
        var observation = dmlCtx.policyObservation().orElseThrow(() ->
                new IllegalStateException("Distributed write requires actual compiled policy observations"));
        if (deps.catalogRegistry() == null) {
            throw new IllegalStateException(
                    "Distributed write requires a catalog registry to create its commit context");
        }
        var targetTable = dmlCtx.targetTable();
        String catalog = targetTable.catalog();
        String schema = targetTable.schema();
        String table = targetTable.table();
        try {
            var allocationIntent = new WriteAllocationIntent(dmlCtx.writeMode(),
                    staticPartitionBindings(dmlCtx.partitionSpec()));
            var tableControl = deps.catalogRegistry().tableControl();
            var catalogHandle = deps.catalogRegistry().getCatalog(catalog);
            if (catalogHandle == null) {
                throw new IllegalStateException(
                        "Distributed write catalog not found: " + catalog);
            }
            var connectorSession = org.pangrova.coordinator.query.session.SessionContextConverter
                    .toConnectorSession(session, catalog, schema);
            var metadata = catalogHandle.getMetadata(connectorSession);
            var qualifiedName = new org.pangrova.spi.connector.metadata.table.QualifiedTableName(
                    catalog, schema, table);
            var tableHandle = org.pangrova.coordinator.query.statement.TableResolver
                    .requireTableHandle(metadata, connectorSession, qualifiedName);
            tableControl.requireManagedForWrite(catalog, schema, table, dmlCtx.writeMode(),
                    tableControl.enforcing()
                            ? resolveTableIdentity(metadata, connectorSession, tableHandle)
                            : java.util.Optional.empty());
            requireWriteCapability(metadata, connectorSession, tableHandle, dmlCtx.writeMode(),
                    qualifiedName);
            var deferredAuthority = resolveGeneratedWriteAuthority(
                    qualifiedName, metadata, tableHandle, connectorSession, dmlCtx.writeMode(), observation,
                    session.internalMaterializationExecution());
            var reservation = writeCommitTracker.reserveWriteContext(queryId);
            if (!reservation.owner()) {
                requireSameAdmittedWrite(reservation.awaitResult(), connectorSession,
                        deferredAuthority, allocationIntent);
                return;
            }
            allocateReservedWrite(reservation, new WriteAdmission(queryId, catalog, connectorSession, metadata,
                    tableHandle, dmlCtx, catalogHandle, deferredAuthority, allocationIntent));
            LOG.info("event=write_context_auto_registered queryId={} table={} writeMode={}",
                    queryId, targetTable, dmlCtx.writeMode());
        } catch (org.pangrova.spi.security.AccessDeniedException denied) {
            throw denied;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Distributed write context registration failed for queryId="
                            + queryId + " table=" + targetTable, e);
        }
    }

    private record WriteAdmission(String queryId, String catalogName, ConnectorSession session,
            ConnectorMetadata metadata, ConnectorTableHandle table, DmlWriteContext context,
            org.pangrova.metadata.api.catalog.CatalogHandle catalogHandle,
            org.pangrova.spi.connector.coordination.WriteAuthorizationReceipt authority,
            WriteAllocationIntent intent) {}

    private void allocateReservedWrite(
            NativeWriteCommitTracker.WriteReservation reservation, WriteAdmission admission) {
        ConnectorOutputTableHandle outputHandle = null;
        try {
            reservation.beginAllocation();
            outputHandle = beginWrite(admission.metadata(), admission.session(),
                    admission.table(), admission.context());
            var allocated = outputHandle;
            byte[] binding = ConnectorWriteBindingFactory.create(admission.catalogHandle(), allocated).toByteArray();
            reservation.publish(() -> writeCommitTracker.registerWriteContext(
                    admission.queryId(), admission.catalogName(),
                    allocated, admission.session(), binding, admission.authority(), admission.intent(), null));
        } catch (RuntimeException | Error failure) {
            reservation.fail(failure, outputHandle);
            throw failure;
        }
    }

    private static java.util.Optional<org.pangrova.spi.connector.metadata.table.TableIdentity>
            resolveTableIdentity(ConnectorMetadata metadata, ConnectorSession session,
                    ConnectorTableHandle handle) {
        try {
            return metadata.getTableIdentity(session, handle);
        } catch (RuntimeException unavailable) {
            // The control record carries the identity that admission must fence
            // against; an unavailable observation is treated as no evidence.
            return java.util.Optional.empty();
        }
    }

    private void requireWritePrerequisites(CoordinatorSessionContext session) {
        if (session.tenantId() == null || session.tenantId().isBlank()) {
            throw new IllegalArgumentException("Distributed write requires a resolved tenant identity");
        }
        if (writeCommitTracker == null) {
            throw new IllegalStateException("Distributed write requires a commit tracker");
        }
    }

    private static void requireSameAdmittedWrite(NativeWriteCommitTracker.WriteCommitState existing,
            ConnectorSession current, org.pangrova.spi.connector.coordination.WriteAuthorizationReceipt authority,
            WriteAllocationIntent allocationIntent) {
        if (!existing.authorizationReceipt().equals(authority)
                || !existing.allocationIntent().equals(allocationIntent)
                || !existing.session().getUser().equals(current.getUser())
                || !java.util.Objects.equals(existing.session().getProperties().get("tenant_id"),
                        current.getProperties().get("tenant_id"))) {
            throw new org.pangrova.spi.security.AccessDeniedException(
                    "A query write context cannot be reused with different authority or declaration");
        }
    }

    private org.pangrova.spi.connector.coordination.WriteAuthorizationReceipt resolveGeneratedWriteAuthority(
            org.pangrova.spi.connector.metadata.table.QualifiedTableName table,
            ConnectorMetadata metadata, ConnectorTableHandle handle,
            ConnectorSession session, String writeMode, WritePolicyObservation observation,
            boolean internalMaterializationExecution) {
        var currentMetadata = metadata.getTableMetadata(session, handle);
        var definitions = org.pangrova.protocol.metadata.GeneratedColumnMetadataCodec.decode(
                currentMetadata.properties());
        var currentDeclaration = org.pangrova.spi.connector.coordination.WriteDeclarationSnapshot.capture(
                table, currentMetadata, definitions.toByteArray(), definitions.getDefinitionsCount() != 0);
        if (!currentDeclaration.equals(observation.declaration())) {
            throw new org.pangrova.spi.security.AccessDeniedException(
                    "Compiled write declaration changed before admission");
        }
        return GeneratedWriteAuthorityAdmission.resolve(
                new GeneratedWriteAuthoritySupplier.WriteAuthorityRequest(table, session, writeMode,
                        observation.reads(), observation.writePrivileges(), observation.declaration(),
                        internalMaterializationExecution),
                definitions, generatedWriteAuthoritySupplier);
    }

    /**
     * Rejects a write the connector has not declared. Undeclared capabilities are
     * unsupported, so a connector cannot enter the write path by omission.
     */
    private static void requireWriteCapability(ConnectorMetadata metadata,
            ConnectorSession session, ConnectorTableHandle table, String writeMode,
            org.pangrova.spi.connector.metadata.table.QualifiedTableName target) {
        var capabilities = metadata.getTableCapabilities(session, table);
        String mode = writeMode == null ? "" : writeMode.toUpperCase(java.util.Locale.ROOT);
        boolean requiresDelete = mode.equals("DELETE") || mode.equals("UPDATE")
                || mode.equals("MERGE");
        if (requiresDelete && !capabilities.supportsDelete()) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    metadata.getClass().getSimpleName() + " does not support "
                            + mode + " on " + target + " (no delete capability declared)");
        }
        if (!capabilities.supportsWrite()) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    metadata.getClass().getSimpleName() + " does not support writes on "
                            + target + " (no write capability declared)");
        }
    }

    private static ConnectorOutputTableHandle beginWrite(
            ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle table, DmlWriteContext context) {
        return switch (context.writeMode()) {
            case "INSERT" -> metadata.beginInsert(session, table);
            case "OVERWRITE", "UPDATE", "DELETE", "MERGE" ->
                    context.partitionSpec().isEmpty() ? metadata.beginOverwrite(session, table)
                            : metadata.beginOverwrite(session, table, staticPartitionBindings(context.partitionSpec()));
            default -> throw new IllegalArgumentException(
                    "Unsupported distributed write mode: " + context.writeMode());
        };
    }

    private static Map<String, String> staticPartitionBindings(List<String> specification) {
        Map<String, String> result = new java.util.TreeMap<>();
        for (String binding : specification) {
            int separator = binding.indexOf('=');
            if (separator <= 0 || result.putIfAbsent(binding.substring(0, separator),
                    binding.substring(separator + 1)) != null) {
                throw new IllegalArgumentException(
                        "Static overwrite partition bindings must be unique name=value pairs");
            }
        }
        return Map.copyOf(result);
    }

    private DispatchResult dispatchByTopology(
            QueryStateMachine sm,
            CoordinatorSessionContext session,
            PlannedQuery plannedQuery,
            DispatchQueryContext ctx) {
        var artifactIntents = retrievalIntents.collectRetrievalArtifactIntents(ctx, session.tenantId());
        int workerCount = deps.workerSelector().activeWorkerCount();
        int tasksPerWorker = org.pangrova.coordinator.query.execution.dispatch.dag.WorkerSelector
                .MAX_TASKS_PER_WORKER;
        int clusterParallelismLimit = workerCount > Integer.MAX_VALUE / tasksPerWorker
                ? Integer.MAX_VALUE : Math.max(1, workerCount * tasksPerWorker);
        var fragments = artifactIntents.isEmpty()
                ? plannedQuery.fragmentForCluster(clusterParallelismLimit)
                : plannedQuery.fragmentWithPhysicalRetrievalIdentity(clusterParallelismLimit);
        ctx.vectorSearchNode().ifPresent(vector -> LOG.info(
                "event=vector_retrieval_plan queryId={} indexId={} intentCount={} stageCount={}",
                session.queryId(), vector.indexId(), artifactIntents.size(),
                fragments.stages().size()));
        StageDAG rawDag = StageDAG.fromFragmentedPlan(fragments);
        StageDAG dag = rawDag.withStages(session.queryId());
        registerStageListeners(dag, sm);
        registerRuntimeFilterTargets(
                ctx.runtimeFilterDescriptors(), dag, session.queryId());

        WriteConfig writeConfig = ctx.dmlWriteContext()
                .map(d -> new WriteConfig(d.targetTable().toString(), d.writeMode(),
                        d.partitionSpec(), writeFileFormat(session.queryId()), 0,
                        connectorWriteBinding(session.queryId())))
                .orElse(null);
        java.util.Optional<RerankIntent> rerank = retrievalIntents.collectRerankIntent(ctx);
        var dagQueryContext = new DAGQueryContext(
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(session),
                ctx.snapshotOverrides(), ctx.originalSql(),
                ctx.preOptColumnNames(), writeConfig,
                artifactIntents,
                retrievalIntents.collectRetrievalMergeIntent(ctx),
                rerank);

        DispatchResult result = deps.dagEngine().dispatch(
                session, dag, ctx.compiler(), deps.workerSelector(),
                deps.dagDispatchTimeoutMs(), dagQueryContext,
                ready -> onTaskReady(sm, ctx, ready.resultEndpoint()));
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        return result;
    }

    static void onTaskReady(QueryStateMachine sm, DispatchQueryContext context,
                            org.pangrova.client.protocol.ResultEndpoint endpoint) {
        // A WRITE task emits commit metadata, not a Flight result. Publish its
        // bounded control-plane acknowledgement after the fenced catalog commit.
        if (context.dmlWriteContext().isEmpty()) {
            sm.setResultEndpoint(endpoint);
        }
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        if (context.dmlWriteContext().isPresent()) {
            sm.markWriteDispatchReady();
        }
    }

    private String connectorWriteBinding(String queryId) {
        if (writeCommitTracker == null || deps.catalogRegistry() == null) {
            throw new IllegalStateException(
                    "Distributed write binding requires commit tracker and catalog registry");
        }
        var writeState = writeCommitTracker.peekWriteState(queryId);
        if (writeState == null) {
            throw new IllegalStateException(
                    "Distributed write output handle is not registered: " + queryId);
        }
        return java.util.Base64.getEncoder().encodeToString(writeState.writeOutputBinding());
    }

    private String writeFileFormat(String queryId) {
        var state = writeCommitTracker.peekWriteState(queryId);
        if (state == null) {
            throw new IllegalStateException("Distributed write output handle is not registered: " + queryId);
        }
        var physical = state.outputHandle().nativePhysicalWriteAllocation();
        if (physical.isPresent()) return physical.orElseThrow().policy().options().get("file.format");
        return state.outputHandle().nativeWriteAllocation().isPresent() ? "lance" : "parquet";
    }

    // ── Task completion ──

    static void awaitWriteCompletion(QueryStateMachine query, String queryId, String taskId,
                                     long timeoutMs) {
        long timeoutSec = TimeUnit.MILLISECONDS.toSeconds(timeoutMs);
        try {
            QueryStateMachine.State terminal = query.nativeWriteCompletionReady()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            if (terminal != QueryStateMachine.State.FINISHED) {
                throw new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED,
                        "Native DML task failed: queryId=" + queryId
                                + " taskId=" + taskId + " state=" + terminal
                                + " error=" + query.getError().map(error -> error.message()).orElse("none"));
            }
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
            String queryId, CoordinatorSessionContext session) {
        if (deps.indexAwareSplitFetcher() != null) {
            return deps.indexAwareSplitFetcher().fetchAndFilterSplits(
                    plannedQuery, snapshotOverrides, queryId, session);
        }
        return SplitFetcher.fetchSplitsWithFilePaths(
                plannedQuery, deps.catalogRegistry(), snapshotOverrides, session);
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
