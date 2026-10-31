package org.pangrova.coordinator.bootstrap;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.coordinator.query.execution.AsyncSubmitConfig;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordination.jdbc.JdbcWorkerRegistry;
import org.pangrova.query.adapter.audit.LogAuditStore;
import org.pangrova.query.core.audit.QueryAuditService;
import org.pangrova.security.core.audit.AuditEventEmitter;
import org.pangrova.security.assembly.audit.AuditWriterConfig;
import org.pangrova.security.assembly.audit.AuditWriterFactory;
import org.pangrova.query.api.state.QueryStateJournal;
import org.pangrova.query.api.state.StateJournalConfig;
import org.pangrova.coordination.api.WorkerRegistry;
import org.pangrova.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.observability.api.MetricsProvider;
import org.pangrova.observability.api.TracingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds the query manager and its durable runtime registries. */
final class QueryRuntimeWiring {

    private static final Logger LOG = LoggerFactory.getLogger(QueryRuntimeWiring.class);
    private static final double DEFAULT_THROTTLE_BURST = 50.0;

    private final Dependencies dependencies;

    QueryRuntimeWiring(Dependencies dependencies) {
        this.dependencies = dependencies;
    }

    WorkerRegistry createWorkerRegistry() {
        if (dependencies.metadataDataSource() != null) {
            LOG.info("event=worker_registry_selected type=JDBC");
            return new JdbcWorkerRegistry(dependencies.metadataDataSource());
        }
        LOG.warn("event=worker_registry_selected type=InMemory reason=no_datasource");
        return new InMemoryWorkerRegistry();
    }

    QueryStateJournal createStateJournal() {
        if (dependencies.metadataDataSource() == null) {
            return new org.pangrova.query.store.memory.InMemoryQueryStateJournal();
        }
        var config = new StateJournalConfig("JDBC", null, StateJournalConfig.NONE.flushIntervalMs(),
                StateJournalConfig.NONE.batchSize(), StateJournalConfig.NONE.retentionHours(),
                StateJournalConfig.NONE.maxResidentEvents());
        return org.pangrova.query.core.state.QueryStateJournalFactory.create(
                config, dependencies.metadataDataSource());
    }

    org.pangrova.ai.control.embedding.EmbeddingModelRegistry createEmbeddingModelRegistry() {
        if (!dependencies.config().embeddingEnabled()) return null;
        if (dependencies.metadataDataSource() == null) {
            throw new IllegalStateException("embedding model catalog requires the durable metadata DataSource");
        }
        var registry = new org.pangrova.ai.control.store.jdbc.embedding.JdbcEmbeddingModelRegistry(
                dependencies.metadataDataSource(), "admin", "users");
        registry.initSchema();
        return registry;
    }

    /**
     * Audit events are durable when the metadata DataSource is available and
     * always mirrored to the log. The in-memory deployment explicitly selects
     * log-only auditing; the writer factory never substitutes a requested backend.
     */
    AuditEventEmitter createAuditEmitter() {
        return new AuditEventEmitter(AuditWriterFactory.create(
                dependencies.metadataDataSource() == null ? AuditWriterConfig.LOG : AuditWriterConfig.COMPOSITE,
                dependencies.metadataDataSource()));
    }

    QueryManagerResult buildQueryManager(QueryDispatcher queryDispatcher,
            QueryStateJournal stateJournal, AuditEventEmitter auditEmitter) {
        AsyncSubmitConfig asyncConfig = AsyncSubmitConfig.defaultEnabled();
        ExecutorService asyncExecutor = ManagedExecutorFactory.newBoundedThreadPool(
                "async-query-submit", 4, 128);
        QueryManagerContext context = new QueryManagerContext(
                queryDispatcher, dependencies.tracingProvider(), dependencies.metricsProvider(),
                dependencies.moduleWiring().getTenantResolver(),
                dependencies.moduleWiring().getQuotaChecker(),
                dependencies.moduleWiring().getLineageEmitter(), auditEmitter,
                new QueryAuditService(LogAuditStore.shared()),
                dependencies.moduleWiring().getResilienceRegistry(),
                new org.pangrova.planner.throttle.QueryThrottlingService(1000, 100.0, DEFAULT_THROTTLE_BURST),
                stateJournal, null, null, asyncConfig, asyncExecutor);
        LOG.info("event=async_submit_wired enabled={} submitTimeoutMs={}",
                asyncConfig.enabled(), asyncConfig.submitTimeoutMs());
        var queryManager = new QueryManager(context);
        queryManager.setLocalCoordinatorId(dependencies.config().coordinatorId());
        return new QueryManagerResult(queryManager, auditEmitter, asyncExecutor);
    }

    record Dependencies(
            CoordinatorBootstrapConfig config,
            TracingProvider tracingProvider,
            MetricsProvider metricsProvider,
            ModuleIntegrationBootstrap moduleWiring,
            DataSource metadataDataSource) {
    }

    record QueryManagerResult(
            QueryManager queryManager,
            AuditEventEmitter auditEmitter,
            ExecutorService asyncExecutor) {
    }
}
