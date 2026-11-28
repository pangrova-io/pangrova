package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.observability.logging.MdcTaskWrapper;
import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.query.api.submission.AsyncSubmitConfig;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.runtime.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.persistence.coordination.postgresql.membership.JdbcWorkerRegistry;
import org.pangrova.query.audit.QueryAuditService;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.server.coordinator.enums.AuditWriterConfig;
import org.pangrova.server.coordinator.assembly.security.audit.AuditWriterFactory;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.persistence.coordination.api.membership.WorkerRegistry;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds the query manager and its durable runtime registries. */
final class QueryRuntimeFactory {

    private static final Logger LOG = LoggerFactory.getLogger(QueryRuntimeFactory.class);
    private static final double DEFAULT_THROTTLE_BURST = 50.0;

    private final Dependencies dependencies;

    QueryRuntimeFactory(Dependencies dependencies) {
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

    QueryStateJournal createStateJournal(
            org.pangrova.common.lifecycle.ProcessIncarnationStore.Identity processIdentity) {
        if (dependencies.metadataDataSource() == null) {
            return new org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal();
        }
        var config = new StateJournalConfig("JDBC", null, StateJournalConfig.NONE.flushIntervalMs(),
                StateJournalConfig.NONE.batchSize(), StateJournalConfig.NONE.retentionHours(),
                StateJournalConfig.NONE.maxResidentEvents());
        return org.pangrova.server.coordinator.bootstrap.query.state.QueryStateJournalFactory.create(
                config, dependencies.metadataDataSource(), processIdentity);
    }

    org.pangrova.ai.control.embedding.EmbeddingModelRegistry createEmbeddingModelRegistry() {
        if (!dependencies.config().embeddingEnabled()) return null;
        if (dependencies.metadataDataSource() == null) {
            throw new IllegalStateException("embedding model catalog requires the durable metadata DataSource");
        }
        var registry = new org.pangrova.persistence.ai.postgresql.embedding.JdbcEmbeddingModelRegistry(
                dependencies.metadataDataSource(), "admin", "users");
        registry.validateSchema();
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
            QueryStateJournal stateJournal, AuditEventEmitter auditEmitter,
            java.util.Optional<org.pangrova.query.api.retention.ReaderPinLifecycle> readerPins) {
        java.util.Objects.requireNonNull(stateJournal, "stateJournal must be selected explicitly");
        AsyncSubmitConfig asyncConfig = AsyncSubmitConfig.defaultEnabled();
        ExecutorService asyncExecutor = createAsyncSubmitExecutor();
        ScheduledExecutorService submitDeadlines = ManagedExecutorFactory.newScheduledThreadPool(
                "coordinator-submit-deadline", 2);
        QueryManager queryManager;
        try {
            var admission = org.pangrova.coordinator.query.QueryManagerCollaborators.admission(
                    dependencies.moduleWiring().getResilienceRegistry(),
                    new org.pangrova.planner.throttle.QueryThrottlingService(
                            1000, 100.0, DEFAULT_THROTTLE_BURST));
            var audit = org.pangrova.coordinator.query.QueryManagerCollaborators.audit(
                    auditEmitter, dependencies.queryAuditService(),
                    dependencies.moduleWiring().getLineageEmitter(),
                    queryDispatcher, null, stateJournal.commitsLineageFacts());
            QueryManagerContext context = new QueryManagerContext(
                    queryDispatcher, dependencies.tracingProvider(), dependencies.metricsProvider(),
                    org.pangrova.coordinator.query.QueryManagerCollaborators.tenant(
                            dependencies.moduleWiring().getTenantResolver(),
                            dependencies.moduleWiring().getQuotaChecker()),
                    admission, audit,
                    org.pangrova.coordinator.query.QueryManagerCollaborators.journal(
                            stateJournal, audit),
                    null, asyncConfig, asyncExecutor, submitDeadlines,
                    new OtelExecutionTraceContextFactory());
            LOG.info("event=async_submit_wired enabled={} submitTimeoutMs={}",
                    asyncConfig.enabled(), asyncConfig.submitTimeoutMs());
            queryManager = new QueryManager(context, readerPins);
        } catch (RuntimeException | Error failure) {
            org.pangrova.common.lifecycle.ResourceCloser.closeAll(failure,
                    asyncExecutor::shutdownNow, submitDeadlines::shutdownNow);
            throw failure;
        }
        queryManager.setLocalCoordinatorId(dependencies.config().coordinatorId());
        return new QueryManagerResult(queryManager, auditEmitter, asyncExecutor, submitDeadlines);
    }

    static ExecutorService createAsyncSubmitExecutor() {
        return new MdcTaskWrapper().wrapExecutorService(
                ManagedExecutorFactory.newBoundedThreadPool("async-query-submit", 4, 128));
    }

    record Dependencies(
            CoordinatorBootstrapConfig config,
            TracingProvider tracingProvider,
            MetricsProvider metricsProvider,
            ModuleIntegrationBootstrap moduleWiring,
            DataSource metadataDataSource,
            QueryAuditService queryAuditService) {
    }

    record QueryManagerResult(
            QueryManager queryManager,
            AuditEventEmitter auditEmitter,
            ExecutorService asyncExecutor, ScheduledExecutorService submitDeadlines) {
    }
}
