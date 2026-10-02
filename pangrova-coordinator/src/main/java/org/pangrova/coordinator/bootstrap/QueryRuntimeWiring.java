package org.pangrova.coordinator.bootstrap;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.coordinator.query.AsyncSubmitConfig;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.coordination.JdbcWorkerRegistry;
import org.pangrova.observability.audit.LogAuditStore;
import org.pangrova.observability.audit.QueryAuditService;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.audit.LoggingAuditEventWriter;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.StateJournalConfig;
import org.pangrova.spi.coordination.WorkerRegistry;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.TracingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds the query manager and its durable runtime registries. */
final class QueryRuntimeWiring {

    private static final Logger LOG = LoggerFactory.getLogger(QueryRuntimeWiring.class);

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
            return org.pangrova.coordinator.coordination.inmemory
                    .InMemoryQueryStateJournal.INSTANCE;
        }
        try {
            var config = new StateJournalConfig("JDBC", null, 100, 50, 24);
            return org.pangrova.coordinator.query.state.QueryStateJournalFactory.create(
                    config, dependencies.metadataDataSource());
        } catch (Exception failure) {
            LOG.warn("event=jdbc_journal_init_failed error={} fallback=InMemory",
                    failure.getMessage());
            return org.pangrova.coordinator.coordination.inmemory
                    .InMemoryQueryStateJournal.INSTANCE;
        }
    }

    QueryManagerResult buildQueryManager(
            QueryDispatcher queryDispatcher, QueryStateJournal stateJournal) {
        var auditEmitter = new AuditEventEmitter(new LoggingAuditEventWriter());
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
                new org.pangrova.planner.throttle.QueryThrottlingService(1000, 100.0, 50.0),
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
