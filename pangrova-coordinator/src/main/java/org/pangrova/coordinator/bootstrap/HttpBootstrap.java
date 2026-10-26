package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.operations.bootstrap.BootstrapFailedException;
import org.pangrova.coordinator.http.CoordinatorHttpServerContext;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.service.MetadataService;
import org.pangrova.observability.audit.LogAuditStore;
import org.pangrova.observability.audit.QueryAuditService;
import org.pangrova.observability.noop.DisabledNativeProfiler;
import org.pangrova.observability.profiling.ProfilerEndpoint;
import org.pangrova.observability.profiling.QueryProfileService;
import org.pangrova.observability.tracing.TracingHttpFilter;
import org.pangrova.operations.bootstrap.HealthCheckEndpoint;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.spi.streaming.CdcLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bootstraps the HTTP server with query, admin, and health endpoints.
 */
final class HttpBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(HttpBootstrap.class);

    private final CoordinatorBootstrapConfig config;
    private final TracingProvider tracingProvider;
    private final AuthenticationManager authenticationManager;
    private final TaskStatusReceiver taskStatusReceiver;
    private final QueryManager queryManager;
    private final WorkerManager workerManager;
    private final CdcLifecycleManager cdcLifecycleManager;
    private final CoordinatorBootstrap coordinatorBootstrap;
    private final SplitAssignmentManager splitAssignmentManager;
    private final MetadataService metadataService;

    private CoordinatorHttpServer httpServer;

    HttpBootstrap(CoordinatorBootstrapConfig config,
                  TracingProvider tracingProvider,
                  AuthenticationManager authenticationManager,
                  TaskStatusReceiver taskStatusReceiver,
                  QueryManager queryManager,
                  WorkerManager workerManager,
                  CdcLifecycleManager cdcLifecycleManager,
                  CoordinatorBootstrap coordinatorBootstrap,
                  SplitAssignmentManager splitAssignmentManager,
                  MetadataService metadataService) {
        this.config = config;
        this.tracingProvider = tracingProvider;
        this.authenticationManager = authenticationManager;
        this.taskStatusReceiver = taskStatusReceiver;
        this.queryManager = queryManager;
        this.workerManager = workerManager;
        this.cdcLifecycleManager = cdcLifecycleManager;
        this.coordinatorBootstrap = coordinatorBootstrap;
        this.splitAssignmentManager = splitAssignmentManager;
        this.metadataService = metadataService;
    }

    void start() {
        try {
            TracingHttpFilter tracingFilter = buildTracingFilter();
            HealthCheckEndpoint healthEndpoint = buildHealthEndpoint();
            ProfilerEndpoint profilerEndpoint = buildProfilerEndpoint();
            QueryProfileService queryProfileService = buildQueryProfileService();
            var httpDeps = new CoordinatorHttpServerContext(
                    taskStatusReceiver, queryManager, tracingFilter,
                    authenticationManager, healthEndpoint, workerManager,
                    profilerEndpoint, cdcLifecycleManager, splitAssignmentManager,
                    queryProfileService, metadataService, config.internalAuthSecret(),
                    coordinatorBootstrap::currentCoordinatorEpoch,
                    coordinatorBootstrap.copySourceReadResource(),
                    coordinatorBootstrap.fileSpaceFilesResource());
            httpServer = new CoordinatorHttpServer(
                    new java.net.InetSocketAddress(config.httpHost(), config.httpPort()), httpDeps);
            httpServer.start();
            LOG.info("Phase: HTTP — listening on port {}", httpServer.getPort());
        } catch (Exception e) {
            throw new BootstrapFailedException("HTTP", e);
        }
    }

    void shutdown() {
        CoordinatorShutdownSequence.stopHttp(httpServer);
    }

    private TracingHttpFilter buildTracingFilter() {
        return new TracingHttpFilter(tracingProvider);
    }

    private HealthCheckEndpoint buildHealthEndpoint() {
        return new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.COORDINATOR,
                new CoordinatorHealthConditions(coordinatorBootstrap));
    }

    private static ProfilerEndpoint buildProfilerEndpoint() {
        return new ProfilerEndpoint(new DisabledNativeProfiler());
    }

    private static QueryProfileService buildQueryProfileService() {
        return new QueryProfileService(new QueryAuditService(LogAuditStore.shared()));
    }

    CoordinatorHttpServer httpServer() { return httpServer; }
}
