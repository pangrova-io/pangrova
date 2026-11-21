package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.enums.ClusterNodeType;
import org.pangrova.server.coordinator.http.CoordinatorHttpServer;
import org.pangrova.common.exception.BootstrapFailedException;
import org.pangrova.server.coordinator.http.CoordinatorHttpServerContext;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.api.resolution.MetadataService;
import org.pangrova.observability.noop.UnavailableProfiler;
import org.pangrova.observability.profiling.ProfilerEndpoint;
import org.pangrova.query.profiling.QueryProfileService;
import org.pangrova.observability.sdk.tracing.http.TracingHttpFilter;
import org.pangrova.common.lifecycle.HealthCheckEndpoint;
import org.pangrova.security.authentication.AuthenticationManager;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.streaming.api.cdc.CdcLifecycleManager;
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
    private final QueryProfileService queryProfileService;
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
                  QueryBootstrap queryBootstrap,
                  WorkerManager workerManager,
                  CdcLifecycleManager cdcLifecycleManager,
                  CoordinatorBootstrap coordinatorBootstrap,
                  SplitAssignmentManager splitAssignmentManager,
                  MetadataService metadataService) {
        this.config = config;
        this.tracingProvider = tracingProvider;
        this.authenticationManager = authenticationManager;
        this.taskStatusReceiver = taskStatusReceiver;
        this.queryManager = queryBootstrap.queryManager();
        this.queryProfileService = queryBootstrap.queryAuditServices().profileService();
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
                ClusterNodeType.COORDINATOR,
                new CoordinatorHealthConditions(coordinatorBootstrap));
    }

    private static ProfilerEndpoint buildProfilerEndpoint() {
        return new ProfilerEndpoint(new UnavailableProfiler());
    }

    CoordinatorHttpServer httpServer() { return httpServer; }
}
