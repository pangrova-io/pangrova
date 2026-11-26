package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.enums.ClusterNodeType;
import org.pangrova.server.coordinator.http.CoordinatorHttpServer;
import org.pangrova.common.exception.BootstrapFailedException;
import org.pangrova.server.coordinator.http.CoordinatorHttpServerContext;
import org.pangrova.server.coordinator.http.endpoint.QueryPeerRouting;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.query.runtime.QueryManager;
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
    private final MetadataService metadataService;

    private CoordinatorHttpServer httpServer;
    private QueryPeerRouting queryPeers;

    HttpBootstrap(CoordinatorBootstrapConfig config,
                  TracingProvider tracingProvider,
                  AuthenticationManager authenticationManager,
                  TaskStatusReceiver taskStatusReceiver,
                  QueryBootstrap queryBootstrap,
                  WorkerManager workerManager,
                  CdcLifecycleManager cdcLifecycleManager,
                  CoordinatorBootstrap coordinatorBootstrap,
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
        this.metadataService = metadataService;
    }

    int bind() {
        try {
            TracingHttpFilter tracingFilter = buildTracingFilter();
            HealthCheckEndpoint healthEndpoint = buildHealthEndpoint();
            ProfilerEndpoint profilerEndpoint = buildProfilerEndpoint();
            var httpDeps = new CoordinatorHttpServerContext(
                    taskStatusReceiver, queryManager, tracingFilter,
                    authenticationManager, healthEndpoint, workerManager,
                    profilerEndpoint, cdcLifecycleManager, queryProfileService, metadataService,
                    config.internalAuthSecret(),
                    coordinatorBootstrap::currentCoordinatorEpoch,
                    coordinatorBootstrap.copySourceReadResource(),
                    coordinatorBootstrap.fileSpaceFilesResource());
            String peerKey = config.securityConfig() == null ? null : config.securityConfig().queryPeerKey();
            if (peerKey != null) {
                queryPeers = new QueryPeerRouting(peerKey);
            }
            httpServer = new CoordinatorHttpServer(
                    new java.net.InetSocketAddress(config.httpHost(), config.httpPort()), httpDeps,
                    coordinatorBootstrap::queryOwnerEndpoint, queryPeers);
            httpServer.bind();
            return httpServer.getPort();
        } catch (Exception e) {
            throw new BootstrapFailedException("HTTP bind", e);
        }
    }

    void start() {
        try {
            httpServer.start();
            LOG.info("Phase: HTTP — listening on port {}", httpServer.getPort());
        } catch (Exception e) {
            throw new BootstrapFailedException("HTTP", e);
        }
    }

    void shutdown() {
        ResourceCloser.closeAll(null, () -> CoordinatorShutdownSequence.stopHttp(httpServer),
                ResourceCloser.present(queryPeers, QueryPeerRouting::close));
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
