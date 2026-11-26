package org.pangrova.server.coordinator.http;

import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;

import org.pangrova.server.coordinator.http.request.RequestLimitConfig;

import java.net.InetSocketAddress;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.ext.ContextResolver;
import org.pangrova.transport.http.EmbeddedHttpServerFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.server.coordinator.http.endpoint.AuthenticationFilter;
import org.pangrova.server.coordinator.http.endpoint.CoordinatorExceptionMapper;
import org.pangrova.server.coordinator.http.request.QueryStatusWaits;
import org.pangrova.server.coordinator.http.endpoint.CoordinatorHealthResource;
import org.pangrova.server.coordinator.http.endpoint.CoordinatorTaskResource;
import org.pangrova.server.coordinator.http.endpoint.QueryResource;
import org.pangrova.server.coordinator.http.endpoint.QueryPeerResource;
import org.pangrova.server.coordinator.http.endpoint.QueryPeerRouting;
import org.pangrova.server.coordinator.http.endpoint.RequestLimitFilter;
import org.pangrova.server.coordinator.http.endpoint.StreamingResource;
import org.pangrova.server.coordinator.http.endpoint.TracingFilter;
import org.pangrova.server.coordinator.http.endpoint.WebUiResource;
import org.pangrova.server.coordinator.http.endpoint.WebUiStaticResource;
import org.pangrova.server.coordinator.http.endpoint.WorkerResource;
import org.pangrova.server.coordinator.http.endpoint.catalog.CatalogDeploymentResource;
import org.pangrova.server.coordinator.http.endpoint.catalog.CatalogRestOperations;
import org.pangrova.server.coordinator.http.endpoint.catalog.CatalogRestResource;
import org.pangrova.server.coordinator.http.endpoint.catalog.OAuth2TokenResource;
import org.pangrova.server.coordinator.http.endpoint.copy.CopySourceWireLoggingPolicy;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.observability.sdk.tracing.http.TracingHttpFilter;
import org.pangrova.common.lifecycle.HealthCheckEndpoint;
import org.pangrova.common.client.query.QueryOwnerEndpoint;
import org.pangrova.security.authentication.AuthenticationManager;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;

import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Coordinator-side Jetty HTTP server lifecycle wrapper.
 * Uses Jersey ResourceConfig + ServletContainer for JAX-RS resource registration.
 */
public final class CoordinatorHttpServer {
    private volatile QueryOwnerEndpoint queryOwnerEndpoint;

    /** Max concurrent request threads for Coordinator (Requirement 50.4). */
    static final int COORDINATOR_MAX_THREADS = 256;

    private final Server server;
    private final boolean copySourceReadEnabled;

    /**
     * Primary constructor accepting a dependencies parameter object.
     * Builds a Jersey ResourceConfig, registers all JAX-RS resources/filters,
     * and mounts via ServletContainer on the Jetty context handler.
     *
     * @param port bind port (0 for random)
     * @param deps grouped dependencies for HTTP server wiring
     */
    public CoordinatorHttpServer(int port, CoordinatorHttpServerContext deps) {
        this(new InetSocketAddress(port), deps);
    }

    /** Binds the HTTP service to the exact address used by its advertised endpoint. */
    public CoordinatorHttpServer(InetSocketAddress address, CoordinatorHttpServerContext deps) {
        this(address, deps, null, null);
    }

    /** Issues query owner locations using the actual bound HTTP port. */
    public CoordinatorHttpServer(InetSocketAddress address, CoordinatorHttpServerContext deps,
            IntFunction<QueryOwnerEndpoint> ownerEndpointFactory, QueryPeerRouting queryPeers) {
        checkNotNull(deps, "deps must not be null");
        if (queryPeers != null && (ownerEndpointFactory == null || deps.queryManager() == null
                || deps.authenticationManager() == null)) {
            throw new IllegalArgumentException(
                    "Query peer routing requires a local query authority and authentication");
        }
        this.copySourceReadEnabled = deps.copySourceReadResource() != null;

        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/");
        this.server = EmbeddedHttpServerFactory.create(address, handler, COORDINATOR_MAX_THREADS);

        var statusWaits = new QueryStatusWaits(server.getThreadPool());
        ResourceConfig config = buildResourceConfig(deps, statusWaits,
                () -> getQueryOwnerEndpoint(ownerEndpointFactory), queryPeers);
        handler.addServlet(new ServletHolder(new ServletContainer(config)), "/*");
    }

    /**
     * Convenience constructor for callers that pass individual parameters.
     * Delegates to the primary constructor via CoordinatorHttpServerDependencies.
     */
    public CoordinatorHttpServer(int port, TaskStatusReceiver receiver,
                                  QueryManager queryManager, TracingHttpFilter tracingHttpFilter) {
        this(port, new CoordinatorHttpServerContext(
                receiver, queryManager, tracingHttpFilter, null, null, null, null, null));
    }

    /**
     * Convenience constructor with authentication support.
     */
    public CoordinatorHttpServer(int port, TaskStatusReceiver receiver,
                                  QueryManager queryManager, TracingHttpFilter tracingHttpFilter,
                                  AuthenticationManager authenticationManager) {
        this(port, new CoordinatorHttpServerContext(
                receiver, queryManager, tracingHttpFilter, authenticationManager,
                null, null, null, null));
    }

    /**
     * Convenience constructor with health check endpoint.
     */
    public CoordinatorHttpServer(int port, TaskStatusReceiver receiver,
                                  QueryManager queryManager, TracingHttpFilter tracingHttpFilter,
                                  AuthenticationManager authenticationManager,
                                  HealthCheckEndpoint healthCheckEndpoint) {
        this(port, new CoordinatorHttpServerContext(
                receiver, queryManager, tracingHttpFilter, authenticationManager,
                healthCheckEndpoint, null, null, null));
    }

    /** Starts the Jetty server. */
    public void start() throws Exception {
        if (copySourceReadEnabled) {
            CopySourceWireLoggingPolicy.requireSafeJettyLogging();
        }
        try {
            server.start();
        } catch (Exception | Error startupFailure) {
            try {
                stop();
            } catch (Exception | Error closeFailure) {
                if (startupFailure != closeFailure) startupFailure.addSuppressed(closeFailure);
            }
            throw startupFailure;
        }
    }

    /** Stops the Jetty server, including resources acquired during a failed start. */
    public void stop() throws Exception {
        // A failed bind has no started servlet context to drain gracefully.
        if (!server.isStarted()) server.setStopTimeout(0);
        server.stop();
    }

    /** Returns the actual bound port (useful when port=0 for random binding). */
    public int getPort() {
        return EmbeddedHttpServerFactory.getPort(server);
    }

    private QueryOwnerEndpoint getQueryOwnerEndpoint(IntFunction<QueryOwnerEndpoint> ownerEndpointFactory) {
        if (ownerEndpointFactory == null) return null;
        var endpoint = queryOwnerEndpoint;
        if (endpoint == null) {
            endpoint = ownerEndpointFactory.apply(getPort());
            queryOwnerEndpoint = endpoint;
        }
        return endpoint;
    }

    private static ResourceConfig buildResourceConfig(CoordinatorHttpServerContext deps, QueryStatusWaits statusWaits,
            Supplier<QueryOwnerEndpoint> ownerEndpoint, QueryPeerRouting queryPeers) {
        ResourceConfig config = new ResourceConfig();

        registerResources(config, deps, statusWaits, ownerEndpoint, queryPeers);
        registerFilters(config, deps, queryPeers != null);
        registerInfrastructure(config);

        return config;
    }

    private static void registerResources(ResourceConfig config, CoordinatorHttpServerContext deps,
            QueryStatusWaits statusWaits, Supplier<QueryOwnerEndpoint> ownerEndpoint, QueryPeerRouting queryPeers) {
        config.register(new org.pangrova.server.coordinator.http.endpoint.ServerInfoResource(
                CoordinatorHttpServer.class.getPackage().getImplementationVersion()));
        config.register(new CoordinatorTaskResource(deps.receiver()));
        registerIfPresent(config, deps.healthCheckEndpoint(), CoordinatorHealthResource::new);
        registerIfPresent(config, deps.workerManager(),
                manager -> new WorkerResource(manager, deps.coordinatorEpochSupplier()));
        registerIfPresent(config, deps.cdcHandler(), StreamingResource::new);
        registerCatalogRestResources(config, deps);
        if (deps.copySourceReadResource() != null) {
            if (deps.authenticationManager() == null || deps.internalAuthSecret() == null
                    || deps.internalAuthSecret().isBlank()) {
                throw new IllegalArgumentException(
                        "COPY source-read endpoint requires internal-service authentication");
            }
            config.register(deps.copySourceReadResource());
        }
        if (deps.fileSpaceFilesResource() != null) {
            if (deps.authenticationManager() == null) {
                throw new IllegalArgumentException(
                        "Files API endpoint requires user authentication");
            }
            config.register(deps.fileSpaceFilesResource());
        }
        registerQueryResources(config, deps, statusWaits, ownerEndpoint, queryPeers);
    }

    private static void registerQueryResources(ResourceConfig config, CoordinatorHttpServerContext deps,
            QueryStatusWaits statusWaits, Supplier<QueryOwnerEndpoint> ownerEndpoint, QueryPeerRouting queryPeers) {
        if (deps.queryManager() != null) {
            var queries = new QueryResource(deps.queryManager(), deps.profilerEndpoint(), deps.queryProfileService(),
                    new OtelExecutionTraceContextFactory(), statusWaits, ownerEndpoint, queryPeers);
            config.register(queries);
            config.register(new WebUiResource(
                    deps.queryManager(), deps.workerManager(), deps.queryProfileService(), queries));
            config.register(new WebUiStaticResource());
            if (queryPeers != null) {
                var peer = new QueryPeerResource(queries, queryPeers, ownerEndpoint);
                config.register(peer);
                config.register((jakarta.ws.rs.container.DynamicFeature) (resource, registration) -> {
                    if (resource.getResourceClass() == QueryPeerResource.class) {
                        registration.register((jakarta.ws.rs.container.ContainerResponseFilter) peer::signResponse,
                                jakarta.ws.rs.container.ContainerResponseFilter.class);
                    }
                }, jakarta.ws.rs.container.DynamicFeature.class);
            }
        }
    }

    private static void registerCatalogRestResources(ResourceConfig config, CoordinatorHttpServerContext deps) {
        if (deps.metadataService() != null) {
            CatalogRestOperations operations = new CatalogRestOperations(deps.metadataService());
            config.register(new CatalogRestResource(operations));
            config.register(new OAuth2TokenResource(deps.authenticationManager()));
            if (deps.internalAuthSecret() != null && !deps.internalAuthSecret().isBlank()) {
                config.register(new CatalogDeploymentResource(deps.metadataService()));
            }
        }
    }

    private static void registerFilters(ResourceConfig config, CoordinatorHttpServerContext deps,
            boolean queryPeerEnabled) {
        config.register(org.pangrova.server.coordinator.http.endpoint.ApiVersionFilter.class);
        config.register(org.pangrova.server.coordinator.http.endpoint.RequestPeerAddressFilter.class);
        if (deps.workerManager() != null && (deps.authenticationManager() == null
                || deps.internalAuthSecret() == null || deps.internalAuthSecret().isBlank())) {
            throw new IllegalArgumentException("Worker control requires configured internal-service authentication");
        }
        int maxRequestsPerMinute = Integer.parseInt(
                System.getProperty("pangrova.http.max-requests-per-minute",
                        String.valueOf(RequestLimitConfig.DEFAULT_MAX_REQUESTS_PER_MINUTE)));
        config.register(new RequestLimitFilter(new RequestLimitConfig(
                RequestLimitConfig.DEFAULT_MAX_REQUEST_SIZE_BYTES, maxRequestsPerMinute)));
        if (deps.authenticationManager() != null) {
            config.register(new AuthenticationFilter(
                    deps.authenticationManager(), deps.internalAuthSecret(), queryPeerEnabled));
        }
        registerIfPresent(config, deps.tracingHttpFilter(), TracingFilter::new);
    }

    private static void registerInfrastructure(ResourceConfig config) {
        config.register(CoordinatorExceptionMapper.class);
        config.register(JacksonFeature.class);
        config.register(new ObjectMapperResolver());
        config.property(ServerProperties.WADL_FEATURE_DISABLE, true);
    }

    private static <T> void registerIfPresent(ResourceConfig config, T dep,
                                               java.util.function.Function<T, Object> factory) {
        if (dep != null) {
            config.register(factory.apply(dep));
        }
    }

    /**
     * JAX-RS ContextResolver that provides the shared ObjectMapper from JsonMapperFactory.
     * Ensures consistent JSON serialization across all JAX-RS endpoints.
     */
    private static final class ObjectMapperResolver implements ContextResolver<ObjectMapper> {
        private final ObjectMapper mapper = JsonMapperFactory.create();

        @Override
        public ObjectMapper getContext(Class<?> type) {
            return mapper;
        }
    }
}
