package org.pangrova.coordinator.http;

import java.net.InetSocketAddress;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.ext.ContextResolver;
import org.pangrova.common.http.EmbeddedHttpServerFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.coordinator.http.endpoint.AuthenticationFilter;
import org.pangrova.coordinator.http.endpoint.CoordinatorExceptionMapper;
import org.pangrova.coordinator.http.endpoint.CoordinatorHealthResource;
import org.pangrova.coordinator.http.endpoint.CoordinatorTaskResource;
import org.pangrova.coordinator.http.endpoint.QueryResource;
import org.pangrova.coordinator.http.endpoint.RequestLimitFilter;
import org.pangrova.coordinator.http.endpoint.SplitResource;
import org.pangrova.coordinator.http.endpoint.StreamingResource;
import org.pangrova.coordinator.http.endpoint.TracingFilter;
import org.pangrova.coordinator.http.endpoint.WebUiResource;
import org.pangrova.coordinator.http.endpoint.WebUiStaticResource;
import org.pangrova.coordinator.http.endpoint.WorkerResource;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogDeploymentResource;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestOperations;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResource;
import org.pangrova.coordinator.http.endpoint.catalog.OAuth2TokenResource;
import org.pangrova.coordinator.http.endpoint.copy.CopySourceWireLoggingPolicy;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.observability.tracing.TracingHttpFilter;
import org.pangrova.operations.bootstrap.HealthCheckEndpoint;
import org.pangrova.security.authn.AuthenticationManager;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;

/**
 * Coordinator-side Jetty HTTP server lifecycle wrapper.
 * Uses Jersey ResourceConfig + ServletContainer for JAX-RS resource registration.
 */
public final class CoordinatorHttpServer {

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
        checkNotNull(deps, "deps must not be null");
        this.copySourceReadEnabled = deps.copySourceReadResource() != null;

        ResourceConfig config = buildResourceConfig(deps);
        ServletContainer servletContainer = new ServletContainer(config);

        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(servletContainer), "/*");

        this.server = EmbeddedHttpServerFactory.create(address, handler, COORDINATOR_MAX_THREADS);
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
        server.start();
    }

    /** Stops the Jetty server. */
    public void stop() throws Exception {
        server.stop();
    }

    /** Returns the actual bound port (useful when port=0 for random binding). */
    public int getPort() {
        return EmbeddedHttpServerFactory.getPort(server);
    }

    private static ResourceConfig buildResourceConfig(CoordinatorHttpServerContext deps) {
        ResourceConfig config = new ResourceConfig();

        registerResources(config, deps);
        registerFilters(config, deps);
        registerInfrastructure(config);

        return config;
    }

    private static void registerResources(ResourceConfig config, CoordinatorHttpServerContext deps) {
        config.register(new CoordinatorTaskResource(deps.receiver()));
        // A-CLIENT-01: one shared idempotency registry per coordinator so retried write submissions
        // carrying the same Idempotency-Key resolve to the same queryId instead of executing twice.
        IdempotencyKeyRegistry idempotencyRegistry = new IdempotencyKeyRegistry();
        registerIfPresent(config, deps.queryManager(),
                qm -> new QueryResource(qm, deps.profilerEndpoint(), deps.queryProfileService(),
                        idempotencyRegistry));
        registerIfPresent(config, deps.healthCheckEndpoint(), CoordinatorHealthResource::new);
        registerIfPresent(config, deps.workerManager(),
                manager -> new WorkerResource(
                        manager, null, deps.coordinatorEpochSupplier()));
        registerIfPresent(config, deps.cdcHandler(), StreamingResource::new);
        registerIfPresent(config, deps.splitAssignmentManager(), SplitResource::new);
        registerCatalogRestResources(config, deps);
        if (deps.copySourceReadResource() != null) {
            if (deps.authenticationManager() == null || deps.internalAuthSecret() == null
                    || deps.internalAuthSecret().isBlank()) {
                throw new IllegalArgumentException(
                        "COPY source-read endpoint requires internal-service authentication");
            }
            config.register(deps.copySourceReadResource());
        }
        registerWebUiResource(config, deps);
    }

    private static void registerWebUiResource(ResourceConfig config, CoordinatorHttpServerContext deps) {
        if (deps.queryManager() != null) {
            config.register(new WebUiResource(
                    deps.queryManager(), deps.workerManager(), deps.queryProfileService()));
            config.register(new WebUiStaticResource());
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

    private static void registerFilters(ResourceConfig config, CoordinatorHttpServerContext deps) {
        config.register(org.pangrova.coordinator.http.endpoint.RequestPeerAddressFilter.class);
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
                    deps.authenticationManager(), deps.internalAuthSecret()));
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
