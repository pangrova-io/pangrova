package org.pangrova.worker.http;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.ext.ContextResolver;
import org.pangrova.common.http.EmbeddedHttpServerFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.worker.http.endpoint.InternalAuthFilter;
import org.pangrova.worker.http.endpoint.WorkerExceptionMapper;
import org.pangrova.worker.http.endpoint.WorkerHealthResource;
import org.pangrova.worker.http.endpoint.TaskResource;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.operations.bootstrap.HealthCheckEndpoint;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embedded HTTP server for Worker task operations and health checks.
 * Uses Jetty + Jersey for JAX-RS endpoint hosting.
 */
public class WorkerHttpServer {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerHttpServer.class);
    static final int WORKER_MAX_THREADS = 64;

    private final Server server;
    private final TaskResource taskResource;

    /** Worker HTTP server configuration. */
    public record WorkerHttpConfig(int port, String workerHost, int transportPort,
                                   String internalAuthSecret) {
        /** Convenience constructor without auth secret (no auth). */
        public WorkerHttpConfig(int port, String workerHost, int transportPort) {
            this(port, workerHost, transportPort, null);
        }
    }

    public WorkerHttpServer(WorkerHttpConfig config, TaskManager taskManager,
                            ExchangeTransport exchangeTransport) {
        this(config, taskManager, exchangeTransport, null);
    }

    /** Creates a server with an explicit health check endpoint. */
    public WorkerHttpServer(WorkerHttpConfig config, TaskManager taskManager,
                            ExchangeTransport exchangeTransport,
                            HealthCheckEndpoint healthCheckEndpoint) {
        checkNotNull(config, "config must not be null");
        checkNotNull(taskManager, "taskManager must not be null");
        checkNotNull(exchangeTransport, "exchangeTransport must not be null");
        this.taskResource = new TaskResource(
                taskManager, exchangeTransport,
                new org.pangrova.worker.exchange.source.ExchangeSourceRegistry(),
                config.workerHost(), config.transportPort(), null,
                taskManager.getVectorResultBuffer());
        ResourceConfig resourceConfig = buildResourceConfig(
                healthCheckEndpoint, taskResource, config.internalAuthSecret());
        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(servletContainer), "/*");
        this.server = EmbeddedHttpServerFactory.create(
                config.port(), handler, WORKER_MAX_THREADS);
    }

    public void start() throws Exception {
        server.start();
    }

    /** Stops the server and releases endpoint resources. */
    public void stop() throws Exception {
        server.stop();
        if (taskResource != null) {
            try {
                taskResource.close();
            } catch (Exception e) {
                LOG.warn("event=task_resource_close_error error={}", e.getMessage());
            }
        }
    }

    public int getPort() {
        return EmbeddedHttpServerFactory.getPort(server);
    }

    private static ResourceConfig buildResourceConfig(
            HealthCheckEndpoint healthCheckEndpoint, TaskResource taskResource,
            String internalAuthSecret) {
        ResourceConfig rc = new ResourceConfig();
        rc.register(taskResource);
        if (healthCheckEndpoint != null) {
            rc.register(new WorkerHealthResource(healthCheckEndpoint));
        }
        if (internalAuthSecret != null && !internalAuthSecret.isBlank()) {
            rc.register(new InternalAuthFilter(internalAuthSecret));
        }
        rc.register(WorkerExceptionMapper.class);
        rc.register(JacksonFeature.class);
        rc.register(new ObjectMapperResolver());
        rc.property(ServerProperties.WADL_FEATURE_DISABLE, true);
        return rc;
    }

    private static final class ObjectMapperResolver implements ContextResolver<ObjectMapper> {
        private final ObjectMapper mapper = JsonMapperFactory.create();

        @Override
        public ObjectMapper getContext(Class<?> type) {
            return mapper;
        }
    }
}
