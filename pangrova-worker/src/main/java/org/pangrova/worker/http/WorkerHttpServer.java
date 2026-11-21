package org.pangrova.worker.http;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.ext.ContextResolver;
import org.pangrova.transport.http.EmbeddedHttpServerFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.worker.http.endpoint.InternalAuthFilter;
import org.pangrova.worker.http.endpoint.WorkerExceptionMapper;
import org.pangrova.worker.http.endpoint.WorkerHealthResource;
import org.pangrova.worker.http.endpoint.TaskResource;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.common.lifecycle.HealthCheckEndpoint;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;

import java.nio.file.Path;
import java.util.EnumSet;

/**
 * Embedded HTTP server for Worker task operations and health checks.
 * Uses Jetty + Jersey for JAX-RS endpoint hosting.
 */
public final class WorkerHttpServer {

    static final int WORKER_MAX_THREADS = 64;

    private final Server server;
    private final ServerConnector copyTaskTlsConnector;
    private final TaskResource taskResource;
    private final org.pangrova.worker.dataset.NativeDatasetControlService datasetControl;
    private final org.pangrova.worker.copy.CopyStageLeases copyStageLeases;
    private final AutoCloseable fileSpaceClose;

    /** Worker HTTP server configuration. */
    public record WorkerHttpConfig(int port, String workerHost, int transportPort,
                                   String internalAuthSecret, String resultProtocol,
                                   CopyTaskTls copyTaskTls, String bindHost) {
        public WorkerHttpConfig {
            if (bindHost == null || bindHost.isBlank()) {
                throw new IllegalArgumentException("Worker HTTP bind host must be explicit");
            }
        }

        public WorkerHttpConfig(int port, String workerHost, int transportPort,
                                String internalAuthSecret, String resultProtocol, CopyTaskTls copyTaskTls) {
            this(port, workerHost, transportPort, internalAuthSecret, resultProtocol, copyTaskTls, "0.0.0.0");
        }

        /** Selects the listening interface independently of the advertised Worker identity. */
        public WorkerHttpConfig withBindHost(String host) {
            return new WorkerHttpConfig(port, workerHost, transportPort,
                    internalAuthSecret, resultProtocol, copyTaskTls, host);
        }

        public WorkerHttpConfig(int port, String workerHost, int transportPort,
                                String internalAuthSecret, String resultProtocol) {
            this(port, workerHost, transportPort, internalAuthSecret, resultProtocol, null);
        }
        /** Convenience constructor without auth secret (no auth). */
        public WorkerHttpConfig(int port, String workerHost, int transportPort) {
            this(port, workerHost, transportPort, null, "grpc");
        }

        public WorkerHttpConfig(
                int port, String workerHost, int transportPort, String internalAuthSecret) {
            this(port, workerHost, transportPort, internalAuthSecret, "grpc");
        }
    }

    /** Worker identity and trusted Coordinator CA for a dedicated private task listener. */
    public record CopyTaskTls(Path certificateChain, Path privateKey, Path rootCa, int port) {
        private static final int MAX_TCP_PORT = 65535;

        public CopyTaskTls(Path certificateChain, Path privateKey, Path rootCa) {
            this(certificateChain, privateKey, rootCa, 0);
        }

        public CopyTaskTls {
            if (port < 0 || port > MAX_TCP_PORT) {
                throw new IllegalArgumentException("COPY task TLS port must be in [0, 65535]");
            }
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
        this(config, taskManager, exchangeTransport, healthCheckEndpoint, null);
    }

    /** Creates a Worker server with its bounded Native dataset metadata control service. */
    public WorkerHttpServer(WorkerHttpConfig config, TaskManager taskManager,
                            ExchangeTransport exchangeTransport, HealthCheckEndpoint healthCheckEndpoint,
                            org.pangrova.worker.dataset.NativeDatasetControlService datasetControl) {
        this(config, taskManager, exchangeTransport, healthCheckEndpoint, datasetControl, null);
    }

    /** Installs process-local result authority without exposing the key to HTTP callers. */
    public WorkerHttpServer(WorkerHttpConfig config, TaskManager taskManager,
                            ExchangeTransport exchangeTransport, HealthCheckEndpoint healthCheckEndpoint,
                            org.pangrova.worker.dataset.NativeDatasetControlService datasetControl,
                            org.pangrova.worker.result.ResultReadTickets resultTickets) {
        this(config, taskManager, exchangeTransport, healthCheckEndpoint, datasetControl,
                resultTickets, null);
    }

    /** Optionally serves the separate pre-claim COPY staging operation. */
    public WorkerHttpServer(WorkerHttpConfig config, TaskManager taskManager,
                            ExchangeTransport exchangeTransport, HealthCheckEndpoint healthCheckEndpoint,
                            org.pangrova.worker.dataset.NativeDatasetControlService datasetControl,
                            org.pangrova.worker.result.ResultReadTickets resultTickets,
                            org.pangrova.worker.copy.CopyStageLeases copyStageLeases) {
        this(config, taskManager, exchangeTransport, healthCheckEndpoint, datasetControl,
                resultTickets, copyStageLeases, null);
    }

    /** Optionally serves authorized Volume and Stage file operations. */
    public WorkerHttpServer(WorkerHttpConfig config, TaskManager taskManager,
                            ExchangeTransport exchangeTransport, HealthCheckEndpoint healthCheckEndpoint,
                            org.pangrova.worker.dataset.NativeDatasetControlService datasetControl,
                            org.pangrova.worker.result.ResultReadTickets resultTickets,
                            org.pangrova.worker.copy.CopyStageLeases copyStageLeases,
                            org.pangrova.worker.file.FileSpaceFileResource fileSpaceResource) {
        checkNotNull(config, "config must not be null");
        checkNotNull(taskManager, "taskManager must not be null");
        checkNotNull(exchangeTransport, "exchangeTransport must not be null");
        this.datasetControl = datasetControl;
        this.copyStageLeases = copyStageLeases;
        // Jersey owns endpoint routing; the server retains only its destruction capability.
        this.fileSpaceClose = fileSpaceResource == null ? null : fileSpaceResource::close;
        if (copyStageLeases != null && copyStageLeases.supportsNativeInputs()) {
            taskManager.getSharedNativeExecutor()
                    .flatMap(org.pangrova.worker.execution.NativeTaskExecutor::ownerOf)
                    .ifPresent(nativeExecutor -> nativeExecutor.setCopyInputs(copyStageLeases));
        }
        this.taskResource = new TaskResource(
                taskManager, exchangeTransport,
                new org.pangrova.worker.exchange.source.ExchangeSourceRegistry(),
                config.workerHost(), config.transportPort(), null,
                new TaskResource.ResultBinding(config.resultProtocol(), resultTickets));
        ResourceConfig resourceConfig = buildResourceConfig(
                healthCheckEndpoint, taskResource, config.internalAuthSecret(), datasetControl,
                copyStageLeases == null ? null
                        : new org.pangrova.worker.http.endpoint.CopyStageResource(
                                taskManager, copyStageLeases),
                fileSpaceResource);
        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/");
        if (copyStageLeases != null) {
            handler.addFilter(new FilterHolder(new org.pangrova.worker.http.endpoint.CopyStageTlsFilter()),
                    "/v1/task/copy-stage", EnumSet.of(jakarta.servlet.DispatcherType.REQUEST));
        }
        ServletHolder servlet = new ServletHolder(servletContainer);
        // Complete routing and provider initialization before this node can be advertised.
        servlet.setInitOrder(0);
        handler.addServlet(servlet, "/*");
        this.server = EmbeddedHttpServerFactory.create(
                new java.net.InetSocketAddress(config.bindHost(), config.port()), handler, WORKER_MAX_THREADS);
        if (config.copyTaskTls() == null) {
            this.copyTaskTlsConnector = null;
        } else {
            CopyTaskWireLoggingPolicy.requireSafeJettyLogging();
            if (config.internalAuthSecret() == null || config.internalAuthSecret().isBlank()) {
                throw new IllegalArgumentException(
                        "COPY private task listener requires internal service authentication");
            }
            SslContextFactory.Server tls = new SslContextFactory.Server();
            tls.setSslContext(org.pangrova.transport.http.tls.PemMutualTls.context(
                    config.copyTaskTls().certificateChain(),
                    config.copyTaskTls().privateKey(), config.copyTaskTls().rootCa()));
            tls.setNeedClientAuth(true);
            tls.setIncludeProtocols("TLSv1.3", "TLSv1.2");
            HttpConfiguration https = new HttpConfiguration();
            https.addCustomizer(new SecureRequestCustomizer());
            this.copyTaskTlsConnector = new ServerConnector(server,
                    new SslConnectionFactory(tls, "http/1.1"),
                    new HttpConnectionFactory(https));
            this.copyTaskTlsConnector.setPort(config.copyTaskTls().port());
            server.addConnector(copyTaskTlsConnector);
        }
    }

    /** Starts the worker HTTP server and its connectors. */
    public void start() throws Exception {
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

    /** Stops the server and releases endpoint resources. */
    public void stop() throws Exception {
        Throwable failure = null;
        try {
            // Graceful drain requires a started context. A failed bind can leave the
            // context STOPPED; stop acquired components directly in that case.
            if (!server.isStarted()) server.setStopTimeout(0);
            server.stop();
        } catch (Exception | Error stopFailure) {
            failure = stopFailure;
        }
        for (AutoCloseable endpoint : new AutoCloseable[] {
                datasetControl, taskResource, copyStageLeases, fileSpaceClose}) {
            if (endpoint == null) continue;
            try {
                endpoint.close();
            } catch (Exception | Error closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else if (failure != closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure instanceof Exception exception) throw exception;
        if (failure instanceof Error error) throw error;
    }

    public int getPort() {
        return EmbeddedHttpServerFactory.getPort(server);
    }

    /** Zero means no private mTLS task listener is configured. */
    public int getCopyTaskTlsPort() {
        return copyTaskTlsConnector == null ? 0 : copyTaskTlsConnector.getLocalPort();
    }

    private static ResourceConfig buildResourceConfig(
            HealthCheckEndpoint healthCheckEndpoint, TaskResource taskResource,
            String internalAuthSecret, org.pangrova.worker.dataset.NativeDatasetControlService datasetControl,
            org.pangrova.worker.http.endpoint.CopyStageResource copyStageResource,
            org.pangrova.worker.file.FileSpaceFileResource fileSpaceResource) {
        ResourceConfig rc = new ResourceConfig();
        rc.register(taskResource);
        if (copyStageResource != null) rc.register(copyStageResource);
        if (fileSpaceResource != null) rc.register(fileSpaceResource);
        if (datasetControl != null) {
            rc.register(new org.pangrova.worker.http.endpoint.NativeDatasetControlResource(datasetControl));
        }
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
