package org.pangrova.worker.bootstrap.phase;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.common.exception.BootstrapFailedException;
import org.pangrova.common.lifecycle.HealthCheckEndpoint;
import org.pangrova.security.api.audit.AuditEventWriter;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.worker.PangrovaWorker;
import org.pangrova.worker.WorkerComponents;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.lifecycle.WorkerShutdownSequence;
import org.pangrova.worker.bootstrap.lifecycle.WorkerHealthConditions;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.http.WorkerHttpServer.WorkerHttpConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages Worker HTTP server lifecycle: Jetty startup, JAX-RS resource registration, health
 * endpoint, and tracing filter.
 */
public final class WorkerHttpBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerHttpBootstrap.class);

    /**
     * Bind addresses that must never be advertised to remote peers: a wildcard tells the OS "listen
     * on every interface" but is not routable, so a peer that dials it cannot reach this worker.
     */
    private static final java.util.Set<String> WILDCARD_ADDRESSES =
            java.util.Set.of("0.0.0.0", "::", "[::]", "::0", "0:0:0:0:0:0:0:0", "*");

    private WorkerHttpBootstrap() {}

    /** Creates and starts the Worker HTTP server with task/exchange JAX-RS resources. */
    public static void startHttp(
            WorkerBootstrapConfig config,
            WorkerComponentRefs refs,
            WorkerObservabilitySetup observability,
            AtomicBoolean started, AuditEventWriter fileAudit) {
        try {
            int resultPort = refs.resultServerPort();
            String workerHost = resolveAdvertisedHost(config);
            String resultProtocol =
                    config.transportSecurityConfig().enabled()
                            ? org.pangrova.common.client.enums.ResultProtocol.GRPC_TLS.value()
                            : org.pangrova.common.client.enums.ResultProtocol.GRPC.value();
            WorkerHttpConfig httpConfig =
                    new WorkerHttpConfig(
                            config.httpPort(),
                            workerHost,
                            resultPort,
                            config.internalAuthSecret(),
                            resultProtocol,
                            copyTaskTls(config.transportSecurityConfig()));
            HealthCheckEndpoint healthEndpoint = buildHealthEndpoint(started, refs);
            ExchangeTransport exchangeTransport = refs.exchangeTransport();
            var control = datasetControl(refs);
            var stageLeases = copyStageLeases(config);
            var fileSpace = buildFileSpaceResource(config, fileAudit);
            WorkerHttpServer server;
            try {
                server = new WorkerHttpServer(
                            httpConfig,
                            refs.taskManager(),
                            exchangeTransport,
                            healthEndpoint,
                            control,
                            new org.pangrova.worker.result.ResultReadTickets(
                                    refs.shuffleHmacCurrentKey()),
                            stageLeases,
                            fileSpace);
            } catch (RuntimeException | Error failure) {
                if (stageLeases != null) {
                    try {
                        stageLeases.close();
                    } catch (Exception cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                }
                if (fileSpace != null) {
                    try {
                        fileSpace.close();
                    } catch (Exception cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                }
                throw failure;
            }
            try {
                server.start();
            } catch (Exception failure) {
                try {
                    server.stop();
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
            refs.setHttpServer(server);
            refs.setWorker(new PangrovaWorker(new WorkerComponents(refs.taskManager())));
            LOG.info(
                    "event=phase_started phase=HTTP port={} workerHost={}",
                    server.getPort(),
                    workerHost);
        } catch (Exception e) {
            throw new BootstrapFailedException("HTTP", e);
        }
    }

    /**
     * The public file plane is served only when descriptors can be authenticated; without the
     * shared secret there is no way to tell a coordinator-issued descriptor from a forged one.
     */
    private static org.pangrova.worker.file.FileSpaceFileResource buildFileSpaceResource(
            WorkerBootstrapConfig config, AuditEventWriter fileAudit) {
        String secret = config.internalAuthSecret();
        if (secret == null || secret.isBlank()) {
            LOG.warn("event=file_space_disabled reason=internal_auth_secret_missing");
            return null;
        }
        return new org.pangrova.worker.file.FileSpaceFileResource(
                org.pangrova.worker.file.ObjectStoreAccessorFactory.discover(), secret, fileAudit);
    }

    private static org.pangrova.worker.copy.CopyStageLeases copyStageLeases(            WorkerBootstrapConfig config) {
        if (config.transportSecurityConfig().copyTaskTlsPort() == 0) return null;
        var file = config.nativeFunctionConfig().configForPlugin("ai-inference");
        if (!"s3".equals(file.get("file.storage-backend"))) {
            throw new IllegalArgumentException(
                    "COPY stage listener requires managed S3 FILE storage");
        }
        String stagingRoot = file.get("file.storage-staging-root");
        if (stagingRoot == null || stagingRoot.isBlank()) {
            throw new IllegalArgumentException("COPY stage listener requires a staging root");
        }
        var storage = org.pangrova.filesystem.s3.blob.S3BlobStorage.forCopyStaging(
                org.pangrova.filesystem.s3.blob.S3BlobStorageSettingsParser.parse(file));
        try {
            var command = new org.pangrova.worker.copy.CopySourceStagingCommand(
                    new org.pangrova.worker.copy.CopySourceReadClient(
                            config.coordinatorUrl(), config.internalAuthSecret()),
                    storage, java.nio.file.Path.of(stagingRoot).resolve("copy-source"),
                    1024L * 1024 * 1024);
            return new org.pangrova.worker.copy.CopyStageLeases(command, storage);
        } catch (RuntimeException failure) {
            storage.close();
            throw failure;
        }
    }

    /** Flight TLS remains independent of the explicitly enabled private COPY listener. */
    static WorkerHttpServer.CopyTaskTls copyTaskTls(
            WorkerBootstrapConfig.TransportSecurityConfig security) {
        if (security.copyTaskTlsPort() == 0) return null;
        return new WorkerHttpServer.CopyTaskTls(
                java.nio.file.Path.of(security.certificateChainPath()),
                java.nio.file.Path.of(security.privateKeyPath()),
                java.nio.file.Path.of(security.rootCaPath()),
                security.copyTaskTlsPort());
    }

    private static org.pangrova.worker.dataset.NativeDatasetControlService datasetControl(
            WorkerComponentRefs refs) {
        if (refs.catalogResourceResolver()
                instanceof org.pangrova.worker.catalog.WorkerCatalogRuntime catalogs) {
            return new org.pangrova.worker.dataset.NativeDatasetControlService(
                    refs.taskManager(), catalogs);
        }
        return null;
    }

    /**
     * Resolves the address this worker advertises to the coordinator and to remote peers
     * (shuffle/result fetching).
     *
     * <p>Priority: {@code worker.advertised-host} → {@code worker.host} when it is a concrete
     * (non-wildcard) address → {@code InetAddress.getLocalHost().getHostName()} → {@code
     * "localhost"}.
     *
     * <p>{@code worker.host} is a bind address whose default is {@code 0.0.0.0}; it is deliberately
     * skipped when it is a wildcard, because advertising a wildcard leaves peers with an address
     * they cannot dial.
     */
    static String resolveAdvertisedHost(WorkerBootstrapConfig config) {
        String advertised = config.advertisedHost();
        if (advertised != null && !advertised.isBlank()) {
            LOG.info(
                    "event=worker_advertised_host_resolved source=config host={}",
                    advertised.trim());
            return advertised.trim();
        }
        String bindHost = config.httpHost();
        if (bindHost != null && !bindHost.isBlank() && !isWildcardAddress(bindHost)) {
            LOG.info(
                    "event=worker_advertised_host_resolved source=bind_host host={}",
                    bindHost.trim());
            return bindHost.trim();
        }
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            LOG.info(
                    "event=worker_advertised_host_resolved source=InetAddress host={} "
                            + "detail=set_worker.advertised-host_explicitly_behind_NAT",
                    hostname);
            return hostname;
        } catch (Exception e) {
            LOG.warn(
                    "event=worker_advertised_host_resolve_failed error={} fallback=localhost",
                    e.getMessage());
            return "localhost";
        }
    }

    /** Returns true when {@code host} is a wildcard bind address that must not be advertised. */
    static boolean isWildcardAddress(String host) {
        return host != null && WILDCARD_ADDRESSES.contains(host.trim());
    }

    /** Stops the Worker HTTP server (delegated to WorkerShutdownSequence). */
    public static void stopHttp(WorkerComponentRefs refs) {
        WorkerShutdownSequence.stopHttp(refs.httpServer());
    }

    static HealthCheckEndpoint buildHealthEndpoint(
            AtomicBoolean started, WorkerComponentRefs refs) {
        return new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.WORKER, new WorkerHealthConditions(started, refs));
    }
}
