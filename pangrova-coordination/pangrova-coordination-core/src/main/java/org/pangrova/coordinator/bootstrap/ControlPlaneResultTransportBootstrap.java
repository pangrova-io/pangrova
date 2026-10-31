package org.pangrova.coordinator.bootstrap;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.pangrova.coordinator.result.CoordinatorDataProducer;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.result.CoordinatorResultWriter;
import org.pangrova.client.core.protocol.ResultEndpoint;
import org.pangrova.client.core.protocol.ResultProtocol;
import org.pangrova.observability.api.TracingProvider;
import org.pangrova.protocol.transport.TransportServerConfig;
import org.pangrova.protocol.transport.TransportServerLifecycle;
import org.pangrova.worker.transport.TransportServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts the bounded Coordinator acknowledgement transport, never query rows. */
final class ControlPlaneResultTransportBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(
            ControlPlaneResultTransportBootstrap.class);

    private ControlPlaneResultTransportBootstrap() {}

    static Started start(CoordinatorBootstrapConfig config, TracingProvider tracing,
                         ScheduledExecutorService scheduler) {
        CoordinatorPreflightChecks.validateAdvertisedHost(config);
        BufferAllocator allocator = new RootAllocator(
                config.coordinatorTransportAllocatorLimitBytes());
        CoordinatorResultBuffer buffer = new CoordinatorResultBuffer(
                scheduler, CoordinatorResultBuffer.DEFAULT_RETENTION_MS);
        TransportServerLifecycle server = null;
        try {
            server = createServer(config, tracing, buffer);
            server.start();
            return new Started(createService(config, buffer, server, allocator),
                    server, allocator, buffer);
        } catch (RuntimeException failure) {
            closeAfterFailure(server, allocator, buffer, failure);
            throw failure;
        }
    }

    private static TransportServerLifecycle createServer(
            CoordinatorBootstrapConfig config, TracingProvider tracing,
            CoordinatorResultBuffer buffer) {
        List<io.grpc.ServerInterceptor> interceptors = tracing == null
                ? List.of()
                : List.of(new org.pangrova.observability.sdk.tracing.adapter.grpc.FlightSpanInterceptor(tracing));
        var tls = config.shuffleTlsConfig();
        var serverConfig = new TransportServerConfig(config.coordinatorTransportPort(),
                tls == null ? null : tls.certificateChain().toString(),
                tls == null ? null : tls.privateKey().toString(),
                tls == null ? null : tls.rootCa().toString(), tls != null,
                config.coordinatorTransportAllocatorLimitBytes());
        return new TransportServer(new CoordinatorDataProducer(buffer), serverConfig, null, interceptors);
    }

    private static CoordinatorResultService createService(
            CoordinatorBootstrapConfig config, CoordinatorResultBuffer buffer,
            TransportServerLifecycle server, BufferAllocator allocator) {
        ResultEndpoint endpoint = new ResultEndpoint(
                config.coordinatorTransportAdvertisedHost(), server.getPort(), "",
                (config.shuffleTlsConfig() == null ? ResultProtocol.GRPC : ResultProtocol.GRPC_TLS).value());
        LOG.info("event=control_plane_result_transport_started port={} advertisedHost={}",
                server.getPort(), config.coordinatorTransportAdvertisedHost());
        return new CoordinatorResultService(buffer,
                new CoordinatorResultWriter(allocator), endpoint);
    }

    private static void closeAfterFailure(
            TransportServerLifecycle server, BufferAllocator allocator, CoordinatorResultBuffer buffer,
            RuntimeException failure) {
        if (server != null) {
            try {
                server.stop();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
        buffer.close();
        try {
            allocator.close();
        } catch (RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    record Started(CoordinatorResultService service, TransportServerLifecycle server,
                   BufferAllocator allocator, CoordinatorResultBuffer buffer) {}
}
