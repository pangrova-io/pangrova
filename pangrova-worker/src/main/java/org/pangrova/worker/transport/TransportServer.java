package org.pangrova.worker.transport;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.spi.transport.DataProducer;
import org.pangrova.spi.transport.TransportServerConfig;
import org.pangrova.spi.transport.TransportServerLifecycle;
import io.grpc.ServerInterceptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Transport gRPC server wrapping the data exchange SDK.
 * Binds a gRPC port, routes DoGet to the injected DataProducer.
 *
 * <p>Concurrency strategy: AtomicBoolean for running flag,
 * volatile for delegate server reference. start/stop are
 * idempotent via CAS on the running flag.</p>
 *
 * <p>Accepts the simplified {@link TransportServerConfig} from SPI (port only)
 * and creates its own {@link BufferAllocator} internally.</p>
 */
@ThreadSafe
public class TransportServer implements TransportServerLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(TransportServer.class);

    private static final long DEFAULT_ALLOCATOR_LIMIT = 256L * 1024 * 1024;

    /** Poll interval while waiting for the allocator to drain during shutdown. */
    private static final long ALLOCATOR_DRAIN_POLL_INTERVAL_MS = 50;

    private final int requestedPort;
    private final DataProducer producer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final BufferAllocator allocator;
    private final boolean ownsAllocator;
    private final TransportServerConfig config;
    private final TransportTicketSigner ticketSigner;
    private final List<ServerInterceptor> serverInterceptors;

    private volatile org.apache.arrow.flight.FlightServer delegate;

    public TransportServer(DataProducer producer, TransportServerConfig config) {
        this(producer, config,
                config.allocatorLimitBytes() > 0 ? config.allocatorLimitBytes() : DEFAULT_ALLOCATOR_LIMIT);
    }

    /** Creates a server with an explicit allocator memory limit. */
    public TransportServer(DataProducer producer, TransportServerConfig config, long allocatorLimit) {
        Objects.requireNonNull(producer, "producer must not be null");
        Objects.requireNonNull(config, "config must not be null");
        this.requestedPort = config.port();
        this.producer = producer;
        this.allocator = new RootAllocator(allocatorLimit);
        this.ownsAllocator = true;
        this.config = config;
        this.ticketSigner = null;
        this.serverInterceptors = List.of();
    }

    /**
     * Creates a TransportServer with ticket signing enabled.
     */
    public TransportServer(DataProducer producer, TransportServerConfig config,
                           TransportTicketSigner ticketSigner) {
        Objects.requireNonNull(producer, "producer must not be null");
        Objects.requireNonNull(config, "config must not be null");
        this.requestedPort = config.port();
        this.producer = producer;
        long limit = config.allocatorLimitBytes() > 0 ? config.allocatorLimitBytes() : DEFAULT_ALLOCATOR_LIMIT;
        this.allocator = new RootAllocator(limit);
        this.ownsAllocator = true;
        this.config = config;
        this.ticketSigner = ticketSigner;
        this.serverInterceptors = List.of();
    }

    /**
     * Creates a TransportServer using an externally-managed allocator
     * (e.g., from OffHeapAllocatorManager). The caller retains ownership
     * of the allocator — this server will not close it.
     */
    public TransportServer(DataProducer producer, TransportServerConfig config, BufferAllocator allocator) {
        Objects.requireNonNull(producer, "producer must not be null");
        Objects.requireNonNull(config, "config must not be null");
        this.requestedPort = config.port();
        this.producer = producer;
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.ownsAllocator = false;
        this.config = config;
        this.ticketSigner = null;
        this.serverInterceptors = List.of();
    }

    /**
     * Creates a TransportServer with gRPC ServerInterceptors (e.g., for distributed
     * tracing via FlightSpanInterceptor). Interceptors are attached to the underlying
     * gRPC NettyServerBuilder via the data exchange SDK's {@code grpc.builderConsumer} hint.
     *
     * @param producer            data producer
     * @param config              transport config (port, TLS settings)
     * @param ticketSigner        optional ticket signer (may be null)
     * @param serverInterceptors  gRPC server interceptors to attach (may be empty)
     */
    public TransportServer(DataProducer producer, TransportServerConfig config,
                           TransportTicketSigner ticketSigner,
                           List<ServerInterceptor> serverInterceptors) {
        Objects.requireNonNull(producer, "producer must not be null");
        Objects.requireNonNull(config, "config must not be null");
        this.requestedPort = config.port();
        this.producer = producer;
        long limit = config.allocatorLimitBytes() > 0 ? config.allocatorLimitBytes() : DEFAULT_ALLOCATOR_LIMIT;
        this.allocator = new RootAllocator(limit);
        this.ownsAllocator = true;
        this.config = config;
        this.ticketSigner = ticketSigner;
        this.serverInterceptors = serverInterceptors != null
                ? List.copyOf(serverInterceptors) : List.of();
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            delegate = buildServer();
            delegate.start();
            LOG.info("event=transport_server_started port={}", getPort());
        } catch (IOException e) {
            running.set(false);
            throw new IllegalStateException("Failed to start TransportServer on port " + requestedPort, e);
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        shutdownDelegate();
        closeOwnedAllocator();
        LOG.info("event=transport_server_stopped");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Returns the actual bound port. Critical for port=0 (random port) usage.
     */
    @Override
    public int getPort() {
        org.apache.arrow.flight.FlightServer server = delegate;
        return server != null ? server.getPort() : requestedPort;
    }

    public DataProducer getProducer() {
        return producer;
    }

    private org.apache.arrow.flight.FlightServer buildServer() throws IOException {
        boolean certificateConfigured = config.tlsCertPath() != null && !config.tlsCertPath().isBlank();
        boolean keyConfigured = config.tlsKeyPath() != null && !config.tlsKeyPath().isBlank();
        if (certificateConfigured != keyConfigured || (config.mtlsEnabled() && !config.mtlsConfigured())) {
            throw new IllegalArgumentException("Incomplete transport TLS or mutual TLS configuration");
        }
        BatchStreamProducer adapter = new BatchStreamProducer(producer, allocator, ticketSigner);
        if (config.tlsEnabled()) {
            java.io.File certFile = new java.io.File(config.tlsCertPath());
            java.io.File keyFile = new java.io.File(config.tlsKeyPath());
            if (!certFile.exists() || !keyFile.exists()) {
                LOG.error("event=transport_tls_cert_invalid certPath={} keyPath={} certExists={} keyExists={}",
                        config.tlsCertPath(), config.tlsKeyPath(),
                        certFile.exists(), keyFile.exists());
                throw new IllegalStateException(
                        "TLS certificate or key file not found: cert=" + config.tlsCertPath()
                                + " key=" + config.tlsKeyPath());
            }
            Location location = Location.forGrpcTls("0.0.0.0", requestedPort);
            if (config.mtlsConfigured()) {
                LOG.info("event=transport_server_mtls_enabled certPath={} trustChainPath={}",
                        config.tlsCertPath(), config.tlsTrustChainPath());
            } else {
                LOG.info("event=transport_server_tls_enabled certPath={}", config.tlsCertPath());
            }
            org.apache.arrow.flight.FlightServer.Builder tlsBuilder =
                    org.apache.arrow.flight.FlightServer.builder(allocator, location, adapter)
                            .useTls(certFile, keyFile);
            if (config.mtlsConfigured()) {
                tlsBuilder.useMTlsClientVerification(new java.io.File(config.tlsTrustChainPath()));
            }
            attachInterceptors(tlsBuilder);
            return tlsBuilder.build();
        }
        Location location = Location.forGrpcInsecure("0.0.0.0", requestedPort);
        org.apache.arrow.flight.FlightServer.Builder builder =
                org.apache.arrow.flight.FlightServer.builder(allocator, location, adapter);
        attachInterceptors(builder);
        return builder.build();
    }

    /**
     * Attaches the configured gRPC ServerInterceptors to the server builder
     * via the data exchange SDK's {@code grpc.builderConsumer} transport hint. No-op when
     * the interceptor list is empty.
     */
    private void attachInterceptors(org.apache.arrow.flight.FlightServer.Builder builder) {
        if (serverInterceptors.isEmpty()) {
            return;
        }
        java.util.function.Consumer<io.grpc.netty.NettyServerBuilder> consumer = grpcBuilder -> {
            for (ServerInterceptor interceptor : serverInterceptors) {
                grpcBuilder.intercept(interceptor);
            }
        };
        builder.transportHint("grpc.builderConsumer", consumer);
        LOG.info("event=transport_server_interceptors_attached count={}", serverInterceptors.size());
    }

    /**
     * Shuts down the transport server using FlightServer.close() — the standard
     * three-phase shutdown protocol from the data exchange SDK:
     * <ol>
     *   <li>shutdown() — graceful, stops accepting new RPCs</li>
     *   <li>awaitTermination(3s) — waits for in-flight RPCs to complete</li>
     *   <li>shutdownNow() — force-cancels pending RPCs, releases Netty ByteBuf</li>
     * </ol>
     * Without shutdownNow(), gRPC's async write pipeline leaves un-released
     * ByteBuf instances that trigger Netty ResourceLeakDetector SEVERE warnings.
     */
    private void shutdownDelegate() {
        org.apache.arrow.flight.FlightServer server = delegate;
        if (server == null) {
            return;
        }
        try {
            server.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=transport_server_shutdown_interrupted");
        }
        delegate = null;
    }

    /**
     * Closes owned allocator, tolerating minor leaks from gRPC async cleanup.
     * Skips close if the allocator is externally managed (ownsAllocator=false).
     */
    private void closeOwnedAllocator() {
        if (!ownsAllocator) {
            return;
        }
        waitForAllocatorDrain();
        try {
            allocator.close();
        } catch (IllegalStateException e) {
            LOG.warn("event=transport_allocator_leak error={}", e.getMessage());
        }
    }

    private void waitForAllocatorDrain() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (allocator.getAllocatedMemory() > 0 && System.nanoTime() < deadline) {
            ThreadUtils.sleepQuietly(ALLOCATOR_DRAIN_POLL_INTERVAL_MS);
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
        }
    }
}
