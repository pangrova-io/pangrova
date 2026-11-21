package org.pangrova.transport.flight;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.protocol.transport.DataProducer;
import org.pangrova.protocol.transport.TransportServerConfig;
import org.pangrova.protocol.transport.TransportServerLifecycle;
import io.grpc.ServerInterceptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    /** Guarded by the synchronized lifecycle methods; failed drain remains retryable. */
    private boolean closed;
    private boolean stopping;

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
    public synchronized void start() {
        if (closed || stopping) {
            throw new IllegalStateException("TransportServer is closing or has been closed");
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            delegate = buildServer();
            delegate.start();
            LOG.info("event=transport_server_started port={}", getPort());
        } catch (IOException | RuntimeException failure) {
            try {
                stop();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw new IllegalStateException("Failed to start TransportServer on port " + requestedPort, failure);
        }
    }

    @Override
    public synchronized void stop() {
        if (closed) {
            return;
        }
        stopping = true;
        running.set(false);
        shutdownDelegate();
        closeOwnedAllocator();
        closed = true;
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

    BufferAllocator allocator() {
        return allocator;
    }

    private org.apache.arrow.flight.FlightServer buildServer() throws IOException {
        BatchStreamProducer batchProducer = new BatchStreamProducer(producer, allocator, ticketSigner);
        validateTlsConfiguration();
        if (config.tlsEnabled()) {
            return buildSecureServer(batchProducer);
        }
        Location location = Location.forGrpcInsecure("0.0.0.0", requestedPort);
        org.apache.arrow.flight.FlightServer.Builder builder =
                org.apache.arrow.flight.FlightServer.builder(allocator, location, batchProducer);
        attachInterceptors(builder);
        return builder.build();
    }

    private void validateTlsConfiguration() {
        boolean certificate = config.tlsCertPath() != null && !config.tlsCertPath().isBlank();
        boolean key = config.tlsKeyPath() != null && !config.tlsKeyPath().isBlank();
        if (certificate != key || (config.mtlsEnabled() && !config.mtlsConfigured())) {
            throw new IllegalArgumentException("TLS requires certificate and key; mTLS also requires a trust chain");
        }
    }

    private org.apache.arrow.flight.FlightServer buildSecureServer(
            BatchStreamProducer batchProducer) throws IOException {
        Path certificate = requireTlsFile(config.tlsCertPath());
        Path key = requireTlsFile(config.tlsKeyPath());
        Path trust = config.mtlsConfigured() ? requireTlsFile(config.tlsTrustChainPath()) : null;
        Location location = Location.forGrpcTls("0.0.0.0", requestedPort);
        try (var certificateInput = Files.newInputStream(certificate);
             var keyInput = Files.newInputStream(key);
             var trustInput = trust == null ? null : Files.newInputStream(trust)) {
            var builder = org.apache.arrow.flight.FlightServer.builder(allocator, location, batchProducer)
                    .useTls(certificateInput, keyInput);
            if (trustInput != null) {
                builder.useMTlsClientVerification(trustInput);
            }
            attachInterceptors(builder);
            var server = builder.build();
            LOG.info("event=transport_server_tls_configured mtls={}", trustInput != null);
            return server;
        }
    }

    private static Path requireTlsFile(String configured) throws IOException {
        Path path = Path.of(configured);
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IOException("TLS input is not a readable regular file: " + path);
        }
        return path;
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
            throw new IllegalStateException("Transport server shutdown was interrupted; owner remains retained", e);
        }
        delegate = null;
    }

    /**
     * Closes the owned allocator only after actual transport memory has drained.
     * Skips close if the allocator is externally managed (ownsAllocator=false).
     */
    private void closeOwnedAllocator() {
        if (!ownsAllocator) {
            return;
        }
        waitForAllocatorDrain();
        long retained = allocator.getAllocatedMemory();
        if (retained != 0) {
            throw new IllegalStateException("Transport allocator still owns " + retained
                    + " bytes; retry after release");
        }
        allocator.close();
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
