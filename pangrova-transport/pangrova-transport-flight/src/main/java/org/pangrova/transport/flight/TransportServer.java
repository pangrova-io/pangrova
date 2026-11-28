package org.pangrova.transport.flight;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.common.concurrency.ThreadSleeper;
import org.pangrova.protocol.transport.DataProducer;
import org.pangrova.protocol.transport.TransportServerConfig;
import org.pangrova.protocol.transport.TransportServerLifecycle;
import io.grpc.ServerInterceptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.FlightProducer;
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
    private final FlightProducer flightProducer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final BufferAllocator allocator;
    private final boolean ownsAllocator;
    private final TransportServerConfig config;
    private final List<ServerInterceptor> serverInterceptors;

    private volatile org.apache.arrow.flight.FlightServer delegate;

    /** Guarded by the synchronized lifecycle methods; failed drain remains retryable. */
    private boolean closed;
    private boolean stopping;

    public TransportServer(DataProducer producer, TransportServerConfig config) {
        this(producer, config, allocatorLimit(config));
    }

    /** Creates a serialized stream server with an explicit allocator memory limit. */
    public TransportServer(DataProducer producer, TransportServerConfig config, long allocatorLimit) {
        this(producer, config, new RootAllocator(allocatorLimit), true, null, List.of());
    }

    public TransportServer(DataProducer producer, TransportServerConfig config, TransportTicketSigner ticketSigner) {
        this(producer, config, new RootAllocator(allocatorLimit(config)), true, ticketSigner, List.of());
    }

    public TransportServer(DataProducer producer, TransportServerConfig config, BufferAllocator allocator) {
        this(producer, config, allocator, false, null, List.of());
    }

    public TransportServer(DataProducer producer, TransportServerConfig config, TransportTicketSigner ticketSigner,
            List<ServerInterceptor> serverInterceptors) {
        this(producer, config, new RootAllocator(allocatorLimit(config)), true, ticketSigner, serverInterceptors);
    }

    /** Binds a direct Arrow Flight producer. The caller retains allocator ownership. */
    public TransportServer(FlightProducer producer, TransportServerConfig config, BufferAllocator allocator,
            List<ServerInterceptor> serverInterceptors) {
        this(producer, config, allocator, false, serverInterceptors);
    }

    private TransportServer(DataProducer producer, TransportServerConfig config, BufferAllocator allocator,
            boolean ownsAllocator, TransportTicketSigner signer, List<ServerInterceptor> serverInterceptors) {
        this(new BatchStreamProducer(Objects.requireNonNull(producer), allocator, signer),
                config, allocator, ownsAllocator, serverInterceptors);
    }

    private TransportServer(FlightProducer producer, TransportServerConfig config, BufferAllocator allocator,
            boolean ownsAllocator, List<ServerInterceptor> serverInterceptors) {
        this.flightProducer = Objects.requireNonNull(producer, "producer");
        this.config = Objects.requireNonNull(config, "config");
        this.requestedPort = config.port();
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.ownsAllocator = ownsAllocator;
        this.serverInterceptors = serverInterceptors == null ? List.of() : List.copyOf(serverInterceptors);
    }

    private static long allocatorLimit(TransportServerConfig config) {
        long limit = config.allocatorLimitBytes();
        return limit > 0 ? limit : DEFAULT_ALLOCATOR_LIMIT;
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

    BufferAllocator allocator() {
        return allocator;
    }

    private org.apache.arrow.flight.FlightServer buildServer() throws IOException {
        validateTlsConfiguration();
        if (config.tlsEnabled()) {
            return buildSecureServer();
        }
        Location location = Location.forGrpcInsecure("0.0.0.0", requestedPort);
        org.apache.arrow.flight.FlightServer.Builder builder =
                org.apache.arrow.flight.FlightServer.builder(allocator, location, flightProducer);
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

    private org.apache.arrow.flight.FlightServer buildSecureServer() throws IOException {
        Path certificate = requireTlsFile(config.tlsCertPath());
        Path key = requireTlsFile(config.tlsKeyPath());
        Path trust = config.mtlsConfigured() ? requireTlsFile(config.tlsTrustChainPath()) : null;
        Location location = Location.forGrpcTls("0.0.0.0", requestedPort);
        try (var certificateInput = Files.newInputStream(certificate);
             var keyInput = Files.newInputStream(key);
             var trustInput = trust == null ? null : Files.newInputStream(trust)) {
            var builder = org.apache.arrow.flight.FlightServer.builder(allocator, location, flightProducer)
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
            ThreadSleeper.sleepQuietly(ALLOCATOR_DRAIN_POLL_INTERVAL_MS);
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
        }
    }
}
