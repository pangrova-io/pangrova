package org.pangrova.coordinator.ai.training;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ServerInterceptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.observability.tracing.FlightSpanInterceptor;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * gRPC server for ML training data serving.
 *
 * <p>Provides high-throughput, zero-copy data streaming to ML training frameworks
 * via the columnar data streaming protocol. Supports version pinning (tag/branch/snapshot),
 * hash-based split descriptors for train/val/test partitioning, uniform random
 * sampling, and parallel endpoint distribution.</p>
 *
 * <p>Authentication is pluggable (JWT by default). RBAC is enforced via the
 * standard {@link AccessControl} SPI — clients must have SELECT privilege on
 * the requested table.</p>
 *
 * <p>Lifecycle: call {@link #start()} to bind the gRPC port, {@link #close()} to
 * shut down gracefully. Implements {@link Closeable} for try-with-resources usage.</p>
 */
@ThreadSafe
public final class TrainingDataServer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TrainingDataServer.class);

    /** Interval between allocator-drain checks during shutdown. */
    private static final long ALLOCATOR_DRAIN_POLL_INTERVAL_MS = 50;
    private static final long ALLOCATOR_DRAIN_TIMEOUT_SECONDS = 3;

    private final TrainingDataServerConfig config;
    private final AccessControl accessControl;
    private final TrainingDataMetrics metrics;
    private final ObjectMapper objectMapper;
    private final Semaphore streamSemaphore;
    private final Set<String> activeStreamIds;
    private final AtomicBoolean started;
    private final AtomicBoolean closed;
    private final CatalogRegistry catalogRegistry;
    private final TracingProvider tracingProvider;

    private volatile org.apache.arrow.flight.FlightServer grpcServer;
    private volatile BufferAllocator allocator;

    /**
     * Creates a new training data server with distributed tracing support.
     *
     * @param config          server configuration (port, auth type, concurrency limits)
     * @param accessControl   RBAC enforcement for table access checks
     * @param metricsProvider metrics provider for instrument creation
     * @param catalogRegistry catalog registry for resolving table data
     * @param tracingProvider tracing provider for FlightSpanInterceptor (may be null)
     */
    public TrainingDataServer(TrainingDataServerConfig config,
                              AccessControl accessControl,
                              MetricsProvider metricsProvider,
                              CatalogRegistry catalogRegistry,
                              TracingProvider tracingProvider) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.accessControl = Objects.requireNonNull(accessControl, "accessControl must not be null");
        Objects.requireNonNull(metricsProvider, "metricsProvider must not be null");
        this.metrics = new TrainingDataMetrics(metricsProvider);
        this.objectMapper = new ObjectMapper();
        this.streamSemaphore = new Semaphore(config.maxConcurrentStreams());
        this.activeStreamIds = ConcurrentHashMap.newKeySet();
        this.started = new AtomicBoolean(false);
        this.closed = new AtomicBoolean(false);
        this.catalogRegistry = catalogRegistry;
        this.tracingProvider = tracingProvider;
    }

    /**
     * Creates a new training data server without tracing.
     */
    public TrainingDataServer(TrainingDataServerConfig config,
                              AccessControl accessControl,
                              MetricsProvider metricsProvider,
                              CatalogRegistry catalogRegistry) {
        this(config, accessControl, metricsProvider, catalogRegistry, null);
    }

    /**
     * Creates a server without catalog registry (business logic testing only).
     * The gRPC endpoint will not be bound; only in-process method calls work.
     *
     * @param config          server configuration
     * @param accessControl   RBAC enforcement
     * @param metricsProvider metrics provider
     */
    public TrainingDataServer(TrainingDataServerConfig config,
                              AccessControl accessControl,
                              MetricsProvider metricsProvider) {
        this(config, accessControl, metricsProvider, null, null);
    }

    /**
     * Starts the gRPC server on the configured port.
     *
     * <p>After this call returns, the server is accepting connections.</p>
     *
     * @throws IllegalStateException if already started or closed
     */
    public void start() {
        if (closed.get()) {
            throw new IllegalStateException("TrainingDataServer has been closed");
        }
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("TrainingDataServer already started");
        }

        LOG.info("event=training_server_starting port={} authType={} maxStreams={}",
                config.port(), config.authType(), config.maxConcurrentStreams());

        if (catalogRegistry != null) {
            bindGrpcEndpoint();
        }

        LOG.info("event=training_server_started port={}", config.port());
    }

    private void bindGrpcEndpoint() {
        allocator = new RootAllocator(256L * 1024 * 1024);
        try {
            Location location = Location.forGrpcInsecure("0.0.0.0", config.port());
            TrainingDataScanner scanner = new TrainingDataScanner(catalogRegistry);
            TrainingDataProducer producer = new TrainingDataProducer(this, scanner, allocator, location);

            org.apache.arrow.flight.FlightServer.Builder builder =
                    org.apache.arrow.flight.FlightServer.builder(allocator, location, producer);
            attachTracingInterceptor(builder);
            grpcServer = builder.build();
            grpcServer.start();
            LOG.info("event=training_grpc_bound port={}", grpcServer.getPort());
        } catch (Exception e) {
            allocator.close();
            allocator = null;
            started.set(false);
            throw new IllegalStateException(
                    "Failed to bind training data gRPC endpoint on port " + config.port(), e);
        }
    }

    /**
     * Attaches the FlightSpanInterceptor for distributed tracing of inbound data streaming
     * RPCs when a TracingProvider is configured. Uses the gRPC SDK's
     * {@code grpc.builderConsumer} hint to register a gRPC ServerInterceptor.
     */
    private void attachTracingInterceptor(org.apache.arrow.flight.FlightServer.Builder builder) {
        if (tracingProvider == null) {
            return;
        }
        ServerInterceptor interceptor = new FlightSpanInterceptor(tracingProvider);
        java.util.function.Consumer<io.grpc.netty.NettyServerBuilder> consumer =
                grpcBuilder -> grpcBuilder.intercept(interceptor);
        builder.transportHint("grpc.builderConsumer", consumer);
        LOG.info("event=training_grpc_tracing_interceptor_attached");
    }

    /**
     * Handles a getFlightInfo request: parses the descriptor, validates auth/RBAC,
     * validates the split expression, and returns endpoint information for parallel loading.
     *
     * @param commandBytes raw JSON command from FlightDescriptor
     * @param identity     authenticated user identity
     * @return list of endpoint tickets for parallel data retrieval
     * @throws InvalidSplitExpressionException if the split expression is malformed
     */
    public List<EndpointTicket> getFlightInfo(byte[] commandBytes, UserIdentity identity) {
        Objects.requireNonNull(commandBytes, "commandBytes must not be null");
        Objects.requireNonNull(identity, "identity must not be null");

        metrics.recordRequest();

        TrainingDataRequest request = parseCommand(commandBytes);
        enforceAccess(request, identity);

        if (request.split() != null) {
            SplitExpressionParser.parse(request.split());
        }

        if (request.version() != null) {
            TrainingDataRequest.VersionReference versionRef = request.parsedVersion();
            validateVersionNotExpired(request.table(), versionRef);
        }

        return planEndpoints(request);
    }

    /**
     * Handles a doGet request: acquires a stream permit, streams data to the client,
     * and tracks metrics.
     *
     * @param ticketBytes serialized ticket from a previous getFlightInfo call
     * @param identity    authenticated user identity
     * @return a stream context for reading batches
     * @throws IllegalStateException if max concurrent streams exceeded
     */
    public StreamContext openStream(byte[] ticketBytes, UserIdentity identity) {
        Objects.requireNonNull(ticketBytes, "ticketBytes must not be null");
        Objects.requireNonNull(identity, "identity must not be null");

        if (!streamSemaphore.tryAcquire()) {
            metrics.recordError();
            throw new IllegalStateException(
                    "Maximum concurrent streams reached: " + config.maxConcurrentStreams());
        }

        String streamId = generateStreamId();
        activeStreamIds.add(streamId);
        metrics.recordStreamOpened();

        LOG.debug("event=training_stream_opened streamId={} user={}", streamId, identity.principal());

        return new StreamContext(streamId, ticketBytes, identity);
    }

    /**
     * Closes a stream, releasing the concurrency permit and updating metrics.
     *
     * @param streamId    the stream identifier from {@link StreamContext}
     * @param bytesServed total bytes served during this stream
     */
    public void closeStream(String streamId, long bytesServed) {
        Objects.requireNonNull(streamId, "streamId must not be null");

        if (activeStreamIds.remove(streamId)) {
            streamSemaphore.release();
            metrics.recordStreamClosed();
            metrics.recordBytesServed(bytesServed);

            LOG.debug("event=training_stream_closed streamId={} bytesServed={}", streamId, bytesServed);
        }
    }

    /**
     * Returns the configured port.
     *
     * @return the gRPC listen port
     */
    public int port() {
        return config.port();
    }

    /**
     * Returns whether the server has been started.
     *
     * @return true if started
     */
    public boolean isStarted() {
        return started.get();
    }

    /**
     * Returns the number of currently active streams.
     *
     * @return active stream count
     */
    public int activeStreamCount() {
        return activeStreamIds.size();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        LOG.info("event=training_server_stopping port={} activeStreams={}",
                config.port(), activeStreamIds.size());

        shutdownGrpcServer();
        activeStreamIds.clear();
        started.set(false);

        LOG.info("event=training_server_stopped port={}", config.port());
    }

    private void shutdownGrpcServer() {
        org.apache.arrow.flight.FlightServer server = grpcServer;
        if (server != null) {
            try {
                server.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("event=training_server_shutdown_interrupted");
            }
            grpcServer = null;
        }
        BufferAllocator alloc = allocator;
        if (alloc != null) {
            waitForAllocatorDrain(alloc);
            try {
                alloc.close();
            } catch (IllegalStateException e) {
                LOG.warn("event=training_allocator_leak error={}", e.getMessage());
            }
            allocator = null;
        }
    }

    private void waitForAllocatorDrain(BufferAllocator alloc) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(ALLOCATOR_DRAIN_TIMEOUT_SECONDS);
        ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "allocator-drain-poller");
            thread.setDaemon(true);
            return thread;
        });
        CountDownLatch drained = new CountDownLatch(1);
        poller.scheduleAtFixedRate(() -> {
            if (alloc.getAllocatedMemory() <= 0 || System.nanoTime() >= deadline) {
                drained.countDown();
            }
        }, 0, ALLOCATOR_DRAIN_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
        try {
            drained.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            poller.shutdownNow();
        }
    }

    private TrainingDataRequest parseCommand(byte[] commandBytes) {
        String json = new String(commandBytes, StandardCharsets.UTF_8);
        try {
            JsonNode node = objectMapper.readTree(json);
            String table = requireTextField(node, "table");
            String version = optionalTextField(node, "version");
            String split = optionalTextField(node, "split");
            List<String> columns = parseColumns(node);
            Double samplingRate = parseSamplingRate(node);

            return new TrainingDataRequest(table, version, split, columns, samplingRate);
        } catch (JsonProcessingException e) {
            metrics.recordError();
            throw new IllegalArgumentException("Invalid Flight descriptor JSON: " + e.getMessage(), e);
        }
    }

    private String requireTextField(JsonNode node, String field) {
        JsonNode fieldNode = node.get(field);
        if (fieldNode == null || fieldNode.isNull() || !fieldNode.isTextual()) {
            throw new IllegalArgumentException(
                    "Missing required field '" + field + "' in Flight descriptor");
        }
        return fieldNode.asText();
    }

    private String optionalTextField(JsonNode node, String field) {
        JsonNode fieldNode = node.get(field);
        if (fieldNode == null || fieldNode.isNull()) {
            return null;
        }
        return fieldNode.asText();
    }

    private List<String> parseColumns(JsonNode node) {
        JsonNode columnsNode = node.get("columns");
        if (columnsNode == null || columnsNode.isNull()) {
            return null;
        }
        if (!columnsNode.isArray()) {
            throw new IllegalArgumentException("'columns' must be an array");
        }
        List<String> columns = new ArrayList<>(columnsNode.size());
        for (JsonNode col : columnsNode) {
            columns.add(col.asText());
        }
        return columns;
    }

    private Double parseSamplingRate(JsonNode node) {
        JsonNode rateNode = node.get("samplingRate");
        if (rateNode == null || rateNode.isNull()) {
            return null;
        }
        return rateNode.asDouble();
    }

    private void enforceAccess(TrainingDataRequest request, UserIdentity identity) {
        String[] parts = request.table().split("\\.");
        if (parts.length < 3) {
            throw new IllegalArgumentException(
                    "Table must be fully qualified (catalog.schema.table), got: " + request.table());
        }
        accessControl.checkCanSelectFromTable(identity, parts[0], parts[1], parts[2]);
    }

    private void validateVersionNotExpired(String table, TrainingDataRequest.VersionReference versionRef) {
        // Version expiration is checked by the connector during scan planning.
        // If the referenced snapshot has been garbage-collected, the connector will throw
        // DATASET_VERSION_EXPIRED (3009). This method logs the validation attempt for
        // observability and serves as the integration point for future version cache.
        LOG.debug("event=version_validation table={} type={} value={}",
                table, versionRef.type(), versionRef.value());
    }

    private List<EndpointTicket> planEndpoints(TrainingDataRequest request) {
        // Parallel endpoints: distribute work across available coordinator instances.
        // For now, return a single endpoint pointing to this server.
        // Multi-coordinator distribution will be added when cluster topology is available.
        String ticket = serializeTicket(request);
        return List.of(new EndpointTicket(ticket, config.port()));
    }

    private String serializeTicket(TrainingDataRequest request) {
        try {
            return objectMapper.writeValueAsString(request);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize ticket: " + e.getMessage(), e);
        }
    }

    private String generateStreamId() {
        return "stream-" + Instant.now().toEpochMilli() + "-" + Thread.currentThread().getId();
    }

    /**
     * Represents a Flight endpoint ticket for parallel data retrieval.
     *
     * @param ticket serialized request for doGet
     * @param port   the server port hosting this endpoint
     */
    public record EndpointTicket(String ticket, int port) {
        public EndpointTicket {
            Objects.requireNonNull(ticket, "ticket must not be null");
        }
    }

    /**
     * Context for an active data stream, returned by {@link #openStream}.
     *
     * @param streamId    unique stream identifier for lifecycle tracking
     * @param ticketBytes the original ticket bytes
     * @param identity    the authenticated user
     */
    public record StreamContext(String streamId, byte[] ticketBytes, UserIdentity identity) {
        public StreamContext {
            Objects.requireNonNull(streamId, "streamId must not be null");
            Objects.requireNonNull(ticketBytes, "ticketBytes must not be null");
            Objects.requireNonNull(identity, "identity must not be null");
            ticketBytes = ticketBytes.clone();
        }

        @Override
        public byte[] ticketBytes() {
            return ticketBytes.clone();
        }
    }
}
