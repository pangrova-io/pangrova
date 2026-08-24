/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.transport;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.query.ResultProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Arrow Flight result fetcher. All Arrow and Flight imports are confined to this class.
 *
 * <p>Manages a pool of {@link FlightClient} instances keyed by {@code host:port}.
 * Clients are created on demand and evicted when connection errors are detected.
 * The owning {@link BufferAllocator} is released in {@link #close()}.</p>
 *
 * <h4>Timeout strategy (per-batch idle detection)</h4>
 * <p>Uses the production-standard pattern for server-streaming RPCs: instead of
 * setting a gRPC call-level deadline (which would kill long-running but healthy
 * streams after a fixed wall-clock duration), each individual {@code stream.next()}
 * call is bounded by {@link #BATCH_IDLE_TIMEOUT_SECONDS}. This means large result
 * sets that continuously produce batches never timeout, while stuck tasks that stop
 * producing data are detected within the idle window.</p>
 *
 * <p>Errors are classified and rethrown as {@link TransportConnectionException}
 * (connection failures) or {@link TransportServerException} (server errors), so
 * callers in {@code client-*} modules never import Arrow types.</p>
 */
public final class FlightResultFetcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FlightResultFetcher.class);

    private static final long DEFAULT_ALLOCATOR_LIMIT = 512L * 1024 * 1024;

    /** HTTP-style status surfaced as "service unavailable" when Flight reports RESOURCE_EXHAUSTED. */
    private static final int HTTP_STATUS_UNAVAILABLE = 503;
    /** HTTP-style status surfaced as "unauthenticated" when Flight reports UNAUTHENTICATED. */
    private static final int HTTP_STATUS_UNAUTHORIZED = 401;
    /** HTTP-style status surfaced as "forbidden" when Flight reports UNAUTHORIZED. */
    private static final int HTTP_STATUS_FORBIDDEN = 403;

    /**
     * Per-batch idle timeout for server-streaming DoGet RPCs.
     * If {@code stream.next()} does not return within this window, the task is
     * considered stuck and a {@link TransportConnectionException} is raised.
     * This is NOT a call-level deadline — streams that continuously produce
     * batches run indefinitely without hitting this limit.
     */
    private static final long BATCH_IDLE_TIMEOUT_SECONDS = 120;

    /**
     * Daemon-thread executor for async {@code stream.next()} calls.
     * Submitting the blocking gRPC read to a dedicated thread allows the
     * calling thread to enforce idle timeout via {@link Future#get(long, TimeUnit)}.
     */
    private static final ExecutorService BATCH_READ_EXECUTOR =
            Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "flight-batch-reader");
                t.setDaemon(true);
                return t;
            });

    private final BufferAllocator allocator;
    private final String trustStorePath;
    private final ConcurrentHashMap<String, FlightClient> clientPool = new ConcurrentHashMap<>();

    public FlightResultFetcher() {
        this(false, null);
    }

    public FlightResultFetcher(boolean useTls, String trustStorePath) {
        this(useTls, trustStorePath, DEFAULT_ALLOCATOR_LIMIT);
    }

    /**
     * Constructor with a configurable Arrow allocator upper bound. The limit is a
     * hard cap on client-side result buffering (defaults to 512 MiB via the other
     * constructors); raise or lower it per deployment to bound client memory. A
     * non-positive value is rejected so the cap can never be silently disabled.
     */
    public FlightResultFetcher(boolean useTls, String trustStorePath, long allocatorLimitBytes) {
        if (allocatorLimitBytes <= 0) {
            throw new IllegalArgumentException("allocatorLimitBytes must be positive");
        }
        this.allocator = new RootAllocator(allocatorLimitBytes);
        this.trustStorePath = trustStorePath;
    }

    /** The Arrow allocator's hard upper bound in bytes (package-private for tests). */
    long allocatorLimitBytes() {
        return allocator.getLimit();
    }

    /**
     * Fetches all result batches and serializes them into a single Arrow IPC byte array.
     *
     * @throws TransportConnectionException on connection failure
     * @throws TransportServerException     on server-reported error
     */
    public byte[] fetch(ResultEndpoint endpoint) {
        String poolKey = poolKey(endpoint);
        FlightClient client = getOrCreateClient(poolKey, resolveLocation(endpoint));
        try (FlightStream stream = client.getStream(ticket(endpoint))) {
            return serializeStream(stream, endpoint);
        } catch (TransportException e) {
            if (isConnectionError(e)) {
                evictClient(poolKey);
            }
            throw e;
        } catch (ConnectException e) {
            evictClient(poolKey);
            throw connectionError("TRANSPORT_CONNECTION_FAILED",
                    "Transport connection failed to " + detail(endpoint), e);
        } catch (Exception e) {
            TransportException classified = classifyError(endpoint, e);
            if (isConnectionError(classified)) {
                evictClient(poolKey);
            }
            throw classified;
        }
    }

    /**
     * Streams result batches, invoking the consumer for each Arrow IPC batch.
     *
     * @throws TransportConnectionException on connection failure
     * @throws TransportServerException     on server-reported error
     */
    public void fetchStream(ResultEndpoint endpoint, Consumer<byte[]> batchConsumer) {
        String poolKey = poolKey(endpoint);
        FlightClient client = getOrCreateClient(poolKey, resolveLocation(endpoint));
        try (FlightStream stream = client.getStream(ticket(endpoint))) {
            VectorSchemaRoot root = stream.getRoot();
            while (awaitNextBatch(stream, endpoint)) {
                batchConsumer.accept(serializeBatch(root));
            }
        } catch (TransportException e) {
            if (isConnectionError(e)) {
                evictClient(poolKey);
            }
            throw e;
        } catch (ConnectException e) {
            evictClient(poolKey);
            throw connectionError("TRANSPORT_CONNECTION_FAILED",
                    "Transport connection failed to " + detail(endpoint), e);
        } catch (Exception e) {
            TransportException classified = classifyError(endpoint, e);
            if (isConnectionError(classified)) {
                evictClient(poolKey);
            }
            throw classified;
        }
    }

    private static byte[] serializeStream(FlightStream stream, ResultEndpoint endpoint)
            throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        VectorSchemaRoot root = stream.getRoot();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(
                root, null, Channels.newChannel(baos))) {
            writer.start();
            while (awaitNextBatch(stream, endpoint)) {
                writer.writeBatch();
            }
            writer.end();
        }
        return baos.toByteArray();
    }

    // PERF-T4: Reuse ThreadLocal ByteArrayOutputStream to avoid per-batch heap allocation.
    // Pre-allocated with 64KB initial capacity to reduce dynamic resizing for typical batches.
    private static final ThreadLocal<ByteArrayOutputStream> THREAD_LOCAL_BAOS =
            ThreadLocal.withInitial(() -> new ByteArrayOutputStream(64 * 1024));

    private static byte[] serializeBatch(VectorSchemaRoot root) throws Exception {
        ByteArrayOutputStream baos = THREAD_LOCAL_BAOS.get();
        baos.reset();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(
                root, null, Channels.newChannel(baos))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return baos.toByteArray();
    }

    /**
     * Awaits the next batch from a Flight stream with per-batch idle timeout.
     *
     * <p>Production-standard pattern for server-streaming RPCs: each individual
     * {@code stream.next()} is bounded by {@link #BATCH_IDLE_TIMEOUT_SECONDS}.
     * If data flows continuously, the stream runs indefinitely; if the server
     * stops producing data, we detect it within the idle window and raise a
     * {@link TransportConnectionException} so callers can retry on a different worker.</p>
     *
     * @param stream   the Arrow Flight stream
     * @param endpoint the endpoint (for error context)
     * @return true if a batch is available, false if stream exhausted
     * @throws TransportConnectionException if idle timeout exceeded
     */
    private static boolean awaitNextBatch(FlightStream stream, ResultEndpoint endpoint) {
        Future<Boolean> future = BATCH_READ_EXECUTOR.submit(stream::next);
        try {
            return future.get(BATCH_IDLE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true); // interrupt the blocked gRPC read
            throw new TransportConnectionException("TRANSPORT_IDLE_TIMEOUT",
                    "Flight DoGet idle timeout: no batch received within "
                    + BATCH_IDLE_TIMEOUT_SECONDS + "s from " + detail(endpoint)
                    + " — server task is likely stuck", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for Flight batch from "
                    + detail(endpoint), e);
        }
    }

    static TransportException classifyError(ResultEndpoint endpoint, Exception e) {
        String msg = e.getMessage();
        LOG.warn("event=transport_error endpoint={}:{} ticket={} errorClass={} errorMessage={}",
                endpoint.host(), endpoint.port(), endpoint.ticket(),
                e.getClass().getName(), msg, e);
        String detail = detail(endpoint) + " cause=" + e.getClass().getSimpleName() + ": " + msg;

        FlightStatusCode statusCode = extractFlightStatusCode(e);
        if (statusCode != null) {
            return classifyByFlightStatus(statusCode, detail, e);
        }
        if (e instanceof ConnectException
                || e instanceof SocketTimeoutException
                || e instanceof UnknownHostException
                || e instanceof NoRouteToHostException) {
            return connectionError("TRANSPORT_CONNECTION_FAILED",
                    "Transport connection failed to " + detail, e);
        }
        if (e instanceof IOException && !(e instanceof EOFException)) {
            return connectionError("TRANSPORT_CONNECTION_FAILED",
                    "Transport connection failed to " + detail, e);
        }
        return serverError("TRANSPORT_ERROR", "Failed to fetch result from " + detail, -1, e);
    }

    private static TransportException classifyByFlightStatus(
            FlightStatusCode code, String detail, Exception cause) {
        return switch (code) {
            case UNAVAILABLE -> connectionError("TRANSPORT_UNAVAILABLE",
                    "Transport unavailable: " + detail, cause);
            case TIMED_OUT -> connectionError("TRANSPORT_TIMED_OUT",
                    "Transport timed out: " + detail, cause);
            case NOT_FOUND -> connectionError("TRANSPORT_NOT_FOUND",
                    "Transport endpoint not found: " + detail, cause);
            case RESOURCE_EXHAUSTED -> serverError("TRANSPORT_RESOURCE_EXHAUSTED",
                    "Transport resource exhausted: " + detail, HTTP_STATUS_UNAVAILABLE, cause);
            case UNAUTHENTICATED -> serverError("TRANSPORT_UNAUTHENTICATED",
                    "Transport unauthenticated: " + detail, HTTP_STATUS_UNAUTHORIZED, cause);
            case UNAUTHORIZED -> serverError("TRANSPORT_UNAUTHORIZED",
                    "Transport unauthorized: " + detail, HTTP_STATUS_FORBIDDEN, cause);
            case CANCELLED -> serverError("TRANSPORT_CANCELLED",
                    "Transport cancelled: " + detail, -1, cause);
            default -> serverError("TRANSPORT_ERROR",
                    "Transport error " + code.name() + ": " + detail, -1, cause);
        };
    }

    static FlightStatusCode extractFlightStatusCode(Exception e) {
        Throwable current = e;
        for (int depth = 0; depth < 5 && current != null; depth++) {
            if (current instanceof FlightRuntimeException fre) {
                return fre.status().code();
            }
            current = current.getCause();
        }
        return null;
    }

    private FlightClient getOrCreateClient(String key, Location location) {
        return clientPool.computeIfAbsent(key,
                k -> buildClient(location));
    }

    /**
     * Builds a FlightClient for the given location.
     *
     * <p><b>Note on keepalive:</b> Arrow Flight Java 19's Builder does not expose
     * the underlying ManagedChannelBuilder for keepalive configuration.
     * For cross-network production deployments where network partition detection
     * is needed, replace this with {@code FlightGrpcUtils.createFlightClient()}
     * using a ManagedChannel built with {@code keepAliveTime(30s)} and
     * {@code keepAliveTimeout(10s)}. For localhost and single-cluster deployments,
     * the per-batch idle timeout ({@link #BATCH_IDLE_TIMEOUT_SECONDS}) provides
     * sufficient stuck-detection coverage.</p>
     */
    private FlightClient buildClient(Location location) {
        return FlightClient.builder(allocator, location).build();
    }

    private void evictClient(String key) {
        FlightClient evicted = clientPool.remove(key);
        if (evicted != null) {
            try {
                evicted.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOG.debug("event=evict_client_close_failed key={} error={}", key, e.getMessage());
            }
        }
    }

    private Location resolveLocation(ResultEndpoint endpoint) {
        ResultProtocol protocol = ResultProtocol.fromValue(endpoint.protocol());
        if (protocol.requiresTls()) {
            if (trustStorePath != null) {
                throw new UnsupportedOperationException(
                        "Arrow Flight Java client does not support custom trust stores. "
                        + "Configure via javax.net.ssl.trustStore system property. "
                        + "trustStorePath=" + trustStorePath);
            }
            return Location.forGrpcTls(endpoint.host(), endpoint.port());
        }
        return Location.forGrpcInsecure(endpoint.host(), endpoint.port());
    }

    @Override
    public void close() {
        for (var entry : clientPool.entrySet()) {
            try {
                entry.getValue().close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOG.debug("event=pool_client_close_failed key={} error={}",
                        entry.getKey(), e.getMessage());
            }
        }
        clientPool.clear();
        allocator.close();
        LOG.debug("FlightResultFetcher closed");
    }

    private static String poolKey(ResultEndpoint endpoint) {
        return endpoint.host() + ":" + endpoint.port();
    }

    private static String detail(ResultEndpoint endpoint) {
        return endpoint.host() + ":" + endpoint.port() + " ticket=" + endpoint.ticket();
    }

    private static Ticket ticket(ResultEndpoint endpoint) {
        return new Ticket(endpoint.ticket().getBytes(StandardCharsets.UTF_8));
    }

    private static boolean isConnectionError(TransportException e) {
        return e instanceof TransportConnectionException;
    }

    private static TransportConnectionException connectionError(
            String code, String message, Throwable cause) {
        return new TransportConnectionException(code, message, cause);
    }

    private static TransportServerException serverError(
            String code, String message, int status, Throwable cause) {
        return new TransportServerException(code, message, status, cause);
    }
}
