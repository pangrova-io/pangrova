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
package org.pangrova.transport.client;

import org.pangrova.transport.client.exception.TransportConnectionException;
import org.pangrova.transport.client.exception.TransportException;
import org.pangrova.transport.client.exception.TransportServerException;

import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.transport.client.security.ResultTransportSecurity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Opens forward-only cursors over Flight result streams.
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
    public static final long BATCH_IDLE_TIMEOUT_SECONDS = 120;

    /**
     * Daemon-thread executor for async {@code stream.next()} calls.
     * Submitting the blocking gRPC read to a dedicated thread allows the
     * calling thread to enforce idle timeout via {@link Future#get(long, TimeUnit)}.
     */
    private final ExecutorService batchReadExecutor =
            Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "flight-batch-reader");
                t.setDaemon(true);
                return t;
            });

    private final BufferAllocator allocator;
    private final ResultTransportSecurity security;
    private final ResultTransportLifecycle lifecycle = new ResultTransportLifecycle();

    public FlightResultFetcher() {
        this(ResultTransportSecurity.systemTrust(false));
    }

    /**
     * Creates a result fetcher with a policy independent of REST trust stores.
     *
     * @param security immutable result TLS policy and optional PEM material
     */
    public FlightResultFetcher(ResultTransportSecurity security) {
        this(security, DEFAULT_ALLOCATOR_LIMIT);
    }

    /**
     * Constructor with a configurable Arrow allocator upper bound. The limit is a
     * hard cap on client-side result buffering (defaults to 512 MiB via the other
     * constructors); raise or lower it per deployment to bound client memory. A
     * non-positive value is rejected so the cap can never be silently disabled.
     *
     * @param security immutable result TLS policy and optional PEM material
     * @param allocatorLimitBytes positive hard bound for result buffering
     */
    public FlightResultFetcher(ResultTransportSecurity security, long allocatorLimitBytes) {
        if (allocatorLimitBytes <= 0) {
            throw new IllegalArgumentException("allocatorLimitBytes must be positive");
        }
        this.security = Objects.requireNonNull(security, "security");
        this.allocator = new RootAllocator(allocatorLimitBytes);
    }

    /** The Arrow allocator's hard upper bound in bytes (package-private for tests). */
    long allocatorLimitBytes() {
        return allocator.getLimit();
    }

    long peakAllocatedBytes() {
        return allocator.getPeakMemoryAllocation();
    }

    /** Opens the stream and reads through the first data batch or EOF under the caller's deadline. */
    public ResultCursor open(ResultEndpoint endpoint, long deadlineEpochMs) {
        Objects.requireNonNull(endpoint, "endpoint");
        var operation = lifecycle.enter();
        ResultTransportLifecycle.ClientLease lease = null;
        ResultTransportLifecycle.StreamOwner stream = null;
        try {
            Location location = resolveLocation(endpoint);
            lease = lifecycle.client(poolKey(endpoint), () -> buildClient(location));
            stream = lifecycle.stream(openStream(lease.client(), endpoint, deadlineEpochMs), batchReadExecutor);
            var cursor = new FlightCursor(endpoint, deadlineEpochMs, operation, lease, stream);
            cursor.prime();
            return cursor;
        } catch (RuntimeException failure) {
            TransportException error = readError(endpoint, deadlineEpochMs, failure, lease);
            closeAfterFailure(stream, error);
            closeAfterFailure(lease, error);
            operation.close();
            throw error;
        } catch (Error failure) {
            closeAfterFailure(stream, failure);
            closeAfterFailure(lease, failure);
            operation.close();
            throw failure;
        }
    }

    private static void closeAfterFailure(AutoCloseable resource, Throwable failure) {
        if (resource == null) return;
        try {
            resource.close();
        } catch (Exception cleanup) {
            failure.addSuppressed(cleanup);
        }
    }

    private static TransportException readError(ResultEndpoint endpoint, long deadlineEpochMs,
            RuntimeException failure, ResultTransportLifecycle.ClientLease lease) {
        TransportException error;
        if (failure instanceof TransportException transport) {
            error = transport;
        } else if (deadlineEpochMs != Long.MAX_VALUE
                && (System.currentTimeMillis() >= deadlineEpochMs
                        || extractFlightStatusCode(failure) == FlightStatusCode.TIMED_OUT)) {
            error = queryDeadlineExceeded(endpoint, failure);
        } else {
            error = classifyError(endpoint, failure);
        }
        if (lease != null && error instanceof TransportConnectionException) {
            lease.retire();
        }
        return error;
    }

    private static FlightStream openStream(
            FlightClient client, ResultEndpoint endpoint, long deadlineEpochMs) {
        if (deadlineEpochMs == Long.MAX_VALUE) {
            return client.getStream(ticket(endpoint));
        }
        long remainingMs = deadlineEpochMs - System.currentTimeMillis();
        if (remainingMs <= 0) {
            throw queryDeadlineExceeded(endpoint, null);
        }
        return client.getStream(ticket(endpoint),
                CallOptions.timeout(remainingMs, TimeUnit.MILLISECONDS));
    }

    private final class FlightCursor implements ResultCursor {
        private final ResultEndpoint endpoint;
        private final long deadlineEpochMs;
        private final ResultTransportLifecycle.Operation operation;
        private final ResultTransportLifecycle.ClientLease lease;
        private final ResultTransportLifecycle.StreamOwner owner;
        private final FlightStream stream;
        private VectorSchemaRoot root;
        private List<ColumnDescriptor> columns;
        private TransportException readFailure;
        private int row = -1;
        private boolean exhausted;
        private boolean closed;

        FlightCursor(ResultEndpoint endpoint, long deadlineEpochMs,
                ResultTransportLifecycle.Operation operation, ResultTransportLifecycle.ClientLease lease,
                ResultTransportLifecycle.StreamOwner owner) {
            this.endpoint = endpoint;
            this.deadlineEpochMs = deadlineEpochMs;
            this.operation = operation;
            this.lease = lease;
            this.owner = owner;
            this.stream = owner.stream();
        }

        void prime() {
            loadBatch();
            if (root == null) {
                throw serverError("TRANSPORT_RESULT_SCHEMA_MISSING", "Flight DoGet ended without a schema at "
                        + detail(endpoint), -1, null);
            }
            columns = List.copyOf(ArrowBatchDecoder.buildColumns(root.getSchema()));
        }

        @Override
        public List<ColumnDescriptor> columns() {
            return columns;
        }

        @Override
        public boolean next() {
            if (closed) throw new IllegalStateException("Result cursor is closed");
            if (readFailure != null) throw readFailure;
            if (exhausted) return false;
            try {
                if (deadlineEpochMs != Long.MAX_VALUE && System.currentTimeMillis() >= deadlineEpochMs) {
                    throw queryDeadlineExceeded(endpoint, null);
                }
                if (++row >= root.getRowCount()) {
                    loadBatch();
                    row = 0;
                }
                return !exhausted;
            } catch (RuntimeException failure) {
                readFailure = readError(endpoint, deadlineEpochMs, failure, lease);
                throw readFailure;
            }
        }

        private void loadBatch() {
            while (awaitNextBatch(stream, endpoint, deadlineEpochMs)) {
                if (root == null && stream.hasRoot()) root = stream.getRoot();
                if (root != null && root.getRowCount() > 0) return;
            }
            if (root == null && stream.hasRoot()) root = stream.getRoot();
            exhausted = true;
        }

        @Override
        public Object value(int column) {
            if (closed || exhausted || row < 0) throw new IllegalStateException("Result cursor has no current row");
            if (readFailure != null) throw readFailure;
            try {
                return ArrowBatchDecoder.extractValue(root.getVector(column), row, columns.get(column));
            } catch (TransportException failure) {
                readFailure = failure;
                throw failure;
            }
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            try (operation; lease; owner) {
                root = null;
            }
        }
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
    private boolean awaitNextBatch(
            FlightStream stream, ResultEndpoint endpoint, long deadlineEpochMs) {
        long waitNanos = readWaitNanos(deadlineEpochMs);
        if (waitNanos <= 0) {
            throw queryDeadlineExceeded(endpoint, null);
        }
        Future<Boolean> future = lifecycle.read(batchReadExecutor, stream);
        try {
            return future.get(waitNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            stream.cancel("Result read deadline or idle timeout", e);
            future.cancel(false); // RPC cancellation wakes the SDK; its real reader retains ownership until exit.
            if (deadlineEpochMs != Long.MAX_VALUE
                    && System.currentTimeMillis() >= deadlineEpochMs) {
                throw queryDeadlineExceeded(endpoint, e);
            }
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

    static long readWaitNanos(long deadlineEpochMs) {
        long idleNanos = TimeUnit.SECONDS.toNanos(BATCH_IDLE_TIMEOUT_SECONDS);
        if (deadlineEpochMs == Long.MAX_VALUE) {
            return idleNanos;
        }
        long remainingMs = deadlineEpochMs - System.currentTimeMillis();
        if (remainingMs <= 0) {
            return 0;
        }
        return Math.min(idleNanos, TimeUnit.MILLISECONDS.toNanos(remainingMs));
    }

    private static TransportConnectionException queryDeadlineExceeded(
            ResultEndpoint endpoint, Throwable cause) {
        return new TransportConnectionException(
                "TRANSPORT_QUERY_DEADLINE_EXCEEDED",
                "Flight DoGet query deadline exceeded for " + detail(endpoint), cause);
    }

    static TransportException classifyError(ResultEndpoint endpoint, Exception e) {
        String msg = e.getMessage();
        LOG.warn("event=transport_error endpoint={}:{} errorClass={} errorMessage={}",
                endpoint.host(), endpoint.port(),
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

    /**
     * Builds a FlightClient for the given location.
     *
     * <p>The official SDK channel builder and client share one owned channel;
     * termination is verified before releasing client allocation owners.</p>
     */
    private ResultTransportClientOwner buildClient(Location location) {
        return ResultTransportClientProvider.open(allocator, location, security, lifecycle::registerClient);
    }

    private Location resolveLocation(ResultEndpoint endpoint) {
        ResultProtocol protocol = ResultProtocol.fromValue(endpoint.protocol());
        if (security.requireTls() && !protocol.requiresTls()) {
            throw connectionError("TRANSPORT_TLS_REQUIRED",
                    "TLS is required but the result endpoint advertises plaintext transport", null);
        }
        if (protocol.requiresTls()) {
            return Location.forGrpcTls(endpoint.host(), endpoint.port());
        }
        return Location.forGrpcInsecure(endpoint.host(), endpoint.port());
    }

    /** Seals admission; releases owners after actual streams and readers exit; failures retain owners for retry. */
    @Override
    public void close() {
        if (!lifecycle.beginClose()) return;
        boolean completed = false;
        try {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Interrupted before result cleanup");
            }
            lifecycle.closeStreams();
            lifecycle.closeClients();
            batchReadExecutor.shutdown();
            if (!batchReadExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                throw connectionError("TRANSPORT_LIFECYCLE_FAILED",
                        "Result read executor has not exited; retry close", null);
            }
            allocator.close();
            completed = true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw connectionError("TRANSPORT_LIFECYCLE_FAILED",
                    "Result close was interrupted; retry close", interrupted);
        } finally {
            lifecycle.endClose(completed);
        }
    }

    static String poolKey(ResultEndpoint endpoint) {
        return endpoint.protocol() + "://" + endpoint.host() + ":" + endpoint.port();
    }

    private static String detail(ResultEndpoint endpoint) {
        return endpoint.host() + ":" + endpoint.port();
    }

    private static Ticket ticket(ResultEndpoint endpoint) {
        return new Ticket(endpoint.ticket().getBytes(StandardCharsets.UTF_8));
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
