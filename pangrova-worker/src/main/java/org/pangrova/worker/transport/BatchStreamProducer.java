package org.pangrova.worker.transport;

import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.spi.transport.DataProducer;
import org.pangrova.spi.transport.DataTicket;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Data exchange producer that adapts {@link DataProducer} to the data exchange protocol.
 * Production-grade replacement with bounded backpressure and timeout protection.
 */
public class BatchStreamProducer implements FlightProducer {

    private static final Logger LOG = LoggerFactory.getLogger(BatchStreamProducer.class);

    /** Maximum total wait when polling for the FIRST batch (or EOF) before
     * giving up and surfacing TRANSPORT_ERROR. Previously this was an
     * unbounded loop, which could deadlock the JDBC driver when the
     * underlying task disappeared (e.g. evicted by cleanup). */
    private static final long FIRST_BATCH_TIMEOUT_MILLIS =
            Long.getLong("pangrova.flight.firstBatchTimeoutMillis", 120_000L);

    /** Maximum wait between subsequent batches before erroring out. */
    private static final long NEXT_BATCH_TIMEOUT_MILLIS =
            Long.getLong("pangrova.flight.nextBatchTimeoutMillis", 300_000L);

    /** Backoff between not-ready polls. Avoids burning CPU when the task is
     * still producing data. */
    private static final long POLL_BACKOFF_MILLIS =
            Long.getLong("pangrova.flight.pollBackoffMillis", 50L);

    /** Maximum buffered batches between producer and Flight stream consumer.
     * Producer blocks on put() when full, providing natural backpressure. */
    private static final int MAX_BUFFERED_BATCHES =
            Integer.getInteger("pangrova.flight.maxBufferedBatches", 4);

    private static final byte[] POISON_PILL = new byte[0];

    /**
     * Shared bounded thread pool for batch-fill producers. Avoids spawning a new
     * untracked thread per Flight getStream request (F-WORKER-04). Bounded to
     * prevent OS thread exhaustion under load.
     */
    private static final ExecutorService BATCH_FILL_EXECUTOR = new ThreadPoolExecutor(
            4, 16, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(64),
            r -> {
                Thread t = new Thread(r);
                t.setName("flight-batch-fill-pool");
                t.setDaemon(true);
                return t;
            },
            new ThreadPoolExecutor.CallerRunsPolicy());

    private final DataProducer producer;
    private final BufferAllocator allocator;
    private final TransportTicketSigner ticketSigner;

    public BatchStreamProducer(DataProducer producer, BufferAllocator allocator,
                               TransportTicketSigner ticketSigner) {
        this.producer = producer;
        this.allocator = allocator;
        this.ticketSigner = ticketSigner;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        VectorSchemaRoot outputRoot = null;
        try {
            byte[] ticketBytes = ticket.getBytes();
            if (ticketSigner != null) {
                ticketSigner.validate(ticketBytes);
            }
            DataTicket dataTicket = deserializeTicket(ticketBytes);

            byte[] firstBatch = pollUntilDataOrEnd(dataTicket, FIRST_BATCH_TIMEOUT_MILLIS);
            if (firstBatch == null || firstBatch.length == 0) {
                sendEmptyResponse(listener);
                return;
            }

            outputRoot = createOutputRoot(firstBatch, allocator);
            listener.start(outputRoot);
            VectorLoader loader = new VectorLoader(outputRoot);
            decodeBatches(firstBatch, loader, listener);
            streamRemainingBatches(dataTicket, loader, listener);
            listener.completed();
        } catch (Exception e) {
            LOG.error("event=flight_get_stream_failed error={}", e.getMessage(), e);
            listener.error(e);
        } finally {
            if (outputRoot != null) {
                outputRoot.clear();
                outputRoot.close();
            }
        }
    }

    private static DataTicket deserializeTicket(byte[] ticketBytes) {
        try {
            return DataTicket.deserialize(ticketBytes);
        } catch (IllegalArgumentException e) {
            String raw = new String(ticketBytes, java.nio.charset.StandardCharsets.UTF_8);
            LOG.debug("event=raw_ticket_fallback raw={}", raw);
            return new DataTicket(raw, "result", 0, DataTicket.TicketType.RESULT);
        }
    }

    /**
     * Polls producer until data, EOF, or timeout. null = not-ready (retry).
     * Bounded by {@code timeoutMillis} so a vanished task can no longer
     * wedge the Flight handler thread indefinitely.
     */
    private byte[] pollUntilDataOrEnd(DataTicket ticket, long timeoutMillis) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS
                .toNanos(timeoutMillis);
        while (!Thread.currentThread().isInterrupted()) {
            byte[] data = producer.doGet(ticket);
            if (data != null) {
                return data;
            }
            if (System.nanoTime() >= deadline) {
                LOG.warn("event=flight_poll_timeout ticket={} timeoutMs={}",
                        ticket, timeoutMillis);
                throw CallStatus.TIMED_OUT
                        .withDescription("Timed out waiting for first batch after "
                                + timeoutMillis + "ms")
                        .toRuntimeException();
            }
            sleepForBackoff();
        }
        return null;
    }

    private static void sleepForBackoff() {
        ThreadUtils.sleepQuietly(POLL_BACKOFF_MILLIS);
    }

    /**
     * Continues streaming batches after the first until EOF (empty byte[]).
     * Uses a bounded queue (capacity = MAX_BUFFERED_BATCHES) between a
     * producer thread and the Flight stream consumer. The producer blocks
     * on queue.put() when the buffer is full, providing natural backpressure
     * to the native execution layer.
     */
    private void streamRemainingBatches(DataTicket ticket, VectorLoader loader,
                                         ServerStreamListener listener) throws Exception {
        ArrayBlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(MAX_BUFFERED_BATCHES);
        // F-WORKER-04: Use shared ExecutorService instead of per-request untracked Thread.
        Future<?> producerFuture = BATCH_FILL_EXECUTOR.submit(() -> fillQueue(ticket, queue));
        try {
            consumeQueue(queue, loader, listener);
        } finally {
            producerFuture.cancel(true);
        }
    }

    private void fillQueue(DataTicket ticket, ArrayBlockingQueue<byte[]> queue) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(NEXT_BATCH_TIMEOUT_MILLIS);
        try {
            while (!Thread.currentThread().isInterrupted()) {
                byte[] data = producer.doGet(ticket);
                if (data != null && data.length == 0) {
                    queue.put(POISON_PILL);
                    return;
                }
                if (data == null) {
                    if (System.nanoTime() >= deadline) {
                        queue.put(POISON_PILL);
                        return;
                    }
                    sleepForBackoff();
                    continue;
                }
                queue.put(data);
                deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(NEXT_BATCH_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void consumeQueue(ArrayBlockingQueue<byte[]> queue, VectorLoader loader,
                               ServerStreamListener listener) throws Exception {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] data = queue.poll(NEXT_BATCH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (data == null) {
                LOG.warn("event=flight_consumer_timeout timeoutMs={}", NEXT_BATCH_TIMEOUT_MILLIS);
                throw CallStatus.TIMED_OUT
                        .withDescription("Timed out waiting for next batch after "
                                + NEXT_BATCH_TIMEOUT_MILLIS + "ms")
                        .toRuntimeException();
            }
            if (data == POISON_PILL) {
                return;
            }
            decodeBatches(data, loader, listener);
        }
    }

    private void sendEmptyResponse(ServerStreamListener listener) {
        Schema emptySchema = new Schema(java.util.Collections.emptyList());
        try (VectorSchemaRoot root = VectorSchemaRoot.create(emptySchema, allocator)) {
            listener.start(root);
            listener.completed();
        }
    }

    private static VectorSchemaRoot createOutputRoot(byte[] batchData,
                                                      BufferAllocator alloc) throws Exception {
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new java.io.ByteArrayInputStream(batchData), alloc)) {
            Schema schema = reader.getVectorSchemaRoot().getSchema();
            return VectorSchemaRoot.create(schema, alloc);
        }
    }

    private void decodeBatches(byte[] batchData, VectorLoader loader,
                                ServerStreamListener listener) throws Exception {
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new java.io.ByteArrayInputStream(batchData), allocator)) {
            VectorSchemaRoot srcRoot = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                VectorUnloader unloader = new VectorUnloader(srcRoot);
                try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
                    loader.load(batch);
                }
                listener.putNext();
            }
        }
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        listener.onError(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        Schema advisorySchema = new Schema(java.util.Collections.emptyList());
        Ticket arrowTicket = new Ticket(descriptor.getCommand());
        FlightEndpoint endpoint = new FlightEndpoint(arrowTicket);
        return new FlightInfo(advisorySchema, descriptor, List.of(endpoint), -1, -1);
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        throw CallStatus.UNIMPLEMENTED.withDescription("Put not supported").toRuntimeException();
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        listener.onError(CallStatus.UNIMPLEMENTED.withDescription("Action not supported").toRuntimeException());
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        listener.onError(CallStatus.UNIMPLEMENTED.withDescription("Actions not supported").toRuntimeException());
    }
}
