package org.pangrova.worker.transport;

import org.pangrova.spi.transport.DataTicket;
import org.pangrova.spi.transport.ResultTooLargeException;
import org.pangrova.worker.exchange.source.ColumnarBatchConsumer;
import org.pangrova.worker.exchange.source.ExchangeBatchConsumer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Transport gRPC client wrapping the data exchange SDK.
 * Sends DoGet requests and receives streaming RecordBatch data.
 *
 * <p>Tracks compressed and uncompressed bytes transferred for
 * IPC transport observability via {@link #getCompressedBytesTransferred()}
 * and {@link #getUncompressedBytesTransferred()}.</p>
 */
public class TransportClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TransportClient.class);
    private final String host;
    private final int port;
    private final long maxResultBytes;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final BufferAllocator allocator;
    private final boolean ownsAllocator;
    private final org.apache.arrow.flight.FlightClient delegate;
    private final AtomicLong compressedBytesTransferred = new AtomicLong(0);
    private final AtomicLong uncompressedBytesTransferred = new AtomicLong(0);

    /** Primary constructor — accepts immutable config record. */
    public TransportClient(TransportClientConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        this.host = config.host();
        this.port = config.port();
        this.maxResultBytes = config.maxResultBytes();
        if (config.allocator() != null) {
            this.allocator = config.allocator();
            this.ownsAllocator = false;
        } else {
            long allocatorLimit = Math.max(maxResultBytes, 16L * 1024 * 1024);
            this.allocator = new RootAllocator(allocatorLimit);
            this.ownsAllocator = true;
        }
        this.delegate = buildClient();
        this.connected.set(true);
        LOG.info("event=transport_client_connected host={} port={} maxResultBytes={} externalAllocator={}",
                host, port, maxResultBytes, !ownsAllocator);
    }

    /** Convenience: host + port with defaults. */
    public TransportClient(String host, int port) {
        this(TransportClientConfig.of(host, port));
    }

    /** Convenience: host + port + custom result size limit. */
    public TransportClient(String host, int port, long maxResultBytes) {
        this(TransportClientConfig.of(host, port, maxResultBytes));
    }

    /** Convenience: host + port + external allocator + result size limit. */
    public TransportClient(String host, int port, BufferAllocator allocator, long maxResultBytes) {
        this(TransportClientConfig.of(host, port, allocator, maxResultBytes));
    }

    /**
     * Sends DoGet to the transport server and returns all batches as columnar IPC bytes.
     */
    public byte[] doGet(DataTicket ticket) {
        Objects.requireNonNull(ticket, "ticket must not be null");
        Ticket arrowTicket = new Ticket(ticket.serialize());
        try (FlightStream stream = delegate.getStream(arrowTicket)) {
            return collectStreamAsBytes(stream);
        } catch (Exception e) {
            throw new IllegalStateException("DoGet failed for ticket queryId=" + ticket.queryId(), e);
        }
    }

    /**
     * Streams DoGet results batch-by-batch to the consumer, avoiding
     * full materialization. Each batch is serialized as columnar IPC and
     * delivered independently, keeping memory bounded.
     */
    public void doGetStream(DataTicket ticket, ExchangeBatchConsumer consumer) {
        Objects.requireNonNull(ticket, "ticket must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");
        Ticket arrowTicket = new Ticket(ticket.serialize());
        try (FlightStream stream = delegate.getStream(arrowTicket)) {
            streamBatches(stream, consumer);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "DoGetStream failed for ticket queryId=" + ticket.queryId(), e);
        }
    }

    /**
     * Streams DoGet results batch-by-batch, passing the raw VectorSchemaRoot
     * directly to the consumer without intermediate byte[] serialization.
     * Enables zero-copy exchange when combined with columnar data interface export.
     *
     * <p>The VectorSchemaRoot passed to the consumer is owned by the FlightStream
     * and must not be retained beyond the accept() call.</p>
     */
    public void doGetStreamZeroCopy(DataTicket ticket, ColumnarBatchConsumer consumer) {
        Objects.requireNonNull(ticket, "ticket must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");
        Ticket arrowTicket = new Ticket(ticket.serialize());
        try (FlightStream stream = delegate.getStream(arrowTicket)) {
            streamBatchesZeroCopy(stream, consumer);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "DoGetStreamZeroCopy failed for ticket queryId=" + ticket.queryId(), e);
        }
    }

    public boolean isConnected() {
        return connected.get();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public void close() {
        if (!connected.compareAndSet(true, false)) {
            return;
        }
        closeDelegate();
        if (ownsAllocator) {
            allocator.close();
        }
        LOG.info("event=transport_client_closed host={} port={}", host, port);
    }

    private org.apache.arrow.flight.FlightClient buildClient() {
        Location location = Location.forGrpcInsecure(host, port);
        return org.apache.arrow.flight.FlightClient.builder(allocator, location).build();
    }

    private static final int STREAM_BUFFER_INITIAL_SIZE = 128 * 1024;

    private static final ThreadLocal<ByteArrayOutputStream> STREAM_BAOS =
            ThreadLocal.withInitial(() -> new ByteArrayOutputStream(STREAM_BUFFER_INITIAL_SIZE));

    private byte[] collectStreamAsBytes(FlightStream stream) throws IOException {
        VectorSchemaRoot root = stream.getRoot();
        ByteArrayOutputStream baos = STREAM_BAOS.get();
        baos.reset();
        long uncompressedTotal = 0;
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
            writer.start();
            while (stream.next()) {
                uncompressedTotal += estimateUncompressedSize(root);
                writer.writeBatch();
                checkResultSize(baos.size());
            }
            writer.end();
        }
        long compressedSize = baos.size();
        compressedBytesTransferred.addAndGet(compressedSize);
        uncompressedBytesTransferred.addAndGet(uncompressedTotal);
        return baos.toByteArray();
    }

    private void streamBatches(FlightStream stream, ExchangeBatchConsumer consumer) throws IOException {
        VectorSchemaRoot root = stream.getRoot();
        while (stream.next()) {
            long uncompressed = estimateUncompressedSize(root);
            byte[] batchBytes = serializeSingleBatch(root);
            compressedBytesTransferred.addAndGet(batchBytes.length);
            uncompressedBytesTransferred.addAndGet(uncompressed);
            consumer.accept(batchBytes);
        }
    }

    private void streamBatchesZeroCopy(FlightStream stream, ColumnarBatchConsumer consumer) {
        VectorSchemaRoot root = stream.getRoot();
        while (stream.next()) {
            long uncompressed = estimateUncompressedSize(root);
            uncompressedBytesTransferred.addAndGet(uncompressed);
            try {
                consumer.accept(root);
            } catch (Exception e) {
                throw new IllegalStateException("Zero-copy batch consumer failed", e);
            }
        }
    }

    private byte[] serializeSingleBatch(VectorSchemaRoot root) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return baos.toByteArray();
    }

    private void checkResultSize(long currentSize) {
        if (currentSize > maxResultBytes) {
            throw new ResultTooLargeException(maxResultBytes, currentSize);
        }
    }

    /**
     * Cumulative compressed bytes received over this client's lifetime.
     * Represents the wire-level (IPC serialized) byte count.
     */
    public long getCompressedBytesTransferred() {
        return compressedBytesTransferred.get();
    }

    /**
     * Cumulative uncompressed bytes received over this client's lifetime.
     * Estimated from in-memory VectorSchemaRoot buffer sizes before serialization.
     */
    public long getUncompressedBytesTransferred() {
        return uncompressedBytesTransferred.get();
    }

    /**
     * Resets both byte transfer counters to zero.
     */
    public void resetBytesTransferred() {
        compressedBytesTransferred.set(0);
        uncompressedBytesTransferred.set(0);
    }

    /**
     * Estimates uncompressed in-memory size of a VectorSchemaRoot batch
     * by summing all field vector buffer sizes.
     */
    private static long estimateUncompressedSize(VectorSchemaRoot root) {
        return root.getFieldVectors().stream()
                .mapToLong(v -> v.getBufferSize())
                .sum();
    }

    private void closeDelegate() {
        try {
            delegate.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=transport_client_close_interrupted host={} port={}", host, port);
        }
    }
}
