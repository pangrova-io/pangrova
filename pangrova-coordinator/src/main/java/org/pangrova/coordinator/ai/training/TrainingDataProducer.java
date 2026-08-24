package org.pangrova.coordinator.ai.training;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.pangrova.spi.security.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Arrow Flight producer that adapts the {@link TrainingDataServer} business logic
 * to the Arrow Flight SDK callback interface.
 *
 * <p>Handles the gRPC-level protocol (getInfo, doGet) and delegates authentication,
 * authorization, concurrency control, and request parsing to {@link TrainingDataServer}.</p>
 *
 * <p>Data flow: ML client → gRPC → this producer → TrainingDataServer (auth/parse)
 * → TrainingDataScanner (connector read) → columnar IPC batches → gRPC stream.</p>
 */
final class TrainingDataProducer extends NoOpFlightProducer {

    private static final Logger LOG = LoggerFactory.getLogger(TrainingDataProducer.class);

    private final TrainingDataServer server;
    private final TrainingDataScanner scanner;
    private final BufferAllocator allocator;
    private final Location serverLocation;

    TrainingDataProducer(TrainingDataServer server,
                         TrainingDataScanner scanner,
                         BufferAllocator allocator,
                         Location serverLocation) {
        this.server = Objects.requireNonNull(server, "server must not be null");
        this.scanner = Objects.requireNonNull(scanner, "scanner must not be null");
        this.allocator = Objects.requireNonNull(allocator, "allocator must not be null");
        this.serverLocation = Objects.requireNonNull(serverLocation, "serverLocation must not be null");
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        UserIdentity identity = resolveIdentity(context);
        byte[] commandBytes = descriptor.getCommand();

        List<TrainingDataServer.EndpointTicket> endpoints = server.getFlightInfo(commandBytes, identity);

        List<FlightEndpoint> flightEndpoints = endpoints.stream()
                .map(ep -> new FlightEndpoint(
                        new Ticket(ep.ticket().getBytes(StandardCharsets.UTF_8)),
                        serverLocation))
                .toList();

        Schema schema = scanner.resolveSchema(commandBytes);

        return new FlightInfo(schema, descriptor, flightEndpoints, -1, -1);
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        UserIdentity identity = resolveIdentity(context);
        byte[] ticketBytes = ticket.getBytes();

        TrainingDataServer.StreamContext streamContext = server.openStream(ticketBytes, identity);
        long bytesServed = 0;

        try {
            bytesServed = streamBatches(ticketBytes, listener);
            listener.completed();
        } catch (Exception e) {
            LOG.error("event=training_stream_failed streamId={} error={}",
                    streamContext.streamId(), e.getMessage(), e);
            listener.error(e);
        } finally {
            server.closeStream(streamContext.streamId(), bytesServed);
        }
    }

    private long streamBatches(byte[] ticketBytes, ServerStreamListener listener) throws Exception {
        long totalBytes = 0;
        VectorSchemaRoot outputRoot = null;

        try (TrainingDataScanner.ScanSession session = scanner.openScan(ticketBytes, allocator)) {
            byte[] firstBatch = session.nextBatch();
            if (firstBatch == null || firstBatch.length == 0) {
                sendEmptyStream(listener);
                return 0;
            }

            outputRoot = createOutputRoot(firstBatch);
            listener.start(outputRoot);
            VectorLoader loader = new VectorLoader(outputRoot);

            totalBytes += decodeBatches(firstBatch, loader, listener);

            byte[] batch;
            while ((batch = session.nextBatch()) != null && batch.length > 0) {
                totalBytes += decodeBatches(batch, loader, listener);
            }
        } finally {
            if (outputRoot != null) {
                outputRoot.clear();
                outputRoot.close();
            }
        }

        return totalBytes;
    }

    private void sendEmptyStream(ServerStreamListener listener) {
        Schema emptySchema = new Schema(Collections.emptyList());
        try (VectorSchemaRoot root = VectorSchemaRoot.create(emptySchema, allocator)) {
            listener.start(root);
            listener.completed();
        }
    }

    private VectorSchemaRoot createOutputRoot(byte[] batchData) throws Exception {
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(batchData), allocator)) {
            Schema schema = reader.getVectorSchemaRoot().getSchema();
            return VectorSchemaRoot.create(schema, allocator);
        }
    }

    private long decodeBatches(byte[] batchData, VectorLoader loader,
                               ServerStreamListener listener) throws Exception {
        long bytes = batchData.length;
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(batchData), allocator)) {
            VectorSchemaRoot srcRoot = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                VectorUnloader unloader = new VectorUnloader(srcRoot);
                try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
                    loader.load(recordBatch);
                }
                listener.putNext();
            }
        }
        return bytes;
    }

    private UserIdentity resolveIdentity(CallContext context) {
        String peerIdentity = context.peerIdentity();
        if (peerIdentity == null || peerIdentity.isBlank()) {
            peerIdentity = "anonymous";
        }
        return new UserIdentity(peerIdentity, Set.of(), Map.of(), Instant.now());
    }
}
