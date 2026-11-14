package org.pangrova.testing.nativefixture;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.pangrova.protocol.v2.TaskRequest;

import java.nio.charset.StandardCharsets;

/** Holds an actual unconsumed Flight RPC, never inferred from a client batch owner. */
public final class ActualPausedNativeResultReader implements AutoCloseable {
    private final RootAllocator allocator = new RootAllocator(ActualNativeTaskFixture.TASK_MEMORY_BYTES);
    private final FlightClient client;
    private final FlightStream stream;

    public ActualPausedNativeResultReader(ActualNativeTaskFixture runtime, TaskRequest request) throws Exception {
        FlightClient startingClient = null;
        FlightStream startingStream = null;
        try {
            startingClient = FlightClient.builder(allocator,
                    Location.forGrpcInsecure("127.0.0.1", runtime.resultPort())).build();
            startingStream = startingClient.getStream(new Ticket(
                    runtime.resultTicket(request).getBytes(StandardCharsets.UTF_8)));
            if (!startingStream.next() || startingStream.getRoot().getRowCount() <= 0) {
                throw new IllegalStateException("Actual Flight producer returned no first batch");
            }
            client = startingClient;
            stream = startingStream;
        } catch (Exception | Error failure) {
            closeAfterFailure(startingStream, failure);
            closeAfterFailure(startingClient, failure);
            closeAfterFailure(allocator, failure);
            throw failure;
        }
    }

    public int firstBatchRows() { return stream.getRoot().getRowCount(); }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        for (AutoCloseable owner : new AutoCloseable[] {stream, client, allocator}) {
            try {
                owner.close();
            } catch (Exception cleanup) {
                if (failure == null) failure = cleanup;
                else failure.addSuppressed(cleanup);
            }
        }
        if (failure != null) throw failure;
    }

    private static void closeAfterFailure(AutoCloseable owner, Throwable failure) {
        if (owner == null) return;
        try { owner.close(); }
        catch (Exception | Error cleanup) { failure.addSuppressed(cleanup); }
    }
}
