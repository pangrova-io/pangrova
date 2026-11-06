package org.pangrova.transport.client;

import io.grpc.ManagedChannel;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightGrpcUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.pangrova.common.concurrent.NotThreadSafe;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Owns the actual channel before SDK construction; lifecycle admission serializes cleanup with all client leases. */
@NotThreadSafe
final class ResultTransportClientOwner implements AutoCloseable {
    private final ManagedChannel channel;
    private FlightClient client;

    ResultTransportClientOwner(ManagedChannel channel) { this.channel = Objects.requireNonNull(channel); }

    void initialize(BufferAllocator allocator) {
        client = FlightGrpcUtils.createFlightClient(allocator, channel);
    }

    FlightClient client() { return Objects.requireNonNull(client, "Result client is not initialized"); }
    boolean channelTerminated() { return channel.isTerminated(); }

    @Override public void close() throws InterruptedException {
        channel.shutdown();
        if (!channel.awaitTermination(5, TimeUnit.SECONDS) || !channelTerminated()) {
            throw new TransportConnectionException("TRANSPORT_LIFECYCLE_FAILED",
                    "Result channel is still owned; client allocator is retained for retry", null);
        }
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
