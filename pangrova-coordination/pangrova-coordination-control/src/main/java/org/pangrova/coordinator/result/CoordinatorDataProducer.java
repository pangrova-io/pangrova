package org.pangrova.coordinator.result;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/** Sends the Coordinator's bounded Arrow batch directly to one Flight reader. */
public final class CoordinatorDataProducer extends NoOpFlightProducer {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorDataProducer.class);
    private static final Schema ADVISORY_SCHEMA = new Schema(List.of());
    private final CoordinatorResultBuffer buffer;

    public CoordinatorDataProducer(CoordinatorResultBuffer buffer) {
        this.buffer = Objects.requireNonNull(buffer, "buffer");
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        String queryId = new String(ticket.getBytes(), StandardCharsets.UTF_8);
        try (var result = buffer.claim(queryId)) {
            if (result == null) {
                listener.error(CallStatus.NOT_FOUND.withDescription("Control result is unavailable")
                        .toRuntimeException());
                return;
            }
            if (listener.isCancelled()) return;
            // Flight stages the outgoing data before releasing the query's allocator.
            listener.setUseZeroCopy(false);
            listener.start(result.root());
            listener.putNext();
        } catch (RuntimeException failure) {
            LOG.warn("event=control_result_stream_failed queryId={}", queryId, failure);
            listener.error(failure);
            return;
        }
        listener.completed();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        var endpoint = new FlightEndpoint(new Ticket(descriptor.getCommand()));
        return new FlightInfo(ADVISORY_SCHEMA, descriptor, List.of(endpoint), -1, -1);
    }
}
