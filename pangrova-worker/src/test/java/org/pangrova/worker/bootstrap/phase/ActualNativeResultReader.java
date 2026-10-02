package org.pangrova.worker.bootstrap.phase;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.pangrova.protocol.v2.TaskRequest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Assertions consume the real Native Flight stream; no result is synthesized. */
public final class ActualNativeResultReader {
    private ActualNativeResultReader() {}

    public record Received(int batches, List<Long> values, long rows) {}

    public static Received read(ActualNativeTaskFixture runtime, TaskRequest request) throws Exception {
        var values = new ArrayList<Long>();
        int batches = 0;
        long rows = 0;
        try (var allocator = new RootAllocator(ActualNativeTaskFixture.TASK_MEMORY_BYTES);
             var client = FlightClient.builder(allocator,
                     Location.forGrpcInsecure("127.0.0.1", runtime.resultPort())).build();
             var stream = client.getStream(new Ticket(
                     runtime.resultTicket(request).getBytes(StandardCharsets.UTF_8)))) {
            while (stream.next()) {
                batches++;
                var root = stream.getRoot();
                rows = Math.addExact(rows, root.getRowCount());
                if (root.getFieldVectors().size() != 1) {
                    throw new AssertionError("Expected the actual one-column values plan output");
                }
                for (int row = 0; row < root.getRowCount(); row++) {
                    Object value = root.getVector(0).getObject(row);
                    if (!(value instanceof Long number)) {
                        throw new AssertionError("Native returned an unexpected values type: " + value);
                    }
                    values.add(number);
                }
            }
        }
        return new Received(batches, List.copyOf(values), rows);
    }
}
