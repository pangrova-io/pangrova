package org.pangrova.worker.shuffle;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.spi.annotation.ThreadSafe;

/**
 * Routes incoming data retrieval requests to the correct task's shuffle data
 * based on the multiplexed ticket format.
 *
 * <h3>Design: single shared server per worker</h3>
 * <p>The Pangrova shuffle layer uses a <strong>single native data exchange server per worker</strong>
 * (one port) that multiplexes shuffle data from all active executor handles.
 * This avoids per-task port allocation which does not scale under high task
 * concurrency and exhausts ephemeral ports.</p>
 *
 * <p>Routing works as follows:</p>
 * <ol>
 *   <li>Each task registers its executor handle with the shared server
 *       ({@link #registerHandle}).</li>
 *   <li>Downstream workers issue data retrieval requests with a ticket encoding
 *       {@code (handle, queryId, stageId, partitionId)}.</li>
 *   <li>The native C++ server parses the ticket and routes
 *       to the correct {@code PartitionDemuxer} for the handle.</li>
 *   <li>On task completion the handle is unregistered
 *       ({@link #unregisterHandle}).</li>
 * </ol>
 *
 * <h3>Ticket wire format</h3>
 * <p>The ticket is a newline-delimited string:
 * {@code "<handle>\n<queryId>\n<stageId>\n<partitionId>"}. This matches
 * the C++ {@code parseTicket} in {@code NativeShuffleServer.cpp}.</p>
 *
 * <p>This class lives on the Java side for ticket codec operations and
 * handle tracking. The actual request routing happens in C++ via the
 * {@code NativeShuffleServer} / {@code PartitionDemuxer} layer.</p>
 *
 * @see NativeShuffleTransport
 */
@ThreadSafe
public final class ShuffleRequestRouter {

    private final ConcurrentHashMap<Long, HandleRegistration> handles = new ConcurrentHashMap<>();

    /**
     * Metadata tracked per registered handle.
     *
     * @param executorHandle the native executor handle
     * @param queryId        owning query identifier
     * @param stageId        producing stage identifier
     * @param numPartitions  number of output partitions
     * @param registeredAtMs registration timestamp (epoch millis)
     */
    public record HandleRegistration(
            long executorHandle,
            String queryId,
            int stageId,
            int numPartitions,
            long registeredAtMs) {}

    /**
     * Parsed components of a shuffle ticket.
     *
     * @param executorHandle the executor handle that owns the shuffle data
     * @param queryId        the owning query identifier
     * @param stageId        the producing stage identifier
     * @param partitionId    the shuffle partition to fetch
     */
    public record TicketComponents(
            long executorHandle,
            String queryId,
            int stageId,
            int partitionId) {}

    /**
     * Registers an executor handle as actively serving shuffle data.
     *
     * @param executorHandle native executor handle
     * @param queryId        owning query
     * @param stageId        producing stage
     * @param numPartitions  number of output partitions
     */
    public void registerHandle(long executorHandle, String queryId,
                               int stageId, int numPartitions) {
        handles.put(executorHandle, new HandleRegistration(
                executorHandle, queryId, stageId, numPartitions,
                System.currentTimeMillis()));
    }

    /**
     * Unregisters an executor handle when a task completes or fails.
     *
     * @param executorHandle the handle to remove
     * @return the removed registration, or null if not found
     */
    public HandleRegistration unregisterHandle(long executorHandle) {
        return handles.remove(executorHandle);
    }

    /**
     * Returns the registration for a handle, or null if not registered.
     */
    public HandleRegistration getRegistration(long executorHandle) {
        return handles.get(executorHandle);
    }

    /**
     * Returns the number of currently registered handles.
     */
    public int activeHandleCount() {
        return handles.size();
    }

    /**
     * Returns a snapshot of all active registrations.
     */
    public Map<Long, HandleRegistration> snapshot() {
        return Map.copyOf(handles);
    }

    // ── Ticket codec ───────────────────────────────────────────────────

    /**
     * Encodes a shuffle ticket in the wire format consumed by the native
     * native shuffle server's {@code parseTicket} function.
     *
     * <p>Wire format: {@code "<handle>\n<queryId>\n<stageId>\n<partitionId>"}</p>
     *
     * @param executorHandle the executor handle routing key
     * @param queryId        the owning query identifier
     * @param stageId        the producing stage identifier
     * @param partitionId    the partition to fetch
     * @return encoded ticket string
     */
    public static String encodeTicket(long executorHandle, String queryId,
                                      int stageId, int partitionId) {
        return executorHandle + "\n" + queryId + "\n" + stageId + "\n" + partitionId;
    }

    /**
     * Decodes a shuttle ticket from the wire format.
     *
     * @param ticket the raw ticket string
     * @return parsed components
     * @throws IllegalArgumentException if the ticket format is invalid
     */
    public static TicketComponents decodeTicket(String ticket) {
        if (ticket == null || ticket.isEmpty()) {
            throw new IllegalArgumentException("ticket must not be null or empty");
        }
        String[] parts = ticket.split("\n", -1);
        if (parts.length != 4) {
            throw new IllegalArgumentException(
                    "Invalid ticket format: expected 4 newline-delimited fields, got " + parts.length);
        }
        try {
            long handle = Long.parseLong(parts[0]);
            String queryId = parts[1];
            int stageId = Integer.parseInt(parts[2]);
            int partitionId = Integer.parseInt(parts[3]);
            return new TicketComponents(handle, queryId, stageId, partitionId);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid ticket: non-numeric field", e);
        }
    }
}
