package org.pangrova.coordinator.property.result;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.ByteRange;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.constraints.StringLength;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test: Cache writeback (Property 9).
 * Validates that bytes written to CoordinatorResultBuffer are retrievable
 * with exact byte equality.
 */
class CacheWritebackPropertyTest {

    @Property(tries = 200)
    void putThenGetReturnsSameBytes(
            @ForAll @StringLength(min = 1, max = 50) String queryId,
            @ForAll @Size(min = 1, max = 1024) byte[] resultBytes) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            CoordinatorResultBuffer buffer = new CoordinatorResultBuffer(executor, 60_000L);
            buffer.put(queryId, resultBytes);
            byte[] retrieved = buffer.get(queryId);
            assertThat(retrieved).isEqualTo(resultBytes);
            buffer.close();
        } finally {
            executor.shutdownNow();
        }
    }

    @Property(tries = 200)
    void getMissingQueryReturnsNull(
            @ForAll @StringLength(min = 1, max = 50) String queryId) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            CoordinatorResultBuffer buffer = new CoordinatorResultBuffer(executor, 60_000L);
            assertThat(buffer.get(queryId)).isNull();
            buffer.close();
        } finally {
            executor.shutdownNow();
        }
    }

    @Property(tries = 100)
    void putIsIdempotent(
            @ForAll @StringLength(min = 1, max = 50) String queryId,
            @ForAll @Size(min = 1, max = 512) byte[] firstBytes,
            @ForAll @Size(min = 1, max = 512) byte[] secondBytes) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            CoordinatorResultBuffer buffer = new CoordinatorResultBuffer(executor, 60_000L);
            buffer.put(queryId, firstBytes);
            buffer.put(queryId, secondBytes); // second put is idempotent — first wins
            byte[] retrieved = buffer.get(queryId);
            assertThat(retrieved).isEqualTo(firstBytes);
            buffer.close();
        } finally {
            executor.shutdownNow();
        }
    }
}
