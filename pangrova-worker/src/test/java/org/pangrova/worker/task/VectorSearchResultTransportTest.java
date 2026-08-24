package org.pangrova.worker.task;

import org.pangrova.protocol.rest.task.VectorSearchInfoPayload;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.pangrova.worker.execution.VectorSearchResultBuffer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies that vector search results are properly serialized, buffered,
 * and reported to the coordinator (Task 159: result was computed but discarded).
 */
class VectorSearchResultTransportTest {

    @Test
    void serializeVectorSearchResult_roundTrips() throws Exception {
        // Build a VectorSearchResult with scored rows
        var neighbors = List.of(
                new VectorSearchResult.ScoredRow(10, 0.5),
                new VectorSearchResult.ScoredRow(42, 1.2));
        var result = new VectorSearchResult("file1.parquet", neighbors);

        // Invoke the static serializer via reflection (package-private)
        java.lang.reflect.Method method = TaskLifecycleManager.class.getDeclaredMethod(
                "serializeVectorSearchResult", VectorSearchResult.class);
        method.setAccessible(true);
        byte[] bytes = (byte[]) method.invoke(null, result);

        assertNotNull(bytes);
        // Deserialize and verify
        org.pangrova.protocol.v1.VectorSearchTaskResult proto =
                org.pangrova.protocol.v1.VectorSearchTaskResult.parseFrom(bytes);
        assertEquals("file1.parquet", proto.getDataFilePath());
        assertEquals(2, proto.getNeighborsCount());
        assertEquals(10, proto.getNeighbors(0).getRowOffset());
        assertEquals(0.5, proto.getNeighbors(0).getDistance(), 0.001);
        assertEquals(42, proto.getNeighbors(1).getRowOffset());
        assertEquals(1.2, proto.getNeighbors(1).getDistance(), 0.001);
    }

    @Test
    void vectorResultBuffer_putAndTake() {
        VectorSearchResultBuffer buffer = new VectorSearchResultBuffer();
        byte[] data = new byte[]{1, 2, 3, 4};

        buffer.put("task-1", data);
        assertEquals(1, buffer.size());

        byte[] retrieved = buffer.take("task-1");
        assertNotNull(retrieved);
        assertEquals(4, retrieved.length);
        // take removes the entry
        assertEquals(0, buffer.size());
        assertNull(buffer.take("task-1"));
    }

    @Test
    void vectorResultBuffer_peekDoesNotRemove() {
        VectorSearchResultBuffer buffer = new VectorSearchResultBuffer();
        buffer.put("task-1", new byte[]{5, 6});

        byte[] peeked = buffer.peek("task-1");
        assertNotNull(peeked);
        assertEquals(1, buffer.size()); // still there
    }

    @Test
    void vectorResultBuffer_removeCleanup() {
        VectorSearchResultBuffer buffer = new VectorSearchResultBuffer();
        buffer.put("task-1", new byte[]{7, 8});

        buffer.remove("task-1");
        assertEquals(0, buffer.size());
        assertNull(buffer.take("task-1"));
    }
}
