package org.pangrova.coordinator.query.stage;

import org.pangrova.shuffle.exchange.ExchangeReadMode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates StageEdge readMode derivation and StageDAG edge metadata.
 * Covers REQ-17: StageEdge + StageDAG edge enhancement.
 */
class StageEdgeTest {

    @Test
    void hashDistributionMapsToBlocking() {
        StageEdge edge = new StageEdge(0, 1, org.pangrova.planner.ShuffleType.HASH);
        assertEquals(ExchangeReadMode.BLOCKING, edge.readMode());
    }

    @Test
    void rangeDistributionMapsToBlocking() {
        StageEdge edge = new StageEdge(0, 1, org.pangrova.planner.ShuffleType.RANGE);
        assertEquals(ExchangeReadMode.BLOCKING, edge.readMode());
    }

    @Test
    void broadcastDistributionMapsToStreaming() {
        StageEdge edge = new StageEdge(0, 1, org.pangrova.planner.ShuffleType.BROADCAST);
        assertEquals(ExchangeReadMode.STREAMING, edge.readMode());
    }

    @Test
    void singletonDistributionMapsToStreaming() {
        StageEdge edge = new StageEdge(0, 1, org.pangrova.planner.ShuffleType.SINGLETON);
        assertEquals(ExchangeReadMode.STREAMING, edge.readMode());
    }

    @Test
    void nullDistributionTypeRejected() {
        assertThrows(NullPointerException.class,
                () -> new StageEdge(0, 1, null));
    }

    @Test
    void dagGetEdgeReadModeWithMetadata() {
        StageEdge hashEdge = new StageEdge(0, 1, org.pangrova.planner.ShuffleType.HASH);
        StageEdge broadcastEdge = new StageEdge(2, 1, org.pangrova.planner.ShuffleType.BROADCAST);
        StageDAG dag = StageDAG.fromNodes(List.of(), Map.of(), List.of(hashEdge, broadcastEdge));

        assertEquals(ExchangeReadMode.BLOCKING, dag.getEdgeReadMode(0, 1));
        assertEquals(ExchangeReadMode.STREAMING, dag.getEdgeReadMode(2, 1));
    }

    @Test
    void dagGetEdgeReadModeDefaultsToBlocking() {
        StageDAG dag = StageDAG.fromNodes(List.of(), Map.of(), List.of());
        assertEquals(ExchangeReadMode.BLOCKING, dag.getEdgeReadMode(0, 1));
    }
}
