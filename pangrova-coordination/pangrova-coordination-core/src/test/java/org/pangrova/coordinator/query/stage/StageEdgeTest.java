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
    void defaultFormatIsCanonicalAndUnboundOrUnknownFormatsAreRejected() {
        var format = org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_ARROW_BATCH;
        assertEquals(format, new StageEdge(0, 1, org.pangrova.planner.core.analysis.ShuffleType.SINGLETON).batchSerde());
        for (var invalid : List.of(org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_UNSPECIFIED,
                org.pangrova.protocol.v2.BatchSerde.UNRECOGNIZED)) {
            assertThrows(IllegalArgumentException.class, () -> new StageEdge(0, 1,
                    org.pangrova.planner.core.analysis.ShuffleType.SINGLETON, List.of(), "exchange", 1, invalid, true));
        }
        assertThrows(IllegalArgumentException.class, () -> new StageEdge(0, 1,
                org.pangrova.planner.core.analysis.ShuffleType.SINGLETON, List.of(), "exchange", 1, null, true));
    }

    @Test
    void hashDistributionMapsToBlocking() {
        StageEdge edge = new StageEdge(0, 1, org.pangrova.planner.core.analysis.ShuffleType.HASH);
        assertEquals(ExchangeReadMode.BLOCKING, edge.readMode());
    }

    @Test
    void rangeDistributionMapsToBlocking() {
        StageEdge edge = new StageEdge(0, 1, org.pangrova.planner.core.analysis.ShuffleType.RANGE);
        assertEquals(ExchangeReadMode.BLOCKING, edge.readMode());
    }

    @Test
    void completeBroadcastDistributionMapsToBlocking() {
        StageEdge edge = new StageEdge(0, 1, org.pangrova.planner.core.analysis.ShuffleType.BROADCAST);
        assertEquals(ExchangeReadMode.BLOCKING, edge.readMode());
    }

    @Test
    void completeSingletonDistributionMapsToBlocking() {
        StageEdge edge = new StageEdge(0, 1, org.pangrova.planner.core.analysis.ShuffleType.SINGLETON);
        assertEquals(ExchangeReadMode.BLOCKING, edge.readMode());
    }

    @Test
    void onlyExplicitlyIncompleteSingletonOrBroadcastCanStream() {
        for (var distribution : List.of(org.pangrova.planner.core.analysis.ShuffleType.SINGLETON,
                org.pangrova.planner.core.analysis.ShuffleType.BROADCAST)) {
            var edge = new StageEdge(0, 1, distribution, List.of(), "stream", 1,
                    org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_ARROW_BATCH, false);
            assertEquals(ExchangeReadMode.STREAMING, edge.readMode());
        }
    }

    @Test
    void nullDistributionTypeRejected() {
        assertThrows(NullPointerException.class,
                () -> new StageEdge(0, 1, null));
    }

    @Test
    void dagGetEdgeReadModeWithMetadata() {
        StageEdge hashEdge = new StageEdge(0, 1, org.pangrova.planner.core.analysis.ShuffleType.HASH);
        StageEdge broadcastEdge = new StageEdge(2, 1, org.pangrova.planner.core.analysis.ShuffleType.BROADCAST);
        StageDAG dag = StageDAG.fromNodes(List.of(), Map.of(), List.of(hashEdge, broadcastEdge));

        assertEquals(ExchangeReadMode.BLOCKING, dag.getEdgeReadMode(0, 1));
        assertEquals(ExchangeReadMode.BLOCKING, dag.getEdgeReadMode(2, 1));
    }

    @Test
    void dagGetEdgeReadModeDefaultsToBlocking() {
        StageDAG dag = StageDAG.fromNodes(List.of(), Map.of(), List.of());
        assertEquals(ExchangeReadMode.BLOCKING, dag.getEdgeReadMode(0, 1));
    }
}
