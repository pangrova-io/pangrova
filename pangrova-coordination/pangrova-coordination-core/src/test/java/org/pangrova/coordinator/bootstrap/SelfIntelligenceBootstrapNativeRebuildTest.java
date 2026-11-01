package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.indexing.ai.lifecycle.IndexRebuildScheduler;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.pangrova.spi.indexing.descriptor.IndexType;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SelfIntelligenceBootstrapNativeRebuildTest {

    @Test
    void missingDurableNativeBuildServiceFailsInsteadOfReportingSuccessfulNoOp() {
        var rebuild = SelfIntelligenceBootstrap.buildRebuildFunction(null);

        ExecutionException failure = assertThrows(
                ExecutionException.class,
                () -> rebuild.apply(
                                vectorDescriptor(),
                                IndexRebuildScheduler.RebuildAction.FULL_REBUILD)
                        .get());

        assertInstanceOf(IllegalStateException.class, failure.getCause());
    }

    private static IndexDescriptor vectorDescriptor() {
        Instant now = Instant.now();
        return new IndexDescriptor(
                "index-id", "embedding-index", "catalog", "schema", "documents",
                List.of("embedding"), IndexType.VECTOR, Map.of("algorithm", "HNSW"),
                IndexState.STALE, null, 3, "42", null, now, now, "system");
    }
}
