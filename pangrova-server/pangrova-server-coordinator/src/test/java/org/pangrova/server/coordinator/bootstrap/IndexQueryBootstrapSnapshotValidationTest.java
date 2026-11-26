package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.enums.GenerationExecutionKind;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

class IndexQueryBootstrapSnapshotValidationTest {
    @Test
    void queryOnlyBootstrapDelegatesNativeSnapshotValidation() {
        var catalogs = new CatalogRegistry(memoryTableStores(), null) {};
        var validated = new AtomicReference<GenerationJob>();
        var job = nativeJob();

        try (var bootstrap = new IndexQueryBootstrap(
                new IndexingBootstrapStartOrderingTest.NoOpIndexMetadataStore(),
                null, 5_000, 0, catalogs, validated::set)) {
            bootstrap.validateSnapshotPin(job);
        }

        assertThat(validated).hasValue(job);
    }

    private static GenerationJob nativeJob() {
        Instant now = Instant.parse("2026-11-25T00:00:00Z");
        return new GenerationJob(
                "tenant-a", "job-a", "artifact-a", "TEXT_INDEX", "0",
                "lake.raw.items", "snapshot-a", GenerationExecutionKind.BUILD,
                GenerationDependencySet.sourceSnapshot("lake.raw.items", "snapshot-a"),
                GenerationEmptyPartitionPolicy.FORBID, "s3://warehouse/staging",
                "pangrova.bm25.segment", 1, 0, "a".repeat(64), "writer-a", "",
                "job-a", 0, 0, GenerationJobState.QUEUED, 0, now, now, now);
    }
}
