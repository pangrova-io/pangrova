package org.pangrova.indexing.storage;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class IndexStorageLayoutTest {

    @Test
    void rebuildOfSameSourceFileCannotOverwriteServingGeneration() {
        var layout = new IndexStorageLayout("/indexes");
        var first = descriptor(0);
        var rebuilt = descriptor(1);
        var attempt = new UUID(0, 1);
        String source = "/data/part-1.parquet";

        String servingPath = layout.indexFilePath(first, attempt, source);
        String rebuiltPath = layout.indexFilePath(rebuilt, attempt, source);

        assertThat(servingPath).startsWith("/indexes/v3/catalog/schema/table/index-id/g0/");
        assertThat(rebuiltPath).startsWith("/indexes/v3/catalog/schema/table/index-id/g1/");
        assertThat(rebuiltPath).isNotEqualTo(servingPath);
        assertThat(layout.indexFilePath(first, attempt, source)).isEqualTo(servingPath);
    }

    @Test
    void retiredPathOwnershipUsesOriginalPhysicalGenerationAndExactScope() {
        var layout = new IndexStorageLayout("/indexes");
        String source = "/data/part-1.parquet";
        String original = layout.indexFilePath(descriptor(0), new UUID(0, 1), source);
        assertThat(layout.isRetiredIndexFilePath(descriptor(4), source, 2, original)).isTrue();
        assertThat(layout.isRetiredIndexFilePath(descriptor(4), "different-source", 2, original)).isFalse();
        assertThat(layout.isRetiredIndexFilePath(descriptor(4), source, 0,
                layout.indexFilePath(descriptor(1), new UUID(0, 2), source))).isFalse();
        assertThat(layout.isRetiredIndexFilePath(descriptor(4), source, 2, "/indexes/v1/legacy.idx")).isFalse();
        assertThat(new IndexStorageLayout("/changed-root")
                .isRetiredIndexFilePath(descriptor(4), source, 2, original)).isFalse();
    }

    private static IndexDescriptor descriptor(int generation) {
        return new IndexDescriptor(
                "index-id", "by_value", "catalog", "schema", "table",
                List.of("value"), IndexType.BITMAP, Map.of(),
                IndexState.READY, null, generation, null, null,
                Instant.EPOCH, Instant.EPOCH, "tenant");
    }
}
