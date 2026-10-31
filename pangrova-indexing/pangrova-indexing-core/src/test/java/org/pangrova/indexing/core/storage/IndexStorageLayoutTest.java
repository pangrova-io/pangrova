package org.pangrova.indexing.core.storage;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.state.IndexState;
import org.pangrova.indexing.api.descriptor.IndexType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IndexStorageLayoutTest {

    @Test
    void rebuildOfSameSourceFileCannotOverwriteServingGeneration() {
        var layout = new IndexStorageLayout("/indexes");
        var first = descriptor(0);
        var rebuilt = descriptor(1);
        String source = "/data/part-1.parquet";

        String servingPath = layout.indexFilePath(first, source);
        String rebuiltPath = layout.indexFilePath(rebuilt, source);

        assertThat(servingPath).startsWith("/indexes/v2/catalog/schema/table/index-id/g0/");
        assertThat(rebuiltPath).startsWith("/indexes/v2/catalog/schema/table/index-id/g1/");
        assertThat(rebuiltPath).isNotEqualTo(servingPath);
        assertThat(layout.indexFilePath(first, source)).isEqualTo(servingPath);
    }

    private static IndexDescriptor descriptor(int generation) {
        return new IndexDescriptor(
                "index-id", "by_value", "catalog", "schema", "table",
                List.of("value"), IndexType.BITMAP, Map.of(),
                IndexState.READY, null, generation, null, null,
                Instant.EPOCH, Instant.EPOCH, "tenant");
    }
}
