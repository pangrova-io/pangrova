package org.pangrova.indexing.planner.vector;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.indexing.api.vector.DistanceMetric;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VectorQueryOwnershipTest {
    @Test
    void searchAndMetadataCopiesRetainExactVectorAndFilters() {
        float[] input = {1, 2};
        var filters = new ArrayList<String>();
        filters.add("id > 0");
        var capability = CapabilityRef.newBuilder().setCapabilityId("vector.search")
                .setSemanticVersion("2.0.0")
                .setSignatureSha256(ByteString.copyFrom(new byte[32])).build();
        var node = new VectorSearchNode("catalog.schema.table", "embedding", input,
                10, DistanceMetric.COSINE, "index", capability, 100, filters,
                null, 1, "source", "index-snapshot", 1);
        input[0] = 9;
        filters.clear();
        node.queryVector()[1] = 8;
        var bound = node.withSearchMetadata("source-2", "index-2", 2);
        bound.queryVector()[0] = 7;
        assertThat(node.queryVector()).containsExactly(1, 2);
        assertThat(bound.queryVector()).containsExactly(1, 2);
        assertThat(bound.serializedPreFilters()).containsExactly("id > 0");
        assertThatThrownBy(() -> bound.serializedPreFilters().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(bound.retrievalCapability()).isSameAs(capability);
        assertThat(bound.sourceSnapshot()).isEqualTo("source-2");
        assertThat(node.sourceSnapshot()).isEqualTo("source");
    }
}
