package org.pangrova.indexing.pruning;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.pruning.IndexHint;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AvailabilityWindowPropertyTest {
    @Property
    void unequalSnapshotIdentitiesBypassEvaluationRegardlessOfNumericOrder(@ForAll long id) {
        String pinned = Long.toString(id);
        String distinct = Long.toString(~id);
        for (String key : List.of("iceberg.snapshot_id", "paimon.snapshot_id")) {
            assertThat(outside(Map.of(key, distinct), pinned, pinned)).isTrue();
            assertThat(outside(Map.of(key, distinct), null, pinned)).isTrue();
            assertThat(outside(Map.of(key, pinned), pinned, pinned)).isFalse();
            assertThat(outside(Map.of(key, pinned), null, pinned)).isFalse();
        }
    }

    @Test
    void opaqueTokensRetainTheirSpellingAndMissingCoverageCannotAuthorizeAPresentSplitToken() {
        var split = Map.of("paimon.snapshot_id", "0007");
        assertThat(outside(split, "7", "0007")).isTrue();
        assertThat(outside(split, null, null)).isTrue();
        assertThat(outside(split, null, "0007")).isFalse();
        assertThat(outside(Map.of(), "branch@abc", "branch@abc")).isFalse();
        assertThat(outside(Map.of("iceberg.snapshot_id", "branch@b"), null, "branch@a")).isTrue();
    }

    private static boolean outside(Map<String, String> properties, String query, String indexed) {
        ConnectorSplit split = new ConnectorSplit() {
            @Override
            public Map<String, String> splitProperties() {
                return properties;
            }
        };
        var descriptor = new IndexDescriptor("index", "idx", "lake", "public", "records", List.of("id"),
                IndexType.BLOOM_FILTER, Map.of(), IndexState.READY, null, 0, indexed, indexed,
                Instant.EPOCH, Instant.EPOCH, "default");
        var context = new SplitEvalContext(null, List.of(), Map.of(), query, null, 0, null);
        var hint = new IndexHint(descriptor, new EqualTo("id", 1L));
        return AvailabilityWindowChecker.isOutsideAvailabilityWindow(split, List.of(hint), context);
    }
}
