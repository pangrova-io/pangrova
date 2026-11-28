package org.pangrova.indexing.pruning;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.InList;
import org.pangrova.connector.api.index.predicate.IsNull;
import org.pangrova.connector.api.index.predicate.RangeQuery;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.pruning.IndexHint;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class IndexSplitFilterContractTest {
    @Test
    void singleColumnBTreeSupportsOnlyExactNativeScalarContracts() {
        for (Comparable<?> value : List.of("text", 1, 1L, 1.0D, 1.0F)) {
            assertThat(IndexSplitFilter.hasNativeBTreeContract(hint(List.of("value"),
                    new EqualTo("value", value)))).isTrue();
            assertThat(IndexSplitFilter.hasNativeBTreeContract(hint(List.of("value"),
                    new RangeQuery("value", value, value, true, true)))).isTrue();
            assertThat(IndexSplitFilter.hasNativeBTreeContract(hint(List.of("value"),
                    new InList("value", List.of(value))))).isTrue();
        }
    }

    @Test
    void bTreeRejectsCompositeUnsupportedAndNonPruningPredicates() {
        assertThat(IndexSplitFilter.hasNativeBTreeContract(hint(List.of("first", "second"),
                new EqualTo("first", 1)))).isFalse();
        for (Comparable<?> value : List.of((short) 1, new BigDecimal("1.0"), UUID.randomUUID(), true)) {
            assertThat(IndexSplitFilter.hasNativeBTreeContract(hint(List.of("value"),
                    new EqualTo("value", value)))).isFalse();
        }
        assertThat(IndexSplitFilter.hasNativeBTreeContract(hint(List.of("value"),
                new IsNull("value")))).isFalse();
    }

    private static IndexHint hint(
            List<String> columns,
            org.pangrova.connector.api.index.predicate.IndexPredicate predicate) {
        var descriptor = new IndexDescriptor(
                "index-1", "idx", "lake", "raw", "items", columns, IndexType.BTREE,
                Map.of(), IndexState.READY, null, 0, "snapshot-1", "snapshot-1",
                Instant.EPOCH, Instant.EPOCH, "tenant");
        return new IndexHint(descriptor, predicate);
    }
}
