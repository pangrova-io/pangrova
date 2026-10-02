package org.pangrova.coordinator.metadata;

import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies LazyMetadataSchema caching: repeated getTableMap() calls within TTL
 * return cached results, and invalidateCache() forces a refresh.
 */
class LazyMetadataSchemaCacheTest {

    @Test
    void cachedTableMapReturnedWithinTtl() {
        var schema = new LazyMetadataSchema(null, "cat", "sch", MetadataPlanningTestSessions.session());

        Map<String, Table> first = schema.getTableMap();
        Map<String, Table> second = schema.getTableMap();

        assertThat(first).isSameAs(second);
    }

    @Test
    void invalidateCacheForcesRefresh() {
        var schema = new LazyMetadataSchema(null, "cat", "sch", MetadataPlanningTestSessions.session());

        Map<String, Table> before = schema.getTableMap();
        schema.invalidateCache();
        Map<String, Table> after = schema.getTableMap();

        // Both empty (null registry), but after invalidation a new map instance is created
        assertThat(before).isEmpty();
        assertThat(after).isEmpty();
        assertThat(after).isNotSameAs(before);
    }
}
