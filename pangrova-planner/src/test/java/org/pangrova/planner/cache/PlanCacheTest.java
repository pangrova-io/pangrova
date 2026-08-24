package org.pangrova.planner.cache;

import org.pangrova.planner.PlanResult;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link PlanCache} covering hit/miss accounting, LRU eviction,
 * table-scoped invalidation, and the disabled-cache path.
 */
class PlanCacheTest {

    @Test
    void getEmptyOnMiss() {
        PlanCache cache = new PlanCache();
        PlanCacheKey key = PlanCacheKey.of("select 1");
        assertTrue(cache.get(key).isEmpty());
        assertEquals(1, cache.missCount());
        assertEquals(0, cache.hitCount());
    }

    @Test
    void putThenGetReturnsCachedPlan() {
        PlanCache cache = new PlanCache();
        PlanCacheKey key = PlanCacheKey.of("select 1");
        PlanResult plan = makePlan("dummy", "sch", "t");
        cache.put(key, plan);

        Optional<PlanResult> hit = cache.get(key);
        assertThat(hit).isPresent();
        // Cache stores a detached copy (F-PLANNER-10), so reference identity won't hold.
        // Verify structural equivalence via plan digest and metadata fields.
        assertThat(hit.get().plan().explain()).isEqualTo(plan.plan().explain());
        assertThat(hit.get().preOptimizationColumnNames()).isEqualTo(plan.preOptimizationColumnNames());
        assertEquals(1, cache.hitCount());
    }

    @Test
    void differentSqlYieldsDistinctEntries() {
        PlanCache cache = new PlanCache();
        PlanCacheKey k1 = PlanCacheKey.of("select 1");
        PlanCacheKey k2 = PlanCacheKey.of("select 2");
        cache.put(k1, makePlan("dummy", "sch", "t1"));
        cache.put(k2, makePlan("dummy", "sch", "t2"));

        assertThat(cache.get(k1)).isPresent();
        assertThat(cache.get(k2)).isPresent();
        assertEquals(2, cache.size());
    }

    @Test
    void normalizeSqlCollapsesWhitespaceAndCase() {
        PlanCache cache = new PlanCache();
        PlanCacheKey k1 = PlanCacheKey.of("SELECT  1");
        PlanCacheKey k2 = PlanCacheKey.of("select 1");
        cache.put(k1, makePlan("dummy", "sch", "t"));
        // both keys should hit the same entry
        assertThat(cache.get(k2)).isPresent();
        assertEquals(1, cache.size());
    }

    @Test
    void invalidateByTableDropsMatchingEntries() {
        PlanCache cache = new PlanCache();
        cache.put(PlanCacheKey.of("select * from a"), makePlan("cat", "sch", "a"));
        cache.put(PlanCacheKey.of("select * from b"), makePlan("cat", "sch", "b"));

        cache.invalidateByTable("cat.sch.a");

        assertThat(cache.get(PlanCacheKey.of("select * from a"))).isEmpty();
        assertThat(cache.get(PlanCacheKey.of("select * from b"))).isPresent();
        assertThat(cache.invalidationCount()).isEqualTo(1);
    }

    @Test
    void invalidateByBareTableNameMatchesQualified() {
        PlanCache cache = new PlanCache();
        cache.put(PlanCacheKey.of("select * from orders"), makePlan("cat", "sch", "orders"));
        cache.invalidateByTable("orders");
        assertThat(cache.get(PlanCacheKey.of("select * from orders"))).isEmpty();
    }

    @Test
    void markTableChangedEvictsPlanOnNextGet_validateOnHit() {
        // A-PLAN-01: a lightweight/lazy invalidation (or cross-coordinator version bump) marks the
        // table changed WITHOUT scanning-removing entries; validate-on-hit must still evict the
        // now-stale plan on its next get so a stale-schema plan is never returned.
        PlanCache cache = new PlanCache();
        PlanCacheKey key = PlanCacheKey.of("select * from orders");
        cache.put(key, makePlan("cat", "sch", "orders"));
        assertThat(cache.get(key)).as("fresh plan hits").isPresent();

        cache.markTableChanged("cat.sch.orders");

        assertThat(cache.get(key)).as("stale plan evicted on hit").isEmpty();
        assertThat(cache.get(key)).as("stays evicted after validate-on-hit").isEmpty();
    }

    @Test
    void putAfterVersionBumpCapturesCurrentVersion_notStale() {
        // A-PLAN-01: a plan cached AFTER a table change captures the new version and is not stale.
        PlanCache cache = new PlanCache();
        cache.markTableChanged("cat.sch.orders");
        PlanCacheKey key = PlanCacheKey.of("select * from orders");
        cache.put(key, makePlan("cat", "sch", "orders"));
        assertThat(cache.get(key)).as("plan cached after the change is current").isPresent();
    }

    @Test
    void markTableChangedForUnrelatedTableDoesNotEvict() {
        // A-PLAN-01: validate-on-hit must not over-evict plans that do not reference the changed table.
        PlanCache cache = new PlanCache();
        PlanCacheKey key = PlanCacheKey.of("select * from orders");
        cache.put(key, makePlan("cat", "sch", "orders"));
        cache.markTableChanged("cat.sch.customers");
        assertThat(cache.get(key)).as("unrelated change must not evict").isPresent();
    }

    @Test
    void invalidateAllClearsCache() {
        PlanCache cache = new PlanCache();
        cache.put(PlanCacheKey.of("select 1"), makePlan("dummy", "sch", "t"));
        cache.put(PlanCacheKey.of("select 2"), makePlan("dummy", "sch", "t2"));

        cache.invalidateAll();

        assertEquals(0, cache.size());
        assertThat(cache.invalidationCount()).isEqualTo(2);
    }

    @Test
    void disabledCacheBypassesGetAndPut() {
        PlanCache cache = new PlanCache(PlanCacheConfig.DISABLED);
        cache.put(PlanCacheKey.of("select 1"), makePlan("dummy", "sch", "t"));
        assertThat(cache.get(PlanCacheKey.of("select 1"))).isEmpty();
        assertEquals(0, cache.size());
    }

    @Test
    void lruEvictsOldestWhenSizeExceeded() {
        PlanCache cache = new PlanCache(new PlanCacheConfig(2, true));
        cache.put(PlanCacheKey.of("select 1"), makePlan("c", "s", "t1"));
        cache.put(PlanCacheKey.of("select 2"), makePlan("c", "s", "t2"));
        cache.put(PlanCacheKey.of("select 3"), makePlan("c", "s", "t3"));
        // Caffeine uses W-TinyLfu (not strict LRU): verify eviction occurred,
        // but do NOT assume which specific entry was evicted.
        assertEquals(2, cache.size());
        assertThat(cache.evictionCount()).isEqualTo(1);
        // At least one of the first two entries must be absent (evicted).
        boolean oneEvicted = cache.get(PlanCacheKey.of("select 1")).isEmpty()
                || cache.get(PlanCacheKey.of("select 2")).isEmpty();
        assertThat(oneEvicted).as("Expected at least one entry evicted by size cap").isTrue();
    }

    @Test
    void planCacheKeyEqualityIgnoresSqlFormatting() {
        PlanCacheKey a = PlanCacheKey.of("SELECT  1");
        PlanCacheKey b = PlanCacheKey.of("select 1");
        assertThat(a).isEqualTo(b);
        assertThat(a.keyString()).isEqualTo(b.keyString());
    }

    @Test
    void planCacheKeyDistinguishesSessionProperties() {
        PlanCacheKey a = PlanCacheKey.of("select 1", java.util.Map.of("k", "v1"));
        PlanCacheKey b = PlanCacheKey.of("select 1", java.util.Map.of("k", "v2"));
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void extractReferencedTablesIncludesQualifiedAndBareName() {
        // PlanCache#extractReferencedTables is package-private; verify behavior
        // indirectly through invalidateByTable which uses the extracted set.
        PlanCache cache = new PlanCache();
        cache.put(PlanCacheKey.of("select * from orders"), makePlan("cat", "sch", "orders"));
        cache.invalidateByTable("cat.sch.orders");
        assertEquals(0, cache.size());
    }

    /**
     * Builds a minimal RelNode (LogicalValues with one row) wrapped in a fake
     * TableScan-like structure. We use {@link RelBuilder} to construct a real
     * Calcite plan referencing a registered table, then return the resulting
     * PlanResult. This exercises the real codepath used by
     * {@link PlanCache#extractReferencedTables}.
     */
    private static PlanResult makePlan(String catalog, String schema, String table) {
        RelNode scan = makeRelScan(catalog, schema, table);
        return new PlanResult(scan, List.of("c0"), List.of(), List.of());
    }

    /** Build a real LogicalTableScan referencing {catalog}.{schema}.{table}. */
    private static RelNode makeRelScan(String catalog, String schema, String table) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        SchemaPlus catalogSchema = rootSchema.add(catalog,
                new org.apache.calcite.schema.impl.AbstractSchema());
        SchemaPlus schemaPlus = catalogSchema.add(schema,
                new org.apache.calcite.schema.impl.AbstractSchema());
        schemaPlus.add(table, new SimpleIntTable());

        FrameworkConfig fc = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();
        RelBuilder b = RelBuilder.create(fc);
        return b.scan(catalog, schema, table).build();
    }

    /** A simple table with one INTEGER column "c0". */
    private static final class SimpleIntTable
            extends org.apache.calcite.schema.impl.AbstractTable {
        @Override
        public org.apache.calcite.rel.type.RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("c0", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .build();
        }
    }
}
