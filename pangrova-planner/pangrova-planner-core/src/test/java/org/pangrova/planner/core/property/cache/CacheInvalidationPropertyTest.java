package org.pangrova.planner.core.property.cache;

import org.pangrova.planner.core.cache.CacheEntryMetadata;
import org.pangrova.planner.core.cache.CacheHint;
import org.pangrova.planner.core.cache.CacheKey;
import org.pangrova.planner.core.cache.CachedResult;
import org.pangrova.planner.core.cache.InMemoryL2CacheStore;
import org.pangrova.planner.core.cache.QueryResultCache;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for cache invalidation precision.
 *
 * <p>Verifies that metadata-indexed invalidation is precise (no false positives/negatives)
 * and independent of cache key encoding format.</p>
 *
 * <p><b>Validates: Requirements 1.31, 1.32, 2.31, 2.32</b></p>
 */
class CacheInvalidationPropertyTest {

    @Provide
    Arbitrary<String> tableNames() {
        return Arbitraries.of(
                "orders", "users", "products", "orders_archive",
                "new_orders", "user_roles", "inventory");
    }

    @Provide
    Arbitrary<String> sqls() {
        return Arbitraries.of(
                "SELECT * FROM t", "SELECT count(*) FROM t",
                "SELECT id FROM t WHERE x > 1", "SELECT a, b FROM t JOIN s ON t.id = s.id",
                "INSERT INTO t VALUES (1)");
    }

    @Provide
    Arbitrary<String> tenantIds() {
        return Arbitraries.of("tenant-a", "tenant-b", "tenant-c");
    }

    @Provide
    Arbitrary<String> principals() {
        return Arbitraries.of("alice", "bob", "charlie");
    }

    @Provide
    Arbitrary<String> policyVersions() {
        return Arbitraries.of("v1", "v2", "v3");
    }

    record EntrySpec(String sql, String tenantId, String principal,
                     String policyVersion, Set<String> tables) {
    }

    @Provide
    Arbitrary<EntrySpec> entrySpecs() {
        return Combinators.combine(
                sqls(), tenantIds(), principals(), policyVersions(),
                tableNames().set().ofMinSize(1).ofMaxSize(3)
        ).as(EntrySpec::new);
    }

    /**
     * Property 1: invalidateByTable removes exactly entries referencing that table.
     * No false positives (entries not referencing the table survive).
     * No false negatives (entries referencing the table are removed).
     */
    @Property(tries = 10)
    @Label("Property 1: invalidateByTable is precise — no false positives or negatives")
    void invalidateByTableIsPrecise(
            @ForAll("entrySpecs") EntrySpec target,
            @ForAll("entrySpecs") EntrySpec bystander,
            @ForAll("tableNames") String invalidationTable) {

        QueryResultCache cache = new QueryResultCache(new InMemoryL2CacheStore());

        CacheKey targetKey = CacheKey.of(
                target.sql + " /* target */", target.tenantId, target.principal, Map.of(), target.policyVersion);
        CacheKey bystanderKey = CacheKey.of(
                bystander.sql + " /* bystander */", bystander.tenantId, bystander.principal, Map.of(), bystander.policyVersion);

        CacheEntryMetadata targetMeta = new CacheEntryMetadata(target.tables, target.principal, target.policyVersion);
        CacheEntryMetadata bystanderMeta = new CacheEntryMetadata(bystander.tables, bystander.principal, bystander.policyVersion);

        CachedResult result = new CachedResult(new byte[]{1}, Instant.now());
        cache.put(targetKey, result, targetMeta);
        cache.put(bystanderKey, result, bystanderMeta);

        cache.invalidateByTable(invalidationTable);

        boolean targetReferencesTable = target.tables.contains(invalidationTable.toLowerCase());
        boolean bystanderReferencesTable = bystander.tables.contains(invalidationTable.toLowerCase());

        if (targetReferencesTable) {
            assertThat(cache.get(targetKey, CacheHint.NONE))
                    .as("target entry referencing '%s' should be invalidated", invalidationTable)
                    .isEmpty();
        } else {
            assertThat(cache.get(targetKey, CacheHint.NONE))
                    .as("target entry NOT referencing '%s' should survive", invalidationTable)
                    .isPresent();
        }

        if (bystanderReferencesTable) {
            assertThat(cache.get(bystanderKey, CacheHint.NONE))
                    .as("bystander entry referencing '%s' should be invalidated", invalidationTable)
                    .isEmpty();
        } else {
            assertThat(cache.get(bystanderKey, CacheHint.NONE))
                    .as("bystander entry NOT referencing '%s' should survive", invalidationTable)
                    .isPresent();
        }
    }

    /**
     * Property 2: invalidation is independent of cache key format.
     * Two entries with identical table metadata but completely different key dimensions
     * are both correctly invalidated.
     */
    @Property(tries = 10)
    @Label("Property 2: invalidation survives key format changes — metadata-driven, not key-driven")
    void invalidationSurvivesKeyFormatChanges(
            @ForAll("tableNames") String table,
            @ForAll("sqls") String sql1,
            @ForAll("sqls") String sql2,
            @ForAll("tenantIds") String tenant1,
            @ForAll("tenantIds") String tenant2,
            @ForAll("principals") String principal1,
            @ForAll("principals") String principal2,
            @ForAll("policyVersions") String pv1,
            @ForAll("policyVersions") String pv2) {

        QueryResultCache cache = new QueryResultCache(new InMemoryL2CacheStore());

        CacheKey key1 = CacheKey.of(sql1 + " /* a */", tenant1, principal1, Map.of(), pv1);
        CacheKey key2 = CacheKey.of(sql2 + " /* b */", tenant2, principal2, Map.of("x", "y"), pv2);

        CachedResult result = new CachedResult(new byte[]{1}, Instant.now());
        cache.put(key1, result, new CacheEntryMetadata(Set.of(table), principal1, pv1));
        cache.put(key2, result, new CacheEntryMetadata(Set.of(table), principal2, pv2));

        cache.invalidateByTable(table);

        assertThat(cache.get(key1, CacheHint.NONE))
                .as("entry 1 referencing '%s' should be invalidated regardless of key format", table)
                .isEmpty();
        assertThat(cache.get(key2, CacheHint.NONE))
                .as("entry 2 referencing '%s' should be invalidated regardless of key format", table)
                .isEmpty();
    }
}
