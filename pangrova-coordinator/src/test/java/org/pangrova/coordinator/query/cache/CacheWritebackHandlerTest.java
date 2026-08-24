package org.pangrova.coordinator.query.cache;

import org.pangrova.planner.cache.CacheEntryMetadata;
import org.pangrova.planner.cache.CacheHint;
import org.pangrova.planner.cache.CacheKey;
import org.pangrova.planner.cache.CachedResult;
import org.pangrova.planner.cache.InMemoryL2CacheStore;
import org.pangrova.planner.cache.QueryResultCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CacheWritebackHandler}: writeback logic, table-reference
 * normalization, metadata propagation, and end-to-end integration with
 * {@link QueryResultCache} invalidation channels (by-table / by-user).
 *
 * <p>Validates: Requirements 1.51, 2.51 — every test below exercises a real
 * business path of the writeback pipeline: callers hand pre-extracted plan
 * table references (the planner-side responsibility is decoupled), the handler
 * normalizes them to qualified names, persists with metadata, and downstream
 * invalidation primitives reliably wipe affected entries.</p>
 */
class CacheWritebackHandlerTest {

    private QueryResultCache cache;
    private CacheWritebackHandler handler;

    @BeforeEach
    void setUp() {
        cache = new QueryResultCache(new InMemoryL2CacheStore());
        handler = new CacheWritebackHandler(cache);
    }

    // ── handler.writeback end-to-end business paths ──

    @Test
    void writeback_storesResultInCacheAndReturnsItOnSubsequentGet() {
        CacheKey key = CacheKey.of("SELECT * FROM orders", "t1", "alice", Map.of(), "v1");
        byte[] data = {1, 2, 3, 4};

        handler.writeback(key, data, List.of(List.of("orders")), "alice", "v1");

        Optional<CachedResult> cached = cache.get(key, CacheHint.NONE);
        assertThat(cached).isPresent();
        assertThat(cached.get().data()).isEqualTo(data);
    }

    @Test
    void writeback_propagatesTableMetadata_enablingInvalidationByTable() {
        // Real business path: handler.writeback must persist tableReferences as
        // CacheEntryMetadata.referencedTables so invalidateByTable can wipe it.
        CacheKey key = CacheKey.of("SELECT * FROM orders", "t1", "alice", Map.of(), "v1");
        handler.writeback(key, new byte[]{1, 2, 3},
                List.of(List.of("orders")), "alice", "v1");

        assertThat(cache.get(key, CacheHint.NONE)).isPresent();

        // INSERT INTO orders → invalidate by the qualified table name handler wrote
        cache.invalidateByTable("orders");

        assertThat(cache.get(key, CacheHint.NONE))
                .as("handler-written entry must be wiped by invalidateByTable")
                .isEmpty();
    }

    @Test
    void writeback_withMultipleReferencedTables_invalidationByAnyTableMatches() {
        // JOIN of two tables → handler must register BOTH tables in metadata
        CacheKey key = CacheKey.of("SELECT * FROM orders JOIN users ON ...",
                "t1", "alice", Map.of(), "v1");
        handler.writeback(key, new byte[]{9},
                List.of(List.of("orders"), List.of("users")), "alice", "v1");

        assertThat(cache.get(key, CacheHint.NONE)).isPresent();

        // Mutating EITHER referenced table must invalidate the cached join result
        cache.invalidateByTable("users");

        assertThat(cache.get(key, CacheHint.NONE))
                .as("entry must be invalidated when any referenced table mutates")
                .isEmpty();
    }

    @Test
    void writeback_qualifiesMultiPartTableNamesWithDots() {
        // Calcite gives qualified names as ["catalog","schema","table"];
        // handler must join them with '.' before persisting.
        CacheKey key = CacheKey.of("SELECT * FROM cat.sch.orders",
                "t1", "alice", Map.of(), "v1");
        handler.writeback(key, new byte[]{7},
                List.of(List.of("cat", "sch", "orders")), "alice", "v1");

        assertThat(cache.get(key, CacheHint.NONE)).isPresent();

        // The qualified key MUST be "cat.sch.orders" — not "cat" / "orders" alone
        cache.invalidateByTable("cat");
        assertThat(cache.get(key, CacheHint.NONE))
                .as("partial-segment invalidation must NOT wipe the entry")
                .isPresent();
        cache.invalidateByTable("orders");
        assertThat(cache.get(key, CacheHint.NONE))
                .as("unqualified-segment invalidation must NOT wipe the entry")
                .isPresent();

        cache.invalidateByTable("cat.sch.orders");
        assertThat(cache.get(key, CacheHint.NONE))
                .as("fully-qualified invalidation MUST wipe the entry")
                .isEmpty();
    }

    @Test
    void writeback_normalizesTableNamesToLowerCaseForCaseInsensitiveInvalidation() {
        // SQL identifiers are case-insensitive by convention; handler must lowercase
        // the qualified name so mutation events using any case match.
        CacheKey key = CacheKey.of("SELECT * FROM ORDERS", "t1", "alice", Map.of(), "v1");
        handler.writeback(key, new byte[]{1},
                List.of(List.of("Catalog", "Schema", "ORDERS")), "alice", "v1");

        assertThat(cache.get(key, CacheHint.NONE)).isPresent();

        // lowercase-form invalidation event must match the stored (lowercased) ref
        cache.invalidateByTable("catalog.schema.orders");
        assertThat(cache.get(key, CacheHint.NONE)).isEmpty();
    }

    @Test
    void writeback_withEmptyTableReferences_storesEntryWithoutTableInvalidationKeys() {
        // Tableless query (e.g. SELECT 1) → no referenced tables → entry still
        // stored but no table-mutation event will ever invalidate it.
        CacheKey key = CacheKey.of("SELECT 1", "t1", "alice", Map.of(), "v1");
        handler.writeback(key, new byte[]{42}, List.of(), "alice", "v1");

        assertThat(cache.get(key, CacheHint.NONE)).isPresent();

        // Mutating any unrelated table must NOT invalidate a tableless entry
        cache.invalidateByTable("orders");
        assertThat(cache.get(key, CacheHint.NONE))
                .as("tableless entry must survive unrelated table mutations")
                .isPresent();
    }

    @Test
    void writeback_withNullPrincipal_fallsBackToAnonymousForUserScopedInvalidation() {
        // Anonymous / unauthenticated queries are bucketed under "anonymous"
        // so the user-scoped invalidation channel can target them.
        CacheKey key = CacheKey.of("SELECT 1", "t1", "anonymous", Map.of(), null);
        handler.writeback(key, new byte[]{1}, List.of(), null, null);

        assertThat(cache.get(key, CacheHint.NONE)).isPresent();

        cache.invalidateByUser("anonymous");
        assertThat(cache.get(key, CacheHint.NONE))
                .as("null-principal entries must be invalidatable as 'anonymous'")
                .isEmpty();
    }

    @Test
    void writeback_failureDoesNotPropagate_protectingQueryResponsePath() {
        // Writeback is fire-and-forget — a broken cache MUST NOT poison
        // the query response path (caller is on the hot path).
        QueryResultCache brokenCache = new QueryResultCache(new InMemoryL2CacheStore()) {
            @Override
            public void put(CacheKey key, CachedResult result, CacheEntryMetadata metadata) {
                throw new RuntimeException("simulated cache failure");
            }
        };
        CacheWritebackHandler brokenHandler = new CacheWritebackHandler(brokenCache);
        CacheKey key = CacheKey.of("SELECT 1", "t1", "alice", Map.of(), "v1");

        // Must NOT throw — caller continues to serve the query result to the user.
        brokenHandler.writeback(key, new byte[]{1},
                List.of(List.of("orders")), "alice", "v1");
    }

    // ── Per-table isolation: writeback paths only ──

    @Test
    void writeback_perTableIsolation_mutationOfOneTableLeavesOthersIntact() {
        // Two independent writebacks for two different tables; mutating one
        // must NOT cascade to the other.
        CacheKey ordersKey = CacheKey.of("SELECT * FROM orders", "t1", "alice", Map.of(), "v1");
        CacheKey usersKey = CacheKey.of("SELECT * FROM users", "t1", "alice", Map.of(), "v1");

        handler.writeback(ordersKey, new byte[]{1}, List.of(List.of("orders")), "alice", "v1");
        handler.writeback(usersKey, new byte[]{2}, List.of(List.of("users")), "alice", "v1");

        cache.invalidateByTable("orders");

        assertThat(cache.get(ordersKey, CacheHint.NONE))
                .as("mutated table's entry must be wiped").isEmpty();
        assertThat(cache.get(usersKey, CacheHint.NONE))
                .as("unrelated table's entry must be preserved").isPresent();
    }

    // ── Full executor-cycle simulation: writeback → hit → mutation → miss ──

    @Test
    void fullCycle_writebackHitMutationMiss_endToEnd() {
        CacheKey key = CacheKey.of("SELECT * FROM orders WHERE id > 10",
                "tenant-1", "alice", Map.of(), "v1");
        byte[] resultData = {42, 43, 44};

        // Step 1: query executes, handler writebacks the result with referenced tables
        handler.writeback(key, resultData, List.of(List.of("orders")), "alice", "v1");

        // Step 2: identical query hits cache and gets the same bytes
        Optional<CachedResult> hit = cache.get(key, CacheHint.NONE);
        assertThat(hit).isPresent();
        assertThat(hit.get().data()).isEqualTo(resultData);
        assertThat(cache.hitCount()).isEqualTo(1);

        // Step 3: INSERT/DELETE on "orders" triggers table-mutation invalidation
        cache.invalidateByTable("orders");

        // Step 4: identical query is now a miss — stale data was wiped
        assertThat(cache.get(key, CacheHint.NONE))
                .as("post-mutation re-query must miss to avoid stale read")
                .isEmpty();
    }

    // ── toQualifiedNames: pure normalization rules ──

    @Test
    void toQualifiedNames_emptyInput_returnsEmptySet() {
        assertThat(CacheWritebackHandler.toQualifiedNames(List.of())).isEmpty();
    }

    @Test
    void toQualifiedNames_joinsSegmentsWithDotAndLowercases() {
        Set<String> tables = CacheWritebackHandler.toQualifiedNames(
                List.of(List.of("Catalog", "Schema", "Orders")));
        assertThat(tables).containsExactly("catalog.schema.orders");
    }

    @Test
    void toQualifiedNames_deduplicatesEntriesDifferingOnlyByCase() {
        Set<String> tables = CacheWritebackHandler.toQualifiedNames(
                List.of(List.of("orders"), List.of("ORDERS"), List.of("Orders")));
        assertThat(tables)
                .as("case-different refs to the same table must collapse to one entry")
                .containsExactly("orders");
    }

    @Test
    void toQualifiedNames_preservesDistinctTables() {
        Set<String> tables = CacheWritebackHandler.toQualifiedNames(
                List.of(List.of("orders"), List.of("users"), List.of("payments")));
        assertThat(tables).containsExactlyInAnyOrder("orders", "users", "payments");
    }

    // ── Config record ──

    @Test
    void cacheWritebackConfig_defaultDisabled() {
        assertThat(CacheWritebackConfig.DISABLED.enabled()).isFalse();
    }

    @Test
    void cacheWritebackConfig_enabled() {
        CacheWritebackConfig config = new CacheWritebackConfig(true);
        assertThat(config.enabled()).isTrue();
    }
}
