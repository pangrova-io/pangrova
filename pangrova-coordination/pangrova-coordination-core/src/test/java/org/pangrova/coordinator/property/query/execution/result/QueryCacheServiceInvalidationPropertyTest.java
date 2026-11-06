package org.pangrova.coordinator.property.query.execution.result;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;

import org.pangrova.coordinator.query.cache.ResultCacheCoordinator;
import org.pangrova.coordinator.query.execution.result.QueryCacheService;
import org.pangrova.planner.core.cache.CacheEntryMetadata;
import org.pangrova.planner.core.cache.CacheHint;
import org.pangrova.planner.core.cache.CacheKey;
import org.pangrova.planner.core.cache.CachedResult;
import org.pangrova.planner.core.cache.InMemoryL2CacheStore;
import org.pangrova.planner.core.cache.QueryResultCache;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 17: Cache invalidation completeness.
 *
 * <p>Verifies that {@code invalidateByTable} clears L1 + L2 entries
 * referencing the target table, and {@code invalidateAll} clears all
 * L1 entries. Uses public cache APIs for verification.</p>
 *
 * <p>Uses real {@link QueryResultCache} and {@link InMemoryL2CacheStore}
 * — no mocks.</p>
 *
 * <p><b>Validates: Requirements 8.2, 8.3</b></p>
 */
class QueryCacheServiceInvalidationPropertyTest {

    /**
     * invalidateByTable clears matching entries from both L1 and L2.
     */
    @Property(tries = 50)
    void invalidateByTableClearsL1AndL2(
            @ForAll("tableNames") String tableName,
            @ForAll("principals") String principal) {

        InMemoryL2CacheStore l2Store = new InMemoryL2CacheStore();
        QueryResultCache l1Cache = new QueryResultCache(l2Store);
        ResultCacheCoordinator coordinator = new ResultCacheCoordinator(
                l1Cache, l2Store, null);
        QueryCacheService service = new QueryCacheService(coordinator);

        CacheKey key = CacheKey.of(
                "SELECT * FROM " + tableName, "tenant1", principal,
                Map.of(), null);
        CachedResult result = new CachedResult(
                new byte[]{1, 2, 3}, Instant.now());
        CacheEntryMetadata metadata = new CacheEntryMetadata(
                Set.of(tableName.toLowerCase()), principal, "");
        l1Cache.put(key, result, metadata);

        assertThat(l1Cache.get(key, CacheHint.NONE)).isPresent();
        assertThat(l2Store.get(key.keyString())).isPresent();

        service.invalidateByTable(tableName);

        assertThat(l1Cache.get(key, CacheHint.NONE)).isEmpty();
        assertThat(l2Store.get(key.keyString())).isEmpty();
    }

    /**
     * invalidateAll clears all L1 entries regardless of table.
     */
    @Property(tries = 50)
    void invalidateAllClearsAllL1Entries(
            @ForAll("tableNameLists") List<String> tableNames) {

        InMemoryL2CacheStore l2Store = new InMemoryL2CacheStore();
        QueryResultCache l1Cache = new QueryResultCache(l2Store);
        ResultCacheCoordinator coordinator = new ResultCacheCoordinator(
                l1Cache, l2Store, null);
        QueryCacheService service = new QueryCacheService(coordinator);

        for (String table : tableNames) {
            CacheKey key = CacheKey.of(
                    "SELECT * FROM " + table, "tenant1", "user1",
                    Map.of(), null);
            l1Cache.put(key,
                    new CachedResult(new byte[]{1}, Instant.now()),
                    new CacheEntryMetadata(
                            Set.of(table.toLowerCase()), "user1", ""));
        }

        service.invalidateAll();

        for (String table : tableNames) {
            CacheKey key = CacheKey.of(
                    "SELECT * FROM " + table, "tenant1", "user1",
                    Map.of(), null);
            assertThat(l1Cache.get(key, CacheHint.NONE)).isEmpty();
        }
    }

    /**
     * invalidateByTable preserves entries referencing unrelated tables.
     */
    @Property(tries = 50)
    void invalidateByTablePreservesUnrelatedEntries(
            @ForAll("tableNames") String targetTable,
            @ForAll("tableNames") String otherTable) {

        if (targetTable.equalsIgnoreCase(otherTable)) {
            return;
        }

        InMemoryL2CacheStore l2Store = new InMemoryL2CacheStore();
        QueryResultCache l1Cache = new QueryResultCache(l2Store);
        ResultCacheCoordinator coordinator = new ResultCacheCoordinator(
                l1Cache, l2Store, null);
        QueryCacheService service = new QueryCacheService(coordinator);

        CacheKey targetKey = CacheKey.of(
                "SELECT * FROM " + targetTable, "t1", "u1", Map.of(), null);
        l1Cache.put(targetKey,
                new CachedResult(new byte[]{1}, Instant.now()),
                new CacheEntryMetadata(
                        Set.of(targetTable.toLowerCase()), "u1", ""));

        CacheKey otherKey = CacheKey.of(
                "SELECT * FROM " + otherTable, "t1", "u1", Map.of(), null);
        l1Cache.put(otherKey,
                new CachedResult(new byte[]{2}, Instant.now()),
                new CacheEntryMetadata(
                        Set.of(otherTable.toLowerCase()), "u1", ""));

        service.invalidateByTable(targetTable);

        assertThat(l1Cache.get(targetKey, CacheHint.NONE)).isEmpty();
        assertThat(l1Cache.get(otherKey, CacheHint.NONE)).isPresent();
    }

    /**
     * Null coordinator is a safe no-op for all invalidation operations.
     */
    @Property(tries = 10)
    void nullCoordinatorIsNoOp(
            @ForAll("tableNames") String tableName) {

        QueryCacheService service = new QueryCacheService(null);
        service.invalidateByTable(tableName);
        service.invalidateAll();
    }

    /**
     * Double invalidation is idempotent — invalidating an already-empty
     * cache does not throw.
     */
    @Property(tries = 20)
    void doubleInvalidationIsIdempotent(
            @ForAll("tableNames") String tableName) {

        InMemoryL2CacheStore l2Store = new InMemoryL2CacheStore();
        QueryResultCache l1Cache = new QueryResultCache(l2Store);
        ResultCacheCoordinator coordinator = new ResultCacheCoordinator(
                l1Cache, l2Store, null);
        QueryCacheService service = new QueryCacheService(coordinator);

        CacheKey key = CacheKey.of(
                "SELECT 1 FROM " + tableName, "t1", "u1", Map.of(), null);
        l1Cache.put(key,
                new CachedResult(new byte[]{1}, Instant.now()),
                new CacheEntryMetadata(
                        Set.of(tableName.toLowerCase()), "u1", ""));

        service.invalidateAll();
        assertThat(l1Cache.get(key, CacheHint.NONE)).isEmpty();

        service.invalidateByTable(tableName);
        assertThat(l1Cache.get(key, CacheHint.NONE)).isEmpty();
    }

    @Provide
    Arbitrary<String> tableNames() {
        return Arbitraries.strings()
                .alpha().ofMinLength(3).ofMaxLength(10)
                .map(s -> "catalog.schema." + s.toLowerCase());
    }

    @Provide
    Arbitrary<String> principals() {
        return Arbitraries.strings()
                .alpha().ofMinLength(3).ofMaxLength(8)
                .map(String::toLowerCase);
    }

    @Provide
    Arbitrary<List<String>> tableNameLists() {
        return tableNames().list().ofMinSize(1).ofMaxSize(5);
    }
}
