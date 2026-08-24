package org.pangrova.planner.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class QueryResultCacheTest {

    private InMemoryL2CacheStore l2;
    private QueryResultCache cache;

    @BeforeEach
    void setUp() {
        l2 = new InMemoryL2CacheStore();
        cache = new QueryResultCache(l2);
    }

    @Test
    void l1HitReturnsCachedResult() {
        CacheKey key = cacheKey("SELECT 1", "t1", "alice");
        CachedResult result = new CachedResult(new byte[]{1, 2}, Instant.now());
        cache.put(key, result, new CacheEntryMetadata(Set.of(), "alice", ""));
        Optional<CachedResult> hit = cache.get(key, CacheHint.NONE);
        assertThat(hit).isPresent();
        assertThat(hit.get().data()).isEqualTo(new byte[]{1, 2});
    }

    @Test
    void l1MissL2HitPromotesToL1() {
        CacheKey key = cacheKey("SELECT 2", "t1", "bob");
        CachedResult result = new CachedResult(new byte[]{3, 4}, Instant.now());
        l2.put(key.keyString(), result);
        Optional<CachedResult> hit = cache.get(key, CacheHint.NONE);
        assertThat(hit).isPresent();
        assertThat(hit.get().data()).isEqualTo(new byte[]{3, 4});
        assertThat(cache.l1Store().containsKey(key)).isTrue();
    }

    @Test
    void bothMissReturnsEmpty() {
        CacheKey key = cacheKey("SELECT missing", "t1", "alice");
        Optional<CachedResult> hit = cache.get(key, CacheHint.NONE);
        assertThat(hit).isEmpty();
    }


    @Test
    void noCacheHintBypassesCache() {
        CacheKey key = cacheKey("SELECT 3", "t1", "alice");
        cache.put(key, new CachedResult(new byte[]{5}, Instant.now()),
                new CacheEntryMetadata(Set.of(), "alice", ""));
        Optional<CachedResult> hit = cache.get(key, CacheHint.NO_CACHE);
        assertThat(hit).isEmpty();
    }

    @Test
    void refreshCacheHintBypassesLookup() {
        CacheKey key = cacheKey("SELECT 4", "t1", "alice");
        cache.put(key, new CachedResult(new byte[]{6}, Instant.now()),
                new CacheEntryMetadata(Set.of(), "alice", ""));
        Optional<CachedResult> hit = cache.get(key, CacheHint.REFRESH_CACHE);
        assertThat(hit).isEmpty();
    }

    @Test
    void putWritesToBothL1AndL2() {
        CacheKey key = cacheKey("SELECT 5", "t1", "alice");
        CachedResult result = new CachedResult(new byte[]{7, 8}, Instant.now());
        cache.put(key, result, new CacheEntryMetadata(Set.of(), "alice", ""));
        assertThat(cache.l1Store().containsKey(key)).isTrue();
        assertThat(l2.get(key.keyString())).isPresent();
    }


    @Test
    void invalidateByTableRemovesFromBothLayers() {
        CacheKey ordersKey = cacheKey("SELECT * FROM orders", "t1", "alice");
        CacheKey usersKey = cacheKey("SELECT * FROM users", "t1", "alice");
        CacheEntryMetadata ordersMeta = new CacheEntryMetadata(Set.of("orders"), "alice", "");
        CacheEntryMetadata usersMeta = new CacheEntryMetadata(Set.of("users"), "alice", "");
        cache.put(ordersKey, new CachedResult(new byte[]{1}, Instant.now()), ordersMeta);
        cache.put(usersKey, new CachedResult(new byte[]{2}, Instant.now()), usersMeta);

        cache.invalidateByTable("orders");

        assertThat(cache.l1Store().containsKey(ordersKey)).isFalse();
        assertThat(cache.l1Store().containsKey(usersKey)).isTrue();
    }


    @Test
    void differentTenantsProduceDifferentCacheEntries() {
        CacheKey tenantA = cacheKey("SELECT count(*) FROM orders", "tenant-a", "alice");
        CacheKey tenantB = cacheKey("SELECT count(*) FROM orders", "tenant-b", "alice");
        cache.put(tenantA, new CachedResult(new byte[]{1}, Instant.now()),
                new CacheEntryMetadata(Set.of("orders"), "alice", ""));
        assertThat(cache.get(tenantA, CacheHint.NONE)).isPresent();
        assertThat(cache.get(tenantB, CacheHint.NONE)).isEmpty();
    }

    @Test
    void differentUsersProduceDifferentCacheEntries() {
        CacheKey alice = cacheKey("SELECT * FROM orders", "t1", "alice");
        CacheKey bob = cacheKey("SELECT * FROM orders", "t1", "bob");
        cache.put(alice, new CachedResult(new byte[]{1}, Instant.now()),
                new CacheEntryMetadata(Set.of("orders"), "alice", ""));
        assertThat(cache.get(alice, CacheHint.NONE)).isPresent();
        assertThat(cache.get(bob, CacheHint.NONE)).isEmpty();
    }

    @Test
    void putWithMetadataStoresMetadataIndex() {
        CacheKey key = cacheKey("SELECT * FROM orders JOIN users", "t1", "alice");
        CacheEntryMetadata metadata = new CacheEntryMetadata(Set.of("orders", "users"), "alice", "v1");
        cache.put(key, new CachedResult(new byte[]{1}, Instant.now()), metadata);
        assertThat(cache.metadataIndex().get(key)).isEqualTo(metadata);
    }

    @Test
    void invalidateAllClearsBothLayers() {
        CacheKey k1 = cacheKey("SELECT 1", "t1", "alice");
        CacheKey k2 = cacheKey("SELECT 2", "t1", "bob");
        cache.put(k1, new CachedResult(new byte[]{1}, Instant.now()),
                new CacheEntryMetadata(Set.of(), "alice", ""));
        cache.put(k2, new CachedResult(new byte[]{2}, Instant.now()),
                new CacheEntryMetadata(Set.of(), "bob", ""));
        cache.invalidateAll();
        assertThat(cache.l1Store()).isEmpty();
        assertThat(cache.metadataIndex()).isEmpty();
    }

    // ── Invalidation precision tests (Requirements 2.31, 2.32) ──

    @Test
    void invalidateByTableNoFalsePositives() {
        // "orders" invalidation must NOT remove "orders_archive" or "new_orders"
        CacheKey ordersKey = cacheKey("SELECT * FROM orders", "t1", "alice");
        CacheKey archiveKey = cacheKey("SELECT * FROM orders_archive", "t1", "alice");
        CacheKey newOrdersKey = cacheKey("SELECT * FROM new_orders", "t1", "alice");

        cache.put(ordersKey, result(), new CacheEntryMetadata(Set.of("orders"), "alice", "v1"));
        cache.put(archiveKey, result(), new CacheEntryMetadata(Set.of("orders_archive"), "alice", "v1"));
        cache.put(newOrdersKey, result(), new CacheEntryMetadata(Set.of("new_orders"), "alice", "v1"));

        cache.invalidateByTable("orders");

        assertThat(cache.get(ordersKey, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(archiveKey, CacheHint.NONE)).isPresent();
        assertThat(cache.get(newOrdersKey, CacheHint.NONE)).isPresent();
    }

    @Test
    void invalidateByTableNoFalseNegatives() {
        // Multi-table query referencing "orders" must be invalidated
        CacheKey joinKey = cacheKey("SELECT * FROM orders JOIN users ON orders.uid = users.id", "t1", "alice");
        CacheKey ordersOnly = cacheKey("SELECT id FROM orders", "t1", "bob");
        CacheKey usersOnly = cacheKey("SELECT name FROM users", "t1", "alice");

        cache.put(joinKey, result(), new CacheEntryMetadata(Set.of("orders", "users"), "alice", "v1"));
        cache.put(ordersOnly, result(), new CacheEntryMetadata(Set.of("orders"), "bob", "v1"));
        cache.put(usersOnly, result(), new CacheEntryMetadata(Set.of("users"), "alice", "v1"));

        cache.invalidateByTable("orders");

        assertThat(cache.get(joinKey, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(ordersOnly, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(usersOnly, CacheHint.NONE)).isPresent();
    }

    @Test
    void invalidateByTableIsCaseInsensitive() {
        CacheKey key = cacheKey("SELECT * FROM Orders", "t1", "alice");
        cache.put(key, result(), new CacheEntryMetadata(Set.of("orders"), "alice", "v1"));

        cache.invalidateByTable("ORDERS");

        assertThat(cache.get(key, CacheHint.NONE)).isEmpty();
    }

    @Test
    void invalidateByTableSurvivesKeyFormatChanges() {
        // Invalidation works through metadata index, not key encoding.
        // Two entries with different SQL (different key digests) but same table
        // must both be invalidated.
        CacheKey key1 = CacheKey.of("SELECT * FROM orders WHERE id = 1", "t1", "alice", Map.of(), "v1");
        CacheKey key2 = CacheKey.of("SELECT count(*) FROM orders", "t2", "bob", Map.of("tz", "UTC"), "v2");
        CacheKey key3 = CacheKey.of("SELECT * FROM users", "t1", "alice", Map.of(), "v1");

        // Keys have completely different digests, but metadata tracks the table
        assertThat(key1.keyString()).isNotEqualTo(key2.keyString());

        cache.put(key1, result(), new CacheEntryMetadata(Set.of("orders"), "alice", "v1"));
        cache.put(key2, result(), new CacheEntryMetadata(Set.of("orders"), "bob", "v2"));
        cache.put(key3, result(), new CacheEntryMetadata(Set.of("users"), "alice", "v1"));

        cache.invalidateByTable("orders");

        assertThat(cache.get(key1, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(key2, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(key3, CacheHint.NONE)).isPresent();
    }

    @Test
    void invalidateByUserPrecision() {
        CacheKey aliceKey = cacheKey("SELECT 1", "t1", "alice");
        CacheKey bobKey = cacheKey("SELECT 1", "t1", "bob");
        CacheKey aliceAdminKey = cacheKey("SELECT 2", "t1", "alice_admin");

        cache.put(aliceKey, result(), new CacheEntryMetadata(Set.of(), "alice", "v1"));
        cache.put(bobKey, result(), new CacheEntryMetadata(Set.of(), "bob", "v1"));
        cache.put(aliceAdminKey, result(), new CacheEntryMetadata(Set.of(), "alice_admin", "v1"));

        cache.invalidateByUser("alice");

        assertThat(cache.get(aliceKey, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(bobKey, CacheHint.NONE)).isPresent();
        assertThat(cache.get(aliceAdminKey, CacheHint.NONE)).isPresent();
    }

    @Test
    void invalidateByPolicyVersionPrecision() {
        CacheKey v1Key = cacheKey("SELECT 1", "t1", "alice");
        CacheKey v2Key = cacheKey("SELECT 2", "t1", "bob");

        cache.put(v1Key, result(), new CacheEntryMetadata(Set.of(), "alice", "v1"));
        cache.put(v2Key, result(), new CacheEntryMetadata(Set.of(), "bob", "v2"));

        cache.invalidateByPolicyVersion("v1");

        assertThat(cache.get(v1Key, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(v2Key, CacheHint.NONE)).isPresent();
    }

    private static CachedResult result() {
        return new CachedResult(new byte[]{1}, Instant.now());
    }

    private static CacheKey cacheKey(String sql, String tenantId, String principal) {
        return CacheKey.of(sql, tenantId, principal, Map.of(), null);
    }
}
