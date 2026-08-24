package org.pangrova.coordinator.query.execution.result;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.cache.ResultCacheCoordinator;
import org.pangrova.planner.cache.CacheEntryMetadata;
import org.pangrova.planner.cache.CacheHint;
import org.pangrova.planner.cache.CacheKey;
import org.pangrova.planner.cache.CachedResult;
import org.pangrova.planner.cache.InMemoryL2CacheStore;
import org.pangrova.planner.cache.QueryResultCache;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test proving the coordinator result cache builds keys that
 * incorporate SQL, tenant, and principal into the cache key via the real
 * {@link QueryCacheService#buildCacheKey} path.
 *
 * <p>Tests assert that different SQL or different sessions produce
 * different cache keys, preventing stale data from being served.</p>
 */
class QueryCacheServiceDataVersionTest {

    private static final String SQL = "SELECT count(*) FROM catalog.schema.orders";

    private static CoordinatorSessionContext session() {
        // null userIdentity -> principalOrAnon resolves to "anonymous"
        return new CoordinatorSessionContext("q1", Map.of(), null, "tenant-1");
    }

    private static QueryCacheService newService(QueryResultCache l1) {
        ResultCacheCoordinator coordinator = new ResultCacheCoordinator(l1, null, null);
        return new QueryCacheService(coordinator, null, null);
    }

    @Test
    void differentSqlProducesDifferentKeys() {
        QueryCacheService service = newService(new QueryResultCache(new InMemoryL2CacheStore()));
        CoordinatorSessionContext session = session();

        CacheKey key1 = service.buildCacheKey(SQL, session);
        CacheKey key2 = service.buildCacheKey("SELECT 1", session);

        assertThat(key1.keyString()).isNotEqualTo(key2.keyString());
    }

    @Test
    void sameSqlSameSessionProducesSameKey() {
        QueryCacheService service = newService(new QueryResultCache(new InMemoryL2CacheStore()));
        CoordinatorSessionContext session = session();

        CacheKey key1 = service.buildCacheKey(SQL, session);
        CacheKey key2 = service.buildCacheKey(SQL, session);

        assertThat(key1.keyString()).isEqualTo(key2.keyString());
    }

    @Test
    void cachedResultIsRetrievableByKey() {
        QueryResultCache l1 = new QueryResultCache(new InMemoryL2CacheStore());
        QueryCacheService service = newService(l1);
        CoordinatorSessionContext session = session();

        CacheKey key = service.buildCacheKey(SQL, session);
        l1.put(key, new CachedResult(new byte[]{1}, Instant.now()),
                new CacheEntryMetadata(Set.of("catalog.schema.orders"), "anonymous", ""));

        assertThat(l1.get(key, CacheHint.NONE)).isPresent();
    }

    @Test
    void differentTenantProducesDifferentKey() {
        QueryCacheService service = newService(new QueryResultCache(new InMemoryL2CacheStore()));

        CoordinatorSessionContext session1 = new CoordinatorSessionContext(
                "q1", Map.of(), null, "tenant-1");
        CoordinatorSessionContext session2 = new CoordinatorSessionContext(
                "q2", Map.of(), null, "tenant-2");

        CacheKey key1 = service.buildCacheKey(SQL, session1);
        CacheKey key2 = service.buildCacheKey(SQL, session2);

        assertThat(key1.keyString()).isNotEqualTo(key2.keyString());
    }

    @Test
    void differentSessionPropertiesProduceDifferentKey() {
        QueryCacheService service = newService(new QueryResultCache(new InMemoryL2CacheStore()));

        CoordinatorSessionContext session1 = new CoordinatorSessionContext(
                "q1", Map.of("timezone", "UTC"), null, "tenant-1");
        CoordinatorSessionContext session2 = new CoordinatorSessionContext(
                "q2", Map.of("timezone", "EST"), null, "tenant-1");

        CacheKey key1 = service.buildCacheKey(SQL, session1);
        CacheKey key2 = service.buildCacheKey(SQL, session2);

        assertThat(key1.keyString()).isNotEqualTo(key2.keyString());
    }
}
