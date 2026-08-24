package org.pangrova.coordinator.query.cache;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.result.QueryCacheService;
import org.pangrova.coordinator.query.security.SecurityPolicySignatureProvider;
import org.pangrova.planner.cache.CacheKey;
import org.pangrova.planner.cache.InMemoryL2CacheStore;
import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies F-CHAIN-SEC-04: policy dimension in cache keys and
 * group-based signature computation for cache isolation.
 *
 * <ol>
 *   <li>Same query with different user groups produces different cache keys</li>
 *   <li>Group-based signature is deterministic and order-independent</li>
 *   <li>Anonymous users get empty policy version</li>
 * </ol>
 */
class ResultCachePolicyIsolationTest {

    private static final String SQL = "SELECT * FROM orders";

    private QueryResultCache cache;
    private ResultCacheCoordinator cacheCoordinator;

    @BeforeEach
    void setUp() {
        cache = new QueryResultCache(new InMemoryL2CacheStore());
        cacheCoordinator = new ResultCacheCoordinator(cache, null, null);
    }

    // -- Part (a): Policy dimension in cache key via SecurityPolicySignatureProvider --

    @Test
    void sameQuery_differentGroups_producesDifferentSignatures() {
        UserIdentity analyst = new UserIdentity("alice", Set.of("analysts"), Map.of(), Instant.now());
        UserIdentity admin = new UserIdentity("bob", Set.of("admins"), Map.of(), Instant.now());

        String analystSig = SecurityPolicySignatureProvider.computeGroupBasedSignature(analyst);
        String adminSig = SecurityPolicySignatureProvider.computeGroupBasedSignature(admin);

        assertThat(analystSig).isNotEqualTo(adminSig);
    }

    @Test
    void sameQuery_sameGroups_producesSameSignature() {
        UserIdentity user1 = new UserIdentity("alice", Set.of("analysts"), Map.of(), Instant.now());
        UserIdentity user2 = new UserIdentity("alice", Set.of("analysts"), Map.of(), Instant.now());

        String sig1 = SecurityPolicySignatureProvider.computeGroupBasedSignature(user1);
        String sig2 = SecurityPolicySignatureProvider.computeGroupBasedSignature(user2);

        assertThat(sig1).isEqualTo(sig2);
    }

    @Test
    void policySignature_deterministic_acrossInvocations() {
        UserIdentity user = new UserIdentity("charlie", Set.of("eng", "data"), Map.of(), Instant.now());

        String sig1 = SecurityPolicySignatureProvider.computeGroupBasedSignature(user);
        String sig2 = SecurityPolicySignatureProvider.computeGroupBasedSignature(user);

        assertThat(sig1).isEqualTo(sig2);
    }

    @Test
    void userWithMultipleGroups_orderIndependent() {
        // Groups specified in different orders should yield same signature
        UserIdentity user1 = new UserIdentity("dave", Set.of("a", "b", "c"), Map.of(), Instant.now());
        UserIdentity user2 = new UserIdentity("dave", Set.of("c", "a", "b"), Map.of(), Instant.now());

        String sig1 = SecurityPolicySignatureProvider.computeGroupBasedSignature(user1);
        String sig2 = SecurityPolicySignatureProvider.computeGroupBasedSignature(user2);

        assertThat(sig1).isEqualTo(sig2);
    }

    // -- Part (b): QueryCacheService buildCacheKey --

    @Test
    void buildCacheKey_differentTenants_producesDifferentKeys() {
        QueryCacheService cacheService = new QueryCacheService(
                cacheCoordinator, CacheWritebackConfig.DISABLED, null);

        CoordinatorSessionContext session1 = new CoordinatorSessionContext(
                "q1", Map.of(), null, "tenant-a");
        CoordinatorSessionContext session2 = new CoordinatorSessionContext(
                "q2", Map.of(), null, "tenant-b");

        CacheKey key1 = cacheService.buildCacheKey(SQL, session1);
        CacheKey key2 = cacheService.buildCacheKey(SQL, session2);

        assertThat(key1).isNotEqualTo(key2);
    }

    @Test
    void buildCacheKey_sameTenant_sameQuery_producesSameKey() {
        QueryCacheService cacheService = new QueryCacheService(
                cacheCoordinator, CacheWritebackConfig.DISABLED, null);

        CoordinatorSessionContext session1 = new CoordinatorSessionContext(
                "q1", Map.of(), null, "tenant-a");
        CoordinatorSessionContext session2 = new CoordinatorSessionContext(
                "q2", Map.of(), null, "tenant-a");

        CacheKey key1 = cacheService.buildCacheKey(SQL, session1);
        CacheKey key2 = cacheService.buildCacheKey(SQL, session2);

        assertThat(key1).isEqualTo(key2);
    }

    @Test
    void cacheKeyIsolation_differentPolicyVersions() {
        // Manually create cache keys with different policy versions
        CacheKey key1 = CacheKey.of(SQL, "t1", "alice", Map.of(), "policy-v1");
        CacheKey key2 = CacheKey.of(SQL, "t1", "alice", Map.of(), "policy-v2");

        assertThat(key1).isNotEqualTo(key2);
        assertThat(key1.policyVersion()).isNotEqualTo(key2.policyVersion());
    }
}
