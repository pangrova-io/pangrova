package org.pangrova.planner.core.cache;

import org.pangrova.security.api.event.SecurityPolicyChangeEvent.ColumnMaskChanged;
import org.pangrova.security.api.event.SecurityPolicyChangeEvent.RlsPolicyChanged;
import org.pangrova.security.api.event.SecurityPolicyChangeEvent.RoleAssignmentChanged;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SecurityPolicyChangeListenerTest {

    private QueryResultCache cache;
    private SecurityPolicyChangeListener listener;

    @BeforeEach
    void setUp() {
        cache = new QueryResultCache(new InMemoryL2CacheStore());
        listener = new SecurityPolicyChangeListener(cache);
    }

    @Test
    void rlsPolicyChangeInvalidatesTableEntries() {
        CacheKey ordersKey1 = cacheKey("select * from orders where id = 1", "t1", "alice");
        CacheKey ordersKey2 = cacheKey("select count(*) from orders", "t1", "bob");
        CacheKey usersKey1 = cacheKey("select * from users", "t1", "alice");
        cache.put(ordersKey1, result(), metadata(Set.of("orders"), "alice"));
        cache.put(ordersKey2, result(), metadata(Set.of("orders"), "bob"));
        cache.put(usersKey1, result(), metadata(Set.of("users"), "alice"));

        listener.onSecurityPolicyChange(
                new RlsPolicyChanged("default", "public", "orders", "order_rls"));

        assertThat(cache.get(ordersKey1, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(ordersKey2, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(usersKey1, CacheHint.NONE)).isPresent();
    }

    @Test
    void columnMaskChangeInvalidatesTableEntries() {
        CacheKey salaryKey1 = cacheKey("select * from salary where dept = 'eng'", "t1", "alice");
        CacheKey salaryKey2 = cacheKey("select ssn from salary", "t1", "bob");
        CacheKey deptKey1 = cacheKey("select * from dept", "t1", "alice");
        cache.put(salaryKey1, result(), metadata(Set.of("salary"), "alice"));
        cache.put(salaryKey2, result(), metadata(Set.of("salary"), "bob"));
        cache.put(deptKey1, result(), metadata(Set.of("dept"), "alice"));

        listener.onSecurityPolicyChange(
                new ColumnMaskChanged("default", "public", "salary", "ssn"));

        assertThat(cache.get(salaryKey1, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(salaryKey2, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(deptKey1, CacheHint.NONE)).isPresent();
    }

    @Test
    void roleAssignmentChangeInvalidatesUserEntries() {
        CacheKey aliceQuery1 = cacheKey("select 1", "t1", "alice");
        CacheKey aliceQuery2 = cacheKey("select 2", "t1", "alice");
        CacheKey bobQuery1 = cacheKey("select 1", "t1", "bob");
        cache.put(aliceQuery1, result(), metadata(Set.of(), "alice"));
        cache.put(aliceQuery2, result(), metadata(Set.of(), "alice"));
        cache.put(bobQuery1, result(), metadata(Set.of(), "bob"));

        listener.onSecurityPolicyChange(
                new RoleAssignmentChanged("alice", "analyst"));

        assertThat(cache.get(aliceQuery1, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(aliceQuery2, CacheHint.NONE)).isEmpty();
        assertThat(cache.get(bobQuery1, CacheHint.NONE)).isPresent();
    }

    private static CacheKey cacheKey(String sql, String tenantId, String principal) {
        return CacheKey.of(sql, tenantId, principal, Map.of(), null);
    }

    private static CacheEntryMetadata metadata(Set<String> tables, String user) {
        return new CacheEntryMetadata(tables, user, "");
    }

    private static CachedResult result() {
        return new CachedResult(new byte[]{1}, Instant.now());
    }
}
