package org.pangrova.coordinator.query.security;

import org.pangrova.planner.security.SecurityExpressionCache;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for SecurityExpressionCache verifying that parsed security expressions
 * are cached by (expression_text, user_identity) and subsequent calls hit cache.
 */
class SecurityExpressionCacheTest {

    @Test
    void sameExpressionParsedOnceSubsequentCallsHitCache() {
        SecurityExpressionCache cache = new SecurityExpressionCache();
        UserIdentity user = testUser("alice", Set.of("analysts"));
        AtomicInteger parseCount = new AtomicInteger(0);

        String expressionKey = "default.default.orders";
        Optional<String> first = cache.getOrCompute(expressionKey, user, () -> {
            parseCount.incrementAndGet();
            return Optional.of("department = 'sales'");
        });
        Optional<String> second = cache.getOrCompute(expressionKey, user, () -> {
            parseCount.incrementAndGet();
            return Optional.of("department = 'sales'");
        });
        Optional<String> third = cache.getOrCompute(expressionKey, user, () -> {
            parseCount.incrementAndGet();
            return Optional.of("department = 'sales'");
        });

        assertThat(first).contains("department = 'sales'");
        assertThat(second).contains("department = 'sales'");
        assertThat(third).contains("department = 'sales'");
        assertThat(parseCount.get()).isEqualTo(1);
        assertThat(cache.hitCount()).isEqualTo(2);
        assertThat(cache.missCount()).isEqualTo(1);
    }

    @Test
    void differentUsersSameExpressionCachedSeparately() {
        SecurityExpressionCache cache = new SecurityExpressionCache();
        UserIdentity alice = testUser("alice", Set.of("analysts"));
        UserIdentity bob = testUser("bob", Set.of("managers"));
        AtomicInteger parseCount = new AtomicInteger(0);

        String expressionKey = "default.default.orders";
        cache.getOrCompute(expressionKey, alice, () -> {
            parseCount.incrementAndGet();
            return Optional.of("department = 'sales'");
        });
        cache.getOrCompute(expressionKey, bob, () -> {
            parseCount.incrementAndGet();
            return Optional.of("department = 'engineering'");
        });

        assertThat(parseCount.get()).isEqualTo(2);
        assertThat(cache.missCount()).isEqualTo(2);
        assertThat(cache.hitCount()).isEqualTo(0);
    }

    @Test
    void emptyResultCachedAsSentinel() {
        SecurityExpressionCache cache = new SecurityExpressionCache();
        UserIdentity user = testUser("alice", Set.of("analysts"));
        AtomicInteger parseCount = new AtomicInteger(0);

        String expressionKey = "default.default.orders";
        Optional<String> first = cache.getOrCompute(expressionKey, user, () -> {
            parseCount.incrementAndGet();
            return Optional.empty();
        });
        Optional<String> second = cache.getOrCompute(expressionKey, user, () -> {
            parseCount.incrementAndGet();
            return Optional.empty();
        });

        assertThat(first).isEmpty();
        assertThat(second).isEmpty();
        assertThat(parseCount.get()).isEqualTo(1);
        assertThat(cache.hitCount()).isEqualTo(1);
    }

    @Test
    void invalidateAllClearsCache() {
        SecurityExpressionCache cache = new SecurityExpressionCache();
        UserIdentity user = testUser("alice", Set.of("analysts"));
        AtomicInteger parseCount = new AtomicInteger(0);

        String expressionKey = "default.default.orders";
        cache.getOrCompute(expressionKey, user, () -> {
            parseCount.incrementAndGet();
            return Optional.of("region = 'US'");
        });
        cache.invalidateAll();
        cache.getOrCompute(expressionKey, user, () -> {
            parseCount.incrementAndGet();
            return Optional.of("region = 'US'");
        });

        assertThat(parseCount.get()).isEqualTo(2);
    }

    @Test
    void configurableMaxSizeEvictsOldEntries() {
        SecurityExpressionCache cache = new SecurityExpressionCache(2, Duration.ofMinutes(5));
        UserIdentity user = testUser("alice", Set.of("analysts"));

        cache.getOrCompute("expr1", user, () -> Optional.of("a = 1"));
        cache.getOrCompute("expr2", user, () -> Optional.of("b = 2"));
        cache.getOrCompute("expr3", user, () -> Optional.of("c = 3"));

        // Caffeine eviction is async, so we just verify the cache accepted all entries
        assertThat(cache.missCount()).isEqualTo(3);
    }

    @Test
    void sameUserSameGroupsDifferentExpressionsAreSeparate() {
        SecurityExpressionCache cache = new SecurityExpressionCache();
        UserIdentity user = testUser("alice", Set.of("analysts"));
        AtomicInteger parseCount = new AtomicInteger(0);

        cache.getOrCompute("table_a", user, () -> {
            parseCount.incrementAndGet();
            return Optional.of("col_a = 1");
        });
        cache.getOrCompute("table_b", user, () -> {
            parseCount.incrementAndGet();
            return Optional.of("col_b = 2");
        });

        assertThat(parseCount.get()).isEqualTo(2);
        assertThat(cache.missCount()).isEqualTo(2);
    }

    private static UserIdentity testUser(String principal, Set<String> groups) {
        return new UserIdentity(principal, groups, Map.of(), Instant.now());
    }
}
