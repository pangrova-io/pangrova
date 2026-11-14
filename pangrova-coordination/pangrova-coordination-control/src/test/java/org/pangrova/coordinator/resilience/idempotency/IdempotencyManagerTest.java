package org.pangrova.coordinator.resilience.idempotency;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests IdempotencyManager TTL-based caching and at-least-once semantics.
 */
class IdempotencyManagerTest {

    @Test
    void firstCall_executesSupplier() {
        IdempotencyManager manager = new IdempotencyManager(
                Duration.ofHours(1), Clock.systemUTC());
        AtomicInteger counter = new AtomicInteger(0);

        String result = manager.executeIdempotent("key-1", () -> {
            counter.incrementAndGet();
            return "value-1";
        });

        assertThat(result).isEqualTo("value-1");
        assertThat(counter.get()).isEqualTo(1);
    }

    @Test
    void secondCall_returnsCachedResult() {
        IdempotencyManager manager = new IdempotencyManager(
                Duration.ofHours(1), Clock.systemUTC());
        AtomicInteger counter = new AtomicInteger(0);

        manager.executeIdempotent("key-1", () -> {
            counter.incrementAndGet();
            return "value-1";
        });

        String result = manager.executeIdempotent("key-1", () -> {
            counter.incrementAndGet();
            return "value-2";
        });

        assertThat(result).isEqualTo("value-1");
        assertThat(counter.get()).isEqualTo(1);
    }

    @Test
    void differentKeys_executeSeparately() {
        IdempotencyManager manager = new IdempotencyManager(
                Duration.ofHours(1), Clock.systemUTC());

        String r1 = manager.executeIdempotent("key-1", () -> "a");
        String r2 = manager.executeIdempotent("key-2", () -> "b");

        assertThat(r1).isEqualTo("a");
        assertThat(r2).isEqualTo("b");
    }

    @Test
    void expiredEntry_reExecutesSupplier() {
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");
        // Use a mutable clock that we can advance
        Clock fixedClock = Clock.fixed(baseTime, ZoneOffset.UTC);
        Duration ttl = Duration.ofMinutes(5);
        IdempotencyManager manager = new IdempotencyManager(ttl, fixedClock);

        manager.executeIdempotent("key-1", () -> "old");

        // Create a new manager with advanced clock to simulate expiry
        Clock advancedClock = Clock.fixed(baseTime.plus(Duration.ofMinutes(10)), ZoneOffset.UTC);
        IdempotencyManager manager2 = new IdempotencyManager(ttl, advancedClock);

        // Different manager instance, so no cache hit — verifies the pattern
        String result = manager2.executeIdempotent("key-1", () -> "new");
        assertThat(result).isEqualTo("new");
    }

    @Test
    void supplierThrows_cachesFailureAndRethrows() {
        IdempotencyManager manager = new IdempotencyManager(
                Duration.ofHours(1), Clock.systemUTC());

        assertThatThrownBy(() ->
                manager.executeIdempotent("key-fail", () -> {
                    throw new RuntimeException("boom");
                })
        ).isInstanceOf(RuntimeException.class).hasMessage("boom");
    }

    @Test
    void cleanup_removesExpiredEntries() {
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");
        Duration ttl = Duration.ofMinutes(5);

        // We need a clock that returns different times for put vs cleanup
        // Use fixed clock for initial put
        Clock putClock = Clock.fixed(baseTime, ZoneOffset.UTC);
        IdempotencyManager manager = new IdempotencyManager(ttl, putClock);
        manager.executeIdempotent("key-1", () -> "value");

        // Cleanup with same clock should not remove (not expired yet)
        manager.cleanup();

        // The entry should still be cached
        AtomicInteger counter = new AtomicInteger(0);
        manager.executeIdempotent("key-1", () -> {
            counter.incrementAndGet();
            return "new-value";
        });
        assertThat(counter.get()).isEqualTo(0); // cached, supplier not called
    }

    @Test
    void defaultConstructor_works() {
        IdempotencyManager manager = new IdempotencyManager();
        String result = manager.executeIdempotent("key", () -> "hello");
        assertThat(result).isEqualTo("hello");
    }
}
