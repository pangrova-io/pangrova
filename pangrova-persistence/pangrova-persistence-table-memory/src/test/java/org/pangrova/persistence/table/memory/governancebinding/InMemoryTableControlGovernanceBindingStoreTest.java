package org.pangrova.persistence.table.memory.governancebinding;

import org.junit.jupiter.api.Test;
import org.pangrova.table.api.policybinding.GovernanceBinding;
import org.pangrova.table.api.policybinding.GovernanceBindingType;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryTableControlGovernanceBindingStoreTest {
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private final InMemoryTableControlGovernanceBindingStore store =
            new InMemoryTableControlGovernanceBindingStore();

    @Test
    void upsertReceiptPreservesTheFirstCreationIdentity() {
        UUID tableId = UUID.randomUUID();
        var first = binding(tableId, GovernanceBindingType.QUALITY_RULE, "rule", Map.of("value", "first"));
        store.save(first);
        var update = new GovernanceBinding(UUID.randomUUID(), tableId,
                first.bindingType(), first.bindingRef(), Map.of("value", "updated"),
                "bob", NOW.plusSeconds(20), NOW.plusSeconds(30));

        var saved = store.save(update);

        assertThat(saved.bindingId()).isEqualTo(first.bindingId());
        assertThat(saved.createdBy()).isEqualTo(first.createdBy());
        assertThat(saved.createdAt()).isEqualTo(first.createdAt());
        assertThat(saved.config()).isEqualTo(update.config());
        assertThat(saved.updatedAt()).isEqualTo(update.updatedAt());
        assertThat(store.find(tableId, first.bindingType(), first.bindingRef())).contains(saved);
    }

    @Test
    void competingInitialUpsertsReturnOneCreationIdentity() throws Exception {
        UUID tableId = UUID.randomUUID();
        var first = binding(tableId, GovernanceBindingType.QUALITY_RULE, "rule", Map.of("value", "first"));
        var second = new GovernanceBinding(UUID.randomUUID(), tableId,
                first.bindingType(), first.bindingRef(), Map.of("value", "second"),
                "bob", NOW.plusSeconds(20), NOW.plusSeconds(30));
        var ready = new java.util.concurrent.CountDownLatch(2);
        var start = new java.util.concurrent.CountDownLatch(1);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var a = pool.submit(() -> { ready.countDown(); start.await(); return store.save(first); });
            var b = pool.submit(() -> { ready.countDown(); start.await(); return store.save(second); });
            assertThat(ready.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            start.countDown();
            var savedA = a.get(10, java.util.concurrent.TimeUnit.SECONDS);
            var savedB = b.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(savedA.bindingId()).isEqualTo(savedB.bindingId());
            assertThat(savedA.createdBy()).isEqualTo(savedB.createdBy());
            assertThat(savedA.createdAt()).isEqualTo(savedB.createdAt());
            assertThat(savedA.config()).isEqualTo(first.config());
            assertThat(savedB.config()).isEqualTo(second.config());
            var current = store.find(tableId, first.bindingType(), first.bindingRef()).orElseThrow();
            assertThat(current).isIn(savedA, savedB);
        } finally {
            start.countDown();
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    private static GovernanceBinding binding(UUID tableId, GovernanceBindingType type,
            String ref, Map<String, String> config) {
        return new GovernanceBinding(UUID.randomUUID(), tableId, type, ref, config,
                "alice", NOW, NOW);
    }
}
