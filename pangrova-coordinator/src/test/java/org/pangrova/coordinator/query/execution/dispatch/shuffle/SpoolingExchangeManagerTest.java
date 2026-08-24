package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import org.pangrova.shuffle.ShuffleCommitResult;
import org.pangrova.shuffle.ShuffleCapabilities;
import org.pangrova.shuffle.ShuffleDurability;
import org.pangrova.shuffle.ShuffleHandle;
import org.pangrova.shuffle.ShuffleRegistration;
import org.pangrova.shuffle.spool.SpoolReader;
import org.pangrova.shuffle.spool.SpoolStore;
import org.pangrova.shuffle.spool.SpoolWriter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SpoolingExchangeManager}.
 */
class SpoolingExchangeManagerTest {

    @Test
    void registerShuffle_returnsLocalHandle() {
        SpoolStore store = new InMemorySpoolStore(ShuffleDurability.LOCAL_SPILL);
        SpoolingExchangeManager manager = new SpoolingExchangeManager(
                store, ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL));

        ShuffleRegistration reg = new ShuffleRegistration("q1", 0, 4, 1024L, false);
        ShuffleHandle handle = manager.registerShuffle(reg);

        assertThat(handle).isEqualTo(ShuffleHandle.LOCAL);
    }

    @Test
    void commitShuffle_returnsEmptyForUnregistered() {
        SpoolStore store = new InMemorySpoolStore(ShuffleDurability.LOCAL_SPILL);
        SpoolingExchangeManager manager = new SpoolingExchangeManager(
                store, ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL));

        ShuffleCommitResult result = manager.commitShuffle("unknown-query", 0);
        assertThat(result).isEqualTo(ShuffleCommitResult.EMPTY);
    }

    @Test
    void commitShuffle_returnsEmptyForRegistered() {
        SpoolStore store = new InMemorySpoolStore(ShuffleDurability.LOCAL_SPILL);
        SpoolingExchangeManager manager = new SpoolingExchangeManager(
                store, ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL));

        manager.registerShuffle(new ShuffleRegistration("q1", 0, 4, 1024L, false));
        ShuffleCommitResult result = manager.commitShuffle("q1", 0);

        // Coordinator-side commit is a no-op for spool-backed shuffles
        assertThat(result).isEqualTo(ShuffleCommitResult.EMPTY);
    }

    @Test
    void cleanupShuffle_delegatesToSpoolStore() {
        InMemorySpoolStore store = new InMemorySpoolStore(ShuffleDurability.LOCAL_SPILL);
        SpoolingExchangeManager manager = new SpoolingExchangeManager(
                store, ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL));

        manager.registerShuffle(new ShuffleRegistration("q1", 0, 4, 1024L, false));
        manager.cleanupShuffle("q1");

        assertThat(store.deletedQueryId).isEqualTo("q1");
    }

    @Test
    void capabilities_reflectSpoolStoreDurability() {
        SpoolStore localStore = new InMemorySpoolStore(ShuffleDurability.LOCAL_SPILL);
        SpoolingExchangeManager localManager = new SpoolingExchangeManager(
                localStore, ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL));

        assertThat(localManager.capabilities().durability())
                .isEqualTo(ShuffleDurability.LOCAL_SPILL);
        assertThat(localManager.capabilities().supportsTaskRetry()).isTrue();

        SpoolStore durableStore = new InMemorySpoolStore(ShuffleDurability.DURABLE);
        SpoolingExchangeManager durableManager = new SpoolingExchangeManager(
                durableStore, ShuffleCapabilities.of(ShuffleDurability.DURABLE));

        assertThat(durableManager.capabilities().durability())
                .isEqualTo(ShuffleDurability.DURABLE);
        assertThat(durableManager.capabilities().supportsTaskRetry()).isTrue();
    }

    @Test
    void spoolStore_isAccessible() {
        InMemorySpoolStore store = new InMemorySpoolStore(ShuffleDurability.LOCAL_SPILL);
        SpoolingExchangeManager manager = new SpoolingExchangeManager(
                store, ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL));

        assertThat(manager.spoolStore()).isSameAs(store);
    }

    @Test
    void constructorRejectsNullSpoolStore() {
        assertThatThrownBy(() -> new SpoolingExchangeManager(
                null, ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("spoolStore");
    }

    @Test
    void constructorRejectsNullCapabilities() {
        SpoolStore store = new InMemorySpoolStore(ShuffleDurability.LOCAL_SPILL);
        assertThatThrownBy(() -> new SpoolingExchangeManager(store, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("capabilities");
    }

    @Test
    void registerShuffleRejectsNull() {
        SpoolStore store = new InMemorySpoolStore(ShuffleDurability.LOCAL_SPILL);
        SpoolingExchangeManager manager = new SpoolingExchangeManager(
                store, ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL));

        assertThatThrownBy(() -> manager.registerShuffle(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void close_closesSpoolStore() throws Exception {
        InMemorySpoolStore store = new InMemorySpoolStore(ShuffleDurability.LOCAL_SPILL);
        SpoolingExchangeManager manager = new SpoolingExchangeManager(
                store, ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL));

        manager.registerShuffle(new ShuffleRegistration("q1", 0, 4, 1024L, false));
        manager.close();

        assertThat(store.closed).isTrue();
    }

    @Test
    void cleanupToleratesSpoolStoreFailure() {
        SpoolStore failingStore = new InMemorySpoolStore(ShuffleDurability.LOCAL_SPILL) {
            @Override
            public void delete(String queryId) {
                throw new RuntimeException("simulated cleanup failure");
            }
        };
        SpoolingExchangeManager manager = new SpoolingExchangeManager(
                failingStore, ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL));

        // Should not throw — cleanup failures are logged and tolerated
        manager.cleanupShuffle("q1");
    }

    /**
     * Minimal in-memory SpoolStore for testing. Not a real implementation —
     * only tracks delete() calls and durability.
     */
    static class InMemorySpoolStore implements SpoolStore {
        private final ShuffleDurability durability;
        volatile String deletedQueryId;
        volatile boolean closed;

        InMemorySpoolStore(ShuffleDurability durability) {
            this.durability = durability;
        }

        @Override
        public SpoolWriter open(String queryId, int stageId, int partitionId,
                                int mapId, long attemptId) {
            return new SpoolWriter() {
                @Override
                public void write(ByteBuffer arrowIpc) {}

                @Override
                public void commit() {}

                @Override
                public void close() {}
            };
        }

        @Override
        public SpoolReader read(String queryId, int stageId, int partitionId,
                                int startMapIndex, int endMapIndex) {
            return new SpoolReader() {
                @Override
                public boolean hasNext() { return false; }

                @Override
                public ByteBuffer next() {
                    throw new NoSuchElementException();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public void delete(String queryId) {
            this.deletedQueryId = queryId;
        }

        @Override
        public ShuffleDurability durability() {
            return durability;
        }

        @Override
        public void close() {
            this.closed = true;
        }
    }
}
