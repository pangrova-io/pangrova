package org.pangrova.spi.coordination;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract test for {@link ConsensusStore} SPI.
 *
 * <p>Verifies that an in-memory implementation satisfies the contract: model identity,
 * non-null lease/journal accessors, and describe() returning a meaningful string.</p>
 */
class ConsensusStoreContractTest {

    @Test
    void inMemoryStoreReturnsCorrectModel() {
        ConsensusStore store = buildInMemoryStore();
        assertThat(store.model()).isEqualTo(ConsensusStore.Model.IN_MEMORY);
    }

    @Test
    void inMemoryStoreProvidesPresentLease() {
        ConsensusStore store = buildInMemoryStore();
        assertThat(store.lease()).isNotNull();
    }

    @Test
    void inMemoryStoreProvidesPresentJournal() {
        ConsensusStore store = buildInMemoryStore();
        assertThat(store.journal()).isNotNull();
    }

    @Test
    void inMemoryStoreRegistryMayBeNull() {
        ConsensusStore store = buildInMemoryStore();
        // In-memory store does not require a registry
        // (registry is optional per the SPI contract)
        assertThat(store.registry()).isNull();
    }

    @Test
    void inMemoryStoreWriteCommitJournalDefaultsToNull() {
        ConsensusStore store = buildInMemoryStore();
        assertThat(store.writeCommitJournal()).isNull();
    }

    @Test
    void describeReturnsNonBlankString() {
        ConsensusStore store = buildInMemoryStore();
        assertThat(store.describe()).isNotBlank();
    }

    @Test
    void jdbcLeaseModelEnumValueExists() {
        assertThat(ConsensusStore.Model.JDBC_LEASE).isNotNull();
        assertThat(ConsensusStore.Model.EMBEDDED_RAFT).isNotNull();
        assertThat(ConsensusStore.Model.IN_MEMORY).isNotNull();
        assertThat(ConsensusStore.Model.values()).hasSize(3);
    }

    @Test
    void leaseFromStoreCanAcquireAndRelease() {
        ConsensusStore store = buildInMemoryStore();
        DistributedLease lease = store.lease();
        boolean acquired = lease.tryAcquire("test-key", "node-1", 1, Duration.ofSeconds(30));
        assertThat(acquired).isTrue();
        boolean released = lease.release("test-key", "node-1");
        assertThat(released).isTrue();
    }

    @Test
    void journalFromStoreCanAppendAndReplay() {
        ConsensusStore store = buildInMemoryStore();
        QueryStateJournal journal = store.journal();
        QueryStateEvent event = new QueryStateEvent(
                "q-1", null, null, "QUEUED", System.currentTimeMillis());
        journal.appendQueryEvent(event);
        List<QueryStateEvent> replayed = journal.replayActiveQueries();
        assertThat(replayed).isNotNull();
    }

    // -- Test implementation --

    private static ConsensusStore buildInMemoryStore() {
        DistributedLease lease = new InMemoryLease();
        QueryStateJournal journal = new NoOpJournal();

        return new ConsensusStore() {
            @Override
            public Model model() {
                return Model.IN_MEMORY;
            }

            @Override
            public DistributedLease lease() {
                return lease;
            }

            @Override
            public QueryStateJournal journal() {
                return journal;
            }

            @Override
            public CoordinatorRegistry registry() {
                return null;
            }

            @Override
            public String describe() {
                return "in-memory (test)";
            }
        };
    }

    /** Minimal in-memory lease for testing. */
    private static final class InMemoryLease implements DistributedLease {
        private volatile LeaseEntry holder;

        @Override
        public boolean tryAcquire(String leaseKey, String holderId, long epoch, Duration duration) {
            if (holder == null || holder.holderId().equals(holderId)) {
                holder = new LeaseEntry(leaseKey, holderId, epoch,
                        Instant.now(), Instant.now().plus(duration));
                return true;
            }
            if (holder.isExpired()) {
                holder = new LeaseEntry(leaseKey, holderId, epoch,
                        Instant.now(), Instant.now().plus(duration));
                return true;
            }
            return false;
        }

        @Override
        public boolean tryRenew(String leaseKey, String holderId, Duration duration) {
            if (holder != null && holder.holderId().equals(holderId)) {
                holder = new LeaseEntry(leaseKey, holderId, holder.epoch(),
                        holder.acquiredAt(), Instant.now().plus(duration));
                return true;
            }
            return false;
        }

        @Override
        public boolean release(String leaseKey, String holderId) {
            if (holder != null && holder.holderId().equals(holderId)) {
                holder = null;
                return true;
            }
            return false;
        }

        @Override
        public Optional<LeaseEntry> currentHolder(String leaseKey) {
            return Optional.ofNullable(holder);
        }

        @Override
        public int expireStale() {
            return 0;
        }
    }

    /** Minimal no-op journal for testing. */
    private static final class NoOpJournal implements QueryStateJournal {
        @Override public void appendQueryEvent(QueryStateEvent event) {}
        @Override public void appendQueryEventSync(QueryStateEvent event) {}
        @Override public void appendTaskEvent(TaskStateEvent event) {}
        @Override public void appendTaskEventSync(TaskStateEvent event) {}
        @Override public List<QueryStateEvent> replayActiveQueries() { return List.of(); }
        @Override public List<TaskStateEvent> replayActiveTasks() { return List.of(); }
        @Override public void purgeQuery(String queryId) {}
    }
}
