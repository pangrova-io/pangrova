package org.pangrova.query.core.audit;

import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.query.api.audit.QueryAuditStore;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests QueryAuditService persist/find/cleanup operations.
 * Uses a real in-memory AuditStore implementation (no mocks).
 *
 * Validates: Requirements 17.2
 */
class QueryAuditServiceTest {

    @Test
    void persistAndFindByQueryId() {
        var store = new InMemoryAuditStore();
        var service = new QueryAuditService(store);
        var record = buildRecord("q-001", true);

        service.persist(record);

        Optional<QueryAuditRecord> found = service.findByQueryId("q-001");
        assertThat(found).isPresent();
        assertThat(found.get().queryId()).isEqualTo("q-001");
        assertThat(found.get().success()).isTrue();
    }

    @Test
    void findByQueryIdReturnsEmptyWhenNotFound() {
        var store = new InMemoryAuditStore();
        var service = new QueryAuditService(store);

        assertThat(service.findByQueryId("nonexistent")).isEmpty();
    }

    @Test
    void persistWrapsStorageFailureWithPangrovaException() {
        var failingStore = new FailingAuditStore();
        var service = new QueryAuditService(failingStore);
        var record = buildRecord("q-fail", false);

        assertThatThrownBy(() -> service.persist(record))
                .isInstanceOf(PangrovaException.class)
                .satisfies(ex -> {
                    var ae = (PangrovaException) ex;
                    assertThat(ae.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.OBS_AUDIT_PERSISTENCE_FAILED);
                    assertThat(ae.getMessage()).contains("q-fail");
                })
                .hasCauseInstanceOf(RuntimeException.class);
    }

    @Test
    void cleanupExpiredDelegatesToStore() {
        var store = new InMemoryAuditStore();
        var service = new QueryAuditService(store);
        service.persist(buildRecord("q-old", true));
        service.persist(buildRecord("q-new", true));

        long deleted = service.cleanupExpired(30);

        // InMemoryAuditStore.deleteOlderThan clears all and returns count
        assertThat(deleted).isEqualTo(2);
    }

    @Test
    void persistOverwritesSameQueryId() {
        var store = new InMemoryAuditStore();
        var service = new QueryAuditService(store);

        service.persist(buildRecord("q-dup", true));
        service.persist(buildRecord("q-dup", false));

        Optional<QueryAuditRecord> found = service.findByQueryId("q-dup");
        assertThat(found).isPresent();
        assertThat(found.get().success()).isFalse();
    }

    // -- Helpers --

    private static QueryAuditRecord buildRecord(String queryId, boolean success) {
        return new QueryAuditRecord(
                queryId, "user-1", "SELECT 1", "tenant-a",
                2, 4,
                100L, 200L, 1024L, 0L,
                500L, 100L, 2048L, 512L,
                0L, 0L,
                Instant.now(), success);
    }

    /**
     * Simple in-memory AuditStore for testing persist/find/cleanup.
     */
    private static final class InMemoryAuditStore implements QueryAuditStore {

        private final java.util.Map<String, QueryAuditRecord> records =
                new java.util.LinkedHashMap<>();

        @Override
        public void save(QueryAuditRecord record) {
            records.put(record.queryId(), record);
        }

        @Override
        public Optional<QueryAuditRecord> findById(String queryId) {
            return Optional.ofNullable(records.get(queryId));
        }

        @Override
        public long deleteOlderThan(int retentionDays) {
            long count = records.size();
            records.clear();
            return count;
        }
    }

    /**
     * AuditStore that always throws on save, for testing error wrapping.
     */
    private static final class FailingAuditStore implements QueryAuditStore {

        @Override
        public void save(QueryAuditRecord record) {
            throw new RuntimeException("disk full");
        }

        @Override
        public Optional<QueryAuditRecord> findById(String queryId) {
            return Optional.empty();
        }

        @Override
        public long deleteOlderThan(int retentionDays) {
            return 0;
        }
    }
}
