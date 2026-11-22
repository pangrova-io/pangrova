package org.pangrova.persistence.query.memory.audit;

import org.pangrova.query.profiling.OperatorStatsSource;
import org.pangrova.query.profiling.QueryProfileService;
import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.query.audit.QueryAuditService;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests QueryProfileService: wires InMemoryQueryAuditStore.findById into profile lookup
 * and verifies structured JSON output with measured attempts, timing, and memory.
 */
class QueryProfileServiceTest {

    @Test
    void profileCopiesInputCollectionsAndExposesOnlyImmutableLists() {
        var operators = new ArrayList<OperatorStatsSource.Metric>();
        var metric = new OperatorStatsSource.Metric("Scan", "node", 1, 1, 4, 4, 500, 700, 4, 0);
        operators.add(metric);
        var attempt = new OperatorStatsSource.Attempt(0, 1, 1, 0, "task", "worker",
                "FINISHED", 5, 1, 1, 0, operators);
        var attempts = new ArrayList<>(List.of(attempt));
        var profile = new QueryProfileService.QueryProfile("q", "SELECT 1", true,
                attempts, new QueryProfileService.TimingBreakdown(1, 1, 1, 1),
                new QueryProfileService.MemoryUsage(1, 0, 1, 1, 0, 0), true);
        operators.clear();
        attempts.clear();

        assertThat(profile.attempts()).containsExactly(attempt);
        assertThat(attempt.operators()).containsExactly(metric);
        assertThatThrownBy(() -> profile.attempts().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> attempt.operators().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void getProfileReturnsEmptyWhenQueryNotFound() {
        var store = new InMemoryQueryAuditStore();
        var service = new QueryProfileService(new QueryAuditService(store));

        Optional<QueryProfileService.QueryProfile> result = service.getProfile("nonexistent");

        assertThat(result).isEmpty();
    }

    @Test
    void getProfileReturnsStructuredDataAfterAuditSave() {
        var store = new InMemoryQueryAuditStore();
        var auditService = new QueryAuditService(store);
        var service = new QueryProfileService(auditService);

        var record = new QueryAuditRecord(
                "q-profile-1", "user-1", "SELECT * FROM orders", "tenant-1",
                2, 4,
                500L, 1000L, 4096L, 128L,
                10000L, 5000L, 8192L, 2048L,
                1024L, 512L,
                Instant.now(), true);
        auditService.persist(record);

        Optional<QueryProfileService.QueryProfile> result = service.getProfile("q-profile-1");

        assertThat(result).isPresent();
        var profile = result.get();
        assertThat(profile.queryId()).isEqualTo("q-profile-1");
        assertThat(profile.sql()).isEqualTo("SELECT * FROM orders");
        assertThat(profile.success()).isTrue();
    }

    @Test
    void reportsTheAbsenceOfRetainedAttemptTelemetry() {
        var store = new InMemoryQueryAuditStore();
        var auditService = new QueryAuditService(store);
        var service = new QueryProfileService(auditService);

        var record = new QueryAuditRecord(
                "q-tree-1", "user-1", "SELECT 1", "t-1",
                2, 4,
                200L, 400L, 2048L, 0L,
                1000L, 500L, 4096L, 1024L,
                0L, 0L,
                Instant.now(), true);
        auditService.persist(record);

        var profile = service.getProfile("q-tree-1").orElseThrow();

        assertThat(profile.telemetryAvailable()).isFalse();
        assertThat(profile.attempts()).isEmpty();
        assertThat(profile.timing().totalWallMs()).isEqualTo(400L);
        assertThat(profile.timing().totalCpuMs()).isEqualTo(200L);
    }

    @Test
    void profileUsesRetainedOperatorTelemetry() {
        var store = new InMemoryQueryAuditStore();
        var auditService = new QueryAuditService(store);
        var metric = new OperatorStatsSource.Metric(
                "TableScan", "node-1", 100, 50, 200, 100, 2_000_000, 3_000_000, 4096, 0);
        var attempt = new OperatorStatsSource.Attempt(2, 1, 4, 0, "task-1", "worker-1",
                "FINISHED", 7, 3, 2, 1, List.of(metric));
        var service = new QueryProfileService(auditService, queryId -> List.of(attempt));

        var record = new QueryAuditRecord(
                "q-telemetry-1", "user-1", "SELECT 1", "t-1",
                1, 1,
                100L, 300L, 4096L, 0L,
                100L, 50L, 200L, 100L,
                0L, 0L,
                Instant.now(), true);
        auditService.persist(record);

        var profile = service.getProfile("q-telemetry-1").orElseThrow();
        assertThat(profile.telemetryAvailable()).isTrue();
        assertThat(profile.attempts()).containsExactly(attempt);
        assertThat(profile.attempts().get(0).operators()).containsExactly(metric);
    }

    @Test
    void profileContainsTimingBreakdown() {
        var store = new InMemoryQueryAuditStore();
        var auditService = new QueryAuditService(store);
        var service = new QueryProfileService(auditService);

        var record = new QueryAuditRecord(
                "q-timing-1", "user-1", "SELECT 1", "t-1",
                1, 2,
                300L, 500L, 1024L, 0L,
                2000L, 1000L, 4096L, 2048L,
                0L, 0L,
                Instant.now(), true);
        auditService.persist(record);

        var profile = service.getProfile("q-timing-1").orElseThrow();
        var timing = profile.timing();

        assertThat(timing.totalWallMs()).isEqualTo(500L);
        assertThat(timing.totalCpuMs()).isEqualTo(300L);
        assertThat(timing.inputRows()).isEqualTo(2000L);
        assertThat(timing.outputRows()).isEqualTo(1000L);
    }

    @Test
    void profileContainsMemoryUsage() {
        var store = new InMemoryQueryAuditStore();
        var auditService = new QueryAuditService(store);
        var service = new QueryProfileService(auditService);

        var record = new QueryAuditRecord(
                "q-mem-1", "user-1", "SELECT 1", "t-1",
                1, 1,
                100L, 200L, 8192L, 256L,
                5000L, 2500L, 16384L, 8192L,
                4096L, 2048L,
                Instant.now(), true);
        auditService.persist(record);

        var profile = service.getProfile("q-mem-1").orElseThrow();
        var memory = profile.memory();

        assertThat(memory.peakMemoryBytes()).isEqualTo(8192L);
        assertThat(memory.spilledBytes()).isEqualTo(256L);
        assertThat(memory.inputBytes()).isEqualTo(16384L);
        assertThat(memory.outputBytes()).isEqualTo(8192L);
        assertThat(memory.shuffleReadBytes()).isEqualTo(4096L);
        assertThat(memory.shuffleWriteBytes()).isEqualTo(2048L);
    }
}
