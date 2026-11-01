package org.pangrova.query.core.profiling;

import org.pangrova.query.adapter.audit.LogAuditStore;
import org.pangrova.spi.query.audit.QueryAuditRecord;
import org.pangrova.query.core.audit.QueryAuditService;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests QueryProfileService: wires LogAuditStore.findById into profile lookup
 * and verifies structured JSON output with operator tree, timing, and memory.
 */
class QueryProfileServiceTest {

    @Test
    void profileCopiesInputCollectionsAndExposesOnlyImmutableLists() {
        var children = new ArrayList<QueryProfileService.OperatorNode>();
        var child = new QueryProfileService.OperatorNode("child", "task", 1, 1, 1, 1, List.of());
        children.add(child);
        var root = new QueryProfileService.OperatorNode("root", "root", 1, 1, 1, 1, children);
        var tree = new ArrayList<>(List.of(root));
        var frame = new QueryProfileService.FlameGraphFrame("root", 1);
        var frames = new ArrayList<>(List.of(frame));
        var profile = new QueryProfileService.QueryProfile("q", "SELECT 1", true,
                tree, new QueryProfileService.TimingBreakdown(1, 1, 0, 1, 1),
                new QueryProfileService.MemoryUsage(1, 0, 1, 1, 0, 0), frames);
        children.clear();
        tree.clear();
        frames.clear();

        assertThat(profile.operatorTree()).containsExactly(root);
        assertThat(root.children()).containsExactly(child);
        assertThat(profile.flameGraph()).containsExactly(frame);
        assertThatThrownBy(() -> profile.operatorTree().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> root.children().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> profile.flameGraph().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void getProfileReturnsEmptyWhenQueryNotFound() {
        var store = new LogAuditStore();
        var service = new QueryProfileService(new QueryAuditService(store));

        Optional<QueryProfileService.QueryProfile> result = service.getProfile("nonexistent");

        assertThat(result).isEmpty();
    }

    @Test
    void getProfileReturnsStructuredDataAfterAuditSave() {
        var store = new LogAuditStore();
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
    void profileContainsOperatorTree() {
        var store = new LogAuditStore();
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

        assertThat(profile.operatorTree()).isNotEmpty();
        var root = profile.operatorTree().get(0);
        assertThat(root.name()).isEqualTo("QueryRoot");
        assertThat(root.type()).isEqualTo("root");
        assertThat(root.wallTimeMs()).isEqualTo(400L);
        assertThat(root.cpuTimeMs()).isEqualTo(200L);
        assertThat(root.children()).hasSize(2);

        var stage0 = root.children().get(0);
        assertThat(stage0.name()).isEqualTo("Stage-0");
        assertThat(stage0.type()).isEqualTo("stage");
        assertThat(stage0.children()).hasSize(2);

        var task = stage0.children().get(0);
        assertThat(task.type()).isEqualTo("task");
    }

    @Test
    void profileContainsTimingBreakdown() {
        var store = new LogAuditStore();
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
        assertThat(timing.ioWaitMs()).isEqualTo(200L);
        assertThat(timing.inputRows()).isEqualTo(2000L);
        assertThat(timing.outputRows()).isEqualTo(1000L);
    }

    @Test
    void profileContainsMemoryUsage() {
        var store = new LogAuditStore();
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

    @Test
    void profileContainsFlameGraphFrames() {
        var store = new LogAuditStore();
        var auditService = new QueryAuditService(store);
        var service = new QueryProfileService(auditService);

        var record = new QueryAuditRecord(
                "q-flame-1", "user-1", "SELECT 1", "t-1",
                1, 2,
                400L, 800L, 4096L, 0L,
                1000L, 500L, 2048L, 1024L,
                0L, 0L,
                Instant.now(), true);
        auditService.persist(record);

        var profile = service.getProfile("q-flame-1").orElseThrow();

        assertThat(profile.flameGraph()).isNotEmpty();
        var rootFrame = profile.flameGraph().get(0);
        assertThat(rootFrame.stackPath()).isEqualTo("QueryRoot");
        assertThat(rootFrame.durationMs()).isGreaterThan(0);
    }
}
