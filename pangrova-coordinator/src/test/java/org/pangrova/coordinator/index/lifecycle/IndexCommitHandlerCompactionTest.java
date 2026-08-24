package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitEventType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for compaction-aware index maintenance in {@link IndexCommitHandler} (R11.5).
 * Verifies that COMPACT events are separated from OVERWRITE in the commit
 * processing pipeline and that mergeByTable correctly escalates event types.
 */
class IndexCommitHandlerCompactionTest {

    @Test
    void mergeByTable_compactEventPreserved() {
        CommitEvent compact = new CommitEvent(
                "cat", "sch", "tbl",
                List.of("new1.parquet"),
                List.of("old1.parquet", "old2.parquet"),
                Instant.now(), "snap-2", CommitEventType.COMPACT);

        Map<String, IndexCommitHandler.MergedCommitFiles> merged =
                IndexCommitHandler.mergeByTable(List.of(compact));

        assertThat(merged).hasSize(1);
        var files = merged.values().iterator().next();
        assertThat(files.eventType).isEqualTo(CommitEventType.COMPACT);
        assertThat(files.addedFiles).containsExactly("new1.parquet");
        assertThat(files.removedFiles)
                .containsExactlyInAnyOrder("old1.parquet", "old2.parquet");
    }

    @Test
    void mergeByTable_compactAndAppendEscalatesToCompact() {
        CommitEvent append = new CommitEvent(
                "cat", "sch", "tbl",
                List.of("new1.parquet"), List.of(),
                Instant.now(), "snap-1", CommitEventType.APPEND);
        CommitEvent compact = new CommitEvent(
                "cat", "sch", "tbl",
                List.of("new2.parquet"), List.of("old1.parquet"),
                Instant.now(), "snap-2", CommitEventType.COMPACT);

        Map<String, IndexCommitHandler.MergedCommitFiles> merged =
                IndexCommitHandler.mergeByTable(List.of(append, compact));

        assertThat(merged).hasSize(1);
        var files = merged.values().iterator().next();
        // COMPACT(1) > APPEND(0)
        assertThat(files.eventType).isEqualTo(CommitEventType.COMPACT);
        assertThat(files.addedFiles)
                .containsExactlyInAnyOrder("new1.parquet", "new2.parquet");
        assertThat(files.removedFiles).containsExactly("old1.parquet");
    }

    @Test
    void mergeByTable_overwriteEscalatesOverCompact() {
        CommitEvent compact = new CommitEvent(
                "cat", "sch", "tbl",
                List.of("new1.parquet"), List.of("old1.parquet"),
                Instant.now(), "snap-1", CommitEventType.COMPACT);
        CommitEvent overwrite = new CommitEvent(
                "cat", "sch", "tbl",
                List.of("new2.parquet"), List.of("old2.parquet"),
                Instant.now(), "snap-2", CommitEventType.OVERWRITE);

        Map<String, IndexCommitHandler.MergedCommitFiles> merged =
                IndexCommitHandler.mergeByTable(List.of(compact, overwrite));

        assertThat(merged).hasSize(1);
        var files = merged.values().iterator().next();
        // OVERWRITE(3) > COMPACT(1)
        assertThat(files.eventType).isEqualTo(CommitEventType.OVERWRITE);
    }

    @Test
    void mergeByTable_separateTablesNotMerged() {
        CommitEvent compact1 = new CommitEvent(
                "cat", "sch", "tbl1",
                List.of("new1.parquet"), List.of("old1.parquet"),
                Instant.now(), "snap-1", CommitEventType.COMPACT);
        CommitEvent compact2 = new CommitEvent(
                "cat", "sch", "tbl2",
                List.of("new2.parquet"), List.of("old2.parquet"),
                Instant.now(), "snap-2", CommitEventType.COMPACT);

        Map<String, IndexCommitHandler.MergedCommitFiles> merged =
                IndexCommitHandler.mergeByTable(List.of(compact1, compact2));

        assertThat(merged).hasSize(2);
    }
}
