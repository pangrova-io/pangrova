package org.pangrova.testing.integration.e2e.write;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.E2ETest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual Worker adapter counts and prepared-file ownership, without replacement sinks. */
class NativeWriteExecutorMetricsTest extends NativeWriteLifecycleTestBase {
    @Test
    void commitWriteReturnsResult() throws Exception {
        String table = newTable();
        try (var fixture = lifecycle()) {
            var opened = fixture.open(table);
            var result = fixture.executor().commitWrite(opened.handle());
            assertThat(result.rowsWritten()).isZero();
            assertThat(result.commitMetadata()).isEmpty();
            assertThat(fixture.executor().activeSessionCount()).isZero();
        }
    }

    @Test
    void abortWriteDoesNotThrow() throws Exception {
        String table = newTable();
        try (var fixture = lifecycle()) {
            var opened = fixture.open(table);
            var file = fixture.appendAndFindOwnedFile(opened, 1);
            fixture.executor().abortWrite(opened.handle());
            assertThat(fixture.executor().activeSessionCount()).isZero();
            assertThat(file).doesNotExist();
        }
    }

    @Test
    void activeSessionCountTracksOpenSessions() throws Exception {
        String table = newTable();
        try (var fixture = lifecycle()) {
            assertThat(fixture.executor().activeSessionCount()).isZero();
            var first = fixture.open(table);
            assertThat(fixture.executor().activeSessionCount()).isEqualTo(1);
            var firstFile = fixture.appendAndFindOwnedFile(first, 1);
            var second = fixture.open(table);
            var secondFile = fixture.appendAndFindOwnedFile(second, 2);
            assertThat(fixture.executor().activeSessionCount()).isEqualTo(2);
            assertThat(fixture.executor().commitWrite(first.handle()).rowsWritten()).isEqualTo(1);
            assertThat(fixture.executor().activeSessionCount()).isEqualTo(1);
            fixture.executor().abortWrite(second.handle());
            assertThat(fixture.executor().activeSessionCount()).isZero();
            assertThat(firstFile).isRegularFile();
            assertThat(secondFile).doesNotExist();
        }
    }

    @Test
    void closeAllSessionsCleansUp() throws Exception {
        String table = newTable();
        try (var fixture = lifecycle()) {
            var first = fixture.open(table);
            var firstFile = fixture.appendAndFindOwnedFile(first, 1);
            var second = fixture.open(table);
            var secondFile = fixture.appendAndFindOwnedFile(second, 2);
            assertThat(fixture.executor().activeSessionCount()).isEqualTo(2);
            fixture.executor().closeAllSessions();
            assertThat(fixture.executor().activeSessionCount()).isZero();
            assertThat(firstFile).doesNotExist();
            assertThat(secondFile).doesNotExist();
        }
    }

    @Test
    void commitFailureIncreasesNoActiveSessions() throws Exception {
        String table = newTable();
        try (var fixture = lifecycle()) {
            var opened = fixture.open(table);
            var file = fixture.appendAndFindOwnedFile(opened, 1);
            fixture.executor().publishCommitMetadataForTask(opened.taskId(), new byte[] {1});
            assertThatThrownBy(() -> fixture.executor().commitWrite(opened.handle()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Conflicting write commit metadata");
            assertThat(fixture.executor().activeSessionCount()).isZero();
            byte[] prepared = java.nio.file.Files.readAllBytes(file);
            assertThat(prepared).isNotEmpty();
            fixture.executor().closeAllSessions();
            assertThat(java.nio.file.Files.readAllBytes(file)).isEqualTo(prepared);
            assertThat(fixture.executor().drainCommitMetadataForTask(opened.taskId())).containsExactly(1);
        }
    }

    @Test
    @E2ETest
    void publicSqlInsertAndSelectCrossActualJniAndPublishExactRows() {
        String table = newTable();
        sqlRunner.execute("INSERT INTO " + qualified(table) + " VALUES (1), (2), (3)");
        assertThat(sqlRunner.execute("SELECT id FROM " + qualified(table) + " ORDER BY id"))
                .extracting(row -> row.getLong("id")).containsExactly(1L, 2L, 3L);
    }
}
