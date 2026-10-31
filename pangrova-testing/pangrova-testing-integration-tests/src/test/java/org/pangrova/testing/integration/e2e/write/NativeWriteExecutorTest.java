package org.pangrova.testing.integration.e2e.write;

import org.junit.jupiter.api.Test;
import org.pangrova.worker.nativebridge.jni.NativeRuntimeException;
import org.pangrova.worker.nativebridge.jni.write.WriteDescriptor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Lifecycle components backed by real deployment, SDK sinks and owned Parquet files. */
class NativeWriteExecutorTest extends NativeWriteLifecycleTestBase {
    @Test
    void unboundTaskCannotOpenAnotherTasksConnectorSink() throws Exception {
        String table = newTable();
        try (var fixture = lifecycle()) {
            var opened = fixture.open(table);
            var unbound = new WriteDescriptor(opened.descriptor().dmlOperation(),
                    opened.descriptor().columns(), Map.of("taskId", "unbound-task"));
            assertThatThrownBy(() -> fixture.executor().beginWrite(unbound))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("binding not found");
            assertThat(fixture.executor().activeSessionCount()).isEqualTo(1);
            fixture.appendAndFindOwnedFile(opened, 1);
            fixture.executor().abortWrite(opened.handle());
            assertThat(fixture.executor().activeSessionCount()).isZero();
            assertThat(NativeWriteLifecycleFixture.files(opened)).isEmpty();
        }
    }

    @Test
    void missingCanonicalTaskIdFailsBeforeOpeningConnectorSink() {
        try (var fixture = lifecycle()) {
            var descriptor = descriptor(Map.of());
            assertThatThrownBy(() -> fixture.executor().beginWrite(descriptor))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("canonical taskId");
            assertThat(fixture.executor().activeSessionCount()).isZero();
            assertThat(fixture.cachedCatalogRevisionCount()).isZero();
        }
    }

    @Test
    void lanceFormatFailsBeforeOpeningConnectorSink() {
        try (var fixture = lifecycle()) {
            var descriptor = descriptor(Map.of("taskId", "lance-task", WriteDescriptor.FILE_FORMAT_PROPERTY, "LANCE"));
            assertThatThrownBy(() -> fixture.executor().beginWrite(descriptor))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("external-file commit metadata");
            assertThat(fixture.executor().activeSessionCount()).isZero();
            assertThat(fixture.cachedCatalogRevisionCount()).isZero();
        }
    }

    @Test
    void blankDrainTaskIdIsRejectedRatherThanDrainingAnotherTask() {
        try (var fixture = lifecycle()) {
            assertThatThrownBy(() -> fixture.executor().drainCommitMetadataForTask(" "))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Canonical taskId");
        }
    }

    @Test
    void nativeCommitMetadataIsTaskIsolatedImmutableAndConflictFenced() {
        try (var fixture = lifecycle()) {
            var executor = fixture.executor();
            byte[] source = {1, 2, 3};
            executor.publishCommitMetadataForTask("write-task", source);
            source[0] = 9;
            executor.publishCommitMetadataForTask("write-task", new byte[] {1, 2, 3});
            assertThat(executor.drainCommitMetadataForTask("other-task")).isEmpty();
            assertThatThrownBy(() -> executor.publishCommitMetadataForTask("write-task", new byte[] {4}))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Conflicting write commit metadata");
            assertThat(executor.drainCommitMetadataForTask("write-task")).containsExactly(1, 2, 3);
        }
    }

    @Test
    void nativeCommitMetadataRequiresExactTaskAndNonemptyPayload() {
        try (var fixture = lifecycle()) {
            var executor = fixture.executor();
            assertThatThrownBy(() -> executor.publishCommitMetadataForTask(" ", new byte[] {1}))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> executor.publishCommitMetadataForTask("write-task", null))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> executor.publishCommitMetadataForTask("write-task", new byte[0]))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(executor.drainCommitMetadataForTask("write-task")).isEmpty();
        }
    }

    @Test
    void overwriteForwardsCanonicalStaticPartitionBindings() throws Exception {
        String table = "static_" + java.util.UUID.randomUUID().toString().replace("-", "");
        sqlRunner.executeDdl("CREATE TABLE " + qualified(table)
                + " (id BIGINT, day VARCHAR, region VARCHAR) PARTITIONED BY (day, region)");
        Map<String, String> partitions = Map.of("day", "2026-09-19", "region", "cn");
        try (var fixture = lifecycle()) {
            var opened = fixture.open(table, WriteDescriptor.DmlOperation.OVERWRITE, partitions);
            assertThat(opened.outputHandle().staticPartitions()).isEqualTo(partitions);
            assertThat(opened.request().getExecutionOptions().getWrite().getOutputBinding()
                    .getConnector().getOutputHandle().getWriteColumnNamesList()).containsExactly("id", "day", "region");
            fixture.executor().abortWrite(opened.handle());
            assertThat(fixture.executor().activeSessionCount()).isZero();
            assertThat(NativeWriteLifecycleFixture.files(opened)).isEmpty();
        }
    }

    @Test
    void closeAllSessionsAbortsAndClosesEachActiveSink() throws Exception {
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
            assertThatThrownBy(() -> fixture.append(first.handle(), 3)).isInstanceOf(NativeRuntimeException.class);
            assertThatThrownBy(() -> fixture.append(second.handle(), 3)).isInstanceOf(NativeRuntimeException.class);
        }
    }

    @Test
    void closeAllSessionsOnEmptyMapIsNoOp() {
        try (var fixture = lifecycle()) {
            assertThat(fixture.executor().activeSessionCount()).isZero();
            fixture.executor().closeAllSessions();
            assertThat(fixture.executor().activeSessionCount()).isZero();
        }
    }

    @Test
    void closeAllSessionsContinuesWhenAbortThrows() throws Exception {
        String table = newTable();
        try (var fixture = lifecycle()) {
            var failed = fixture.open(table);
            var failedFile = fixture.appendAndFindOwnedFile(failed, 1);
            var other = fixture.open(table);
            var otherFile = fixture.appendAndFindOwnedFile(other, 2);
            try (var barrier = new NativeWriteLifecycleFixture.DeletionBarrier(failedFile)) {
                assertThatThrownBy(fixture.executor()::closeAllSessions).isInstanceOf(RuntimeException.class);
                assertThat(fixture.executor().activeSessionCount()).isEqualTo(1);
                assertThat(failedFile).isDirectory();
                assertThat(otherFile).doesNotExist();
                assertThatThrownBy(() -> fixture.append(failed.handle(), 3))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("stopped");
            }
            fixture.executor().closeAllSessions();
            assertThat(fixture.executor().activeSessionCount()).isZero();
            assertThat(failedFile).doesNotExist();
        }
    }

    @Test
    void closeViaAutoCloseableDelegatesToCloseAllSessions() throws Exception {
        String table = newTable();
        try (var fixture = lifecycle()) {
            var opened = fixture.open(table);
            var file = fixture.appendAndFindOwnedFile(opened, 1);
            assertThat(fixture.executor().activeSessionCount()).isEqualTo(1);
            assertThatThrownBy(fixture.executor()::close).isInstanceOf(IllegalStateException.class);
            assertThat(fixture.executor().activeSessionCount()).isZero();
            assertThat(file).doesNotExist();
            fixture.executor().releaseTask(opened.taskId());
            fixture.executor().close();
        }
    }

    @Test
    void closeIsIdempotent() throws Exception {
        String table = newTable();
        try (var fixture = lifecycle()) {
            var opened = fixture.open(table);
            assertThatThrownBy(fixture.executor()::close).isInstanceOf(IllegalStateException.class);
            fixture.executor().releaseTask(opened.taskId());
            fixture.executor().close();
            fixture.executor().close();
            assertThat(fixture.executor().activeSessionCount()).isZero();
        }
    }

    @Test
    void committedSessionsAreNotAffectedByCloseAllSessions() throws Exception {
        String table = newTable();
        try (var fixture = lifecycle()) {
            var committed = fixture.open(table);
            var committedFile = fixture.appendAndFindOwnedFile(committed, 1);
            var other = fixture.open(table);
            var otherFile = fixture.appendAndFindOwnedFile(other, 2);
            var result = fixture.executor().commitWrite(committed.handle());
            byte[] prepared = java.nio.file.Files.readAllBytes(committedFile);
            assertThat(result.commitMetadata()).isNotEmpty();
            assertThat(fixture.executor().activeSessionCount()).isEqualTo(1);
            fixture.executor().closeAllSessions();
            assertThat(fixture.executor().activeSessionCount()).isZero();
            assertThat(java.nio.file.Files.readAllBytes(committedFile)).isEqualTo(prepared);
            assertThat(otherFile).doesNotExist();
            assertThat(fixture.executor().drainCommitMetadataForTask(committed.taskId()))
                    .isEqualTo(result.commitMetadata());
        }
    }

    private static WriteDescriptor descriptor(Map<String, String> properties) {
        return new WriteDescriptor(WriteDescriptor.DmlOperation.INSERT,
                List.of(new WriteDescriptor.ColumnSchema("id", "BIGINT")), properties);
    }
}
