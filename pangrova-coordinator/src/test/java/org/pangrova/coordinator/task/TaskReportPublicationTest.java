package org.pangrova.coordinator.task;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStats;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.TaskKind;
import static org.assertj.core.api.Assertions.assertThat;

/** Frozen report identity contracts, not Native execution or drain evidence. */
class TaskReportPublicationTest {
    private final SubmitTaskRequest request = CanonicalTaskRequestTestFixture.request(
            "task", "query", 0, 0, TaskKind.TASK_KIND_READ, new byte[]{1});

    @Test
    void progressWithoutNativeTerminalBytesMatchesItsExactRetryAfterCompaction() {
        var original = report("RUNNING", stats(1), null, null, null, null, null);
        var fact = new TaskReportPublication.Fact(request, original, false);
        var retry = report("RUNNING", stats(1), null, null, null, null, null);
        assertThat(fact.matches(retry)).isTrue();
        fact.report = null;
        fact.request = null;
        assertThat(fact.matches(retry)).isTrue();
        assertThat(fact.matches(report("RUNNING", stats(1), null, null, null, null, new byte[0]))).isFalse();
    }

    @Test
    void sameProgressIdentityRejectsEveryChangedRestPayloadField() {
        var error = new ErrorDetail("CODE", "TYPE", "message");
        var write = new WriteResultDescriptor(List.of(), 1, 2, new byte[]{3});
        var original = report("RUNNING", stats(1), error, write, "CODE", Map.of("detail", "original"), null);
        var fact = new TaskReportPublication.Fact(request, original, false);
        assertThat(fact.matches(report("RUNNING", stats(1), new ErrorDetail("CODE", "TYPE", "message"),
                new WriteResultDescriptor(List.of(), 1, 2, new byte[]{3}), "CODE", Map.of("detail", "original"), null)))
                .isTrue();
        var mutations = List.of(
                report("RUNNING", stats(2), error, write, "CODE", original.errorMetadata(), null),
                report("RUNNING", stats(1), new ErrorDetail("CODE", "TYPE", "changed"), write,
                        "CODE", original.errorMetadata(), null),
                report("RUNNING", stats(1), error, new WriteResultDescriptor(List.of(), 1, 2, new byte[]{4}),
                        "CODE", original.errorMetadata(), null),
                report("RUNNING", stats(1), error, write, "CHANGED", original.errorMetadata(), null),
                report("RUNNING", stats(1), error, write, "CODE", Map.of("detail", "changed"), null));
        for (var changed : mutations) assertThat(fact.matches(changed)).as("changed payload %s", changed).isFalse();
    }

    @Test
    void canonicalBytesRemainExactAndCannotBecomeAbsentOnRetry() {
        var original = report("FINISHED", stats(1), null, null, null, null, new byte[]{1, 2});
        var fact = new TaskReportPublication.Fact(request, original, false);
        assertThat(fact.matches(report("FINISHED", stats(1), null, null, null, null, new byte[]{1, 2}))).isTrue();
        assertThat(fact.matches(report("FINISHED", stats(1), null, null, null, null, new byte[]{1, 3}))).isFalse();
        assertThat(fact.matches(report("FINISHED", stats(1), null, null, null, null, null))).isFalse();
        assertThat(fact.matches(report("FINISHED", stats(2), null, null, null, null, new byte[]{1, 2}))).isFalse();
    }

    private TaskStatusReport report(String state, TaskStats stats, ErrorDetail error,
            WriteResultDescriptor write, String code, Map<String, String> metadata, byte[] canonical) {
        return new TaskStatusReport("task", "query", 0, 0, "READ", "worker", state,
                stats, error, write, 1, 1000, code, metadata,
                TaskAttemptDescriptor.from(request.taskRequest().getAttempt()), canonical);
    }

    private static TaskStats stats(long rows) {
        return new TaskStats(rows, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, List.of(), 0, 0, 0, 0, 0);
    }
}
