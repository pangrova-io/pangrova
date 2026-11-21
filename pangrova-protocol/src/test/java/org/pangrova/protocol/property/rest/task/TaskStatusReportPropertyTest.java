package org.pangrova.protocol.property.rest.task;

import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property-based tests for {@link TaskStatusReport} field validation.
 *
 * <p><b>Validates: Requirements 1.19, 1.20, 2.19, 2.20</b></p>
 */
class TaskStatusReportPropertyTest {

    private static final String[] MANDATORY_FIELDS = {"taskId", "queryId", "workerId", "state"};

    /**
     * Property 1: Construction with any null mandatory field always throws
     * IllegalArgumentException.
     *
     * <p>Generates a random index selecting which mandatory field to nullify,
     * then verifies the constructor rejects the input with a clear message
     * naming the offending field.</p>
     */
    @Property(tries = 10)
    void nullMandatoryFieldAlwaysThrows(
            @ForAll("validStrings") String taskId,
            @ForAll("validStrings") String queryId,
            @ForAll("validStrings") String workerId,
            @ForAll("validStates") String state,
            @ForAll("mandatoryFieldIndex") int nullIndex
    ) {
        String effectiveTaskId = nullIndex == 0 ? null : taskId;
        String effectiveQueryId = nullIndex == 1 ? null : queryId;
        String effectiveWorkerId = nullIndex == 2 ? null : workerId;
        String effectiveState = nullIndex == 3 ? null : state;

        assertThatThrownBy(() -> new TaskStatusReport(
                effectiveTaskId, effectiveQueryId, 0, 0, "COMPUTE",
                effectiveWorkerId, effectiveState,
                null, null, null, 1L, System.currentTimeMillis(),
                null, null, TaskAttemptDescriptor.of(1, 0)
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(MANDATORY_FIELDS[nullIndex]);
    }

    /**
     * Property 2: Construction with blank mandatory field always throws
     * IllegalArgumentException.
     *
     * <p>Blank strings (empty or whitespace-only) are treated the same as
     * null for mandatory fields.</p>
     */
    @Property(tries = 10)
    void blankMandatoryFieldAlwaysThrows(
            @ForAll("validStrings") String taskId,
            @ForAll("validStrings") String queryId,
            @ForAll("validStrings") String workerId,
            @ForAll("validStates") String state,
            @ForAll("mandatoryFieldIndex") int blankIndex,
            @ForAll("blankStrings") String blankValue
    ) {
        String effectiveTaskId = blankIndex == 0 ? blankValue : taskId;
        String effectiveQueryId = blankIndex == 1 ? blankValue : queryId;
        String effectiveWorkerId = blankIndex == 2 ? blankValue : workerId;
        String effectiveState = blankIndex == 3 ? blankValue : state;

        assertThatThrownBy(() -> new TaskStatusReport(
                effectiveTaskId, effectiveQueryId, 0, 0, "COMPUTE",
                effectiveWorkerId, effectiveState,
                null, null, null, 1L, System.currentTimeMillis(),
                null, null, TaskAttemptDescriptor.of(1, 0)
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(MANDATORY_FIELDS[blankIndex]);
    }

    /**
     * Property 3: Construction with all valid mandatory fields succeeds and
     * preserves every field value.
     */
    @Property(tries = 10)
    void validMandatoryFieldsAlwaysSucceed(
            @ForAll("validStrings") String taskId,
            @ForAll("validStrings") String queryId,
            @ForAll("validStrings") String workerId,
            @ForAll("validStates") String state
    ) {
        TaskStatusReport report = new TaskStatusReport(
                taskId, queryId, 1, 2, "COMPUTE",
                workerId, state,
                null, null, null, 5L, 1700000000000L,
                null, null, TaskAttemptDescriptor.of(1, 0)
        );

        assertThat(report.taskId()).isEqualTo(taskId);
        assertThat(report.queryId()).isEqualTo(queryId);
        assertThat(report.workerId()).isEqualTo(workerId);
        assertThat(report.state()).isEqualTo(state);
        assertThat(report.stats()).isNull();
        assertThat(report.error()).isNull();
        assertThat(report.writeResult()).isNull();
        assertThat(report.attempt()).isEqualTo(TaskAttemptDescriptor.of(1, 0));
    }

    @Property(tries = 10)
    void nullAttemptAlwaysThrows(@ForAll("validStrings") String taskId) {
        assertThatThrownBy(() -> new TaskStatusReport(
                taskId, "query", 0, 0, "COMPUTE", "worker", "RUNNING",
                null, null, null, 1L, System.currentTimeMillis(),
                null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attempt");
    }

    // --- Generators ---

    @Provide
    Arbitrary<String> validStrings() {
        return Arbitraries.strings()
                .alpha()
                .ofMinLength(1)
                .ofMaxLength(64)
                .map(s -> "id-" + s);
    }

    @Provide
    Arbitrary<String> validStates() {
        return Arbitraries.of("RUNNING", "FINISHED", "FAILED", "CANCELLED");
    }

    @Provide
    Arbitrary<Integer> mandatoryFieldIndex() {
        return Arbitraries.integers().between(0, 3);
    }

    @Provide
    Arbitrary<String> blankStrings() {
        return Arbitraries.of("", " ", "  ", "\t", "\n");
    }
}
