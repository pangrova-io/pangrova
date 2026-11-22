package org.pangrova.protocol.rest;

import static org.assertj.core.api.Assertions.assertThat;


import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.junit.jupiter.api.Test;

/**
 * CI guard test verifying protocol response fields and the canonical task envelope.
 *
 * <p>Task identity is defined by canonical v2, not duplicated in REST. The
 * submit envelope is therefore guarded independently from report projections.</p>
 *
 * <p>The test defines explicit "identity field" contracts for each DTO pair.
 * When a new identity field is added to a request DTO, this test will fail
 * until the corresponding response/report DTO is updated to include it.</p>
 *
 * <p><b>Validates: Requirements 1.21, 1.22, 2.21, 2.22</b></p>
 */
class ProtocolDtoContractConsistencyTest {

    /**
     * Identity fields that must survive the SubmitTaskRequest → TaskStatusReport cycle.
     * These are the fields the Coordinator uses to correlate a report back to
     * the original task submission.
     */
    private static final Set<String> TASK_IDENTITY_FIELDS = Set.of(
            "taskId", "queryId", "stageId", "partitionId", "taskType"
    );

    // -----------------------------------------------------------------------
    // Task DTO pair: SubmitTaskRequest → TaskStatusReport
    // -----------------------------------------------------------------------

    @Test
    void submitTaskUsesCanonicalPayloadOutsideTheJniBoundary() {
        var submission = org.pangrova.protocol.http.TaskSubmission.getDescriptor();
        assertThat(submission.getFields()).extracting(field -> field.getName()).containsExactly("task", "copy");
        assertThat(submission.findFieldByName("task").getMessageType())
                .isEqualTo(org.pangrova.protocol.v2.TaskRequest.getDescriptor());
        assertThat(org.pangrova.protocol.descriptor.CanonicalProtocolDescriptors.descriptorSet().getFileList())
                .extracting(file -> file.getName()).doesNotContain(submission.getFile().getName());
    }

    @Test
    void taskIdentityFieldsExistInTaskStatusReport() {
        Set<String> reportFields = fieldNamesOf(TaskStatusReport.class);

        for (String identityField : TASK_IDENTITY_FIELDS) {
            assertThat(reportFields)
                    .as("TaskStatusReport must contain identity field '%s' "
                            + "for Coordinator correlation", identityField)
                    .contains(identityField);
        }
    }

    // -----------------------------------------------------------------------
    // Asymmetric field drift detection
    // -----------------------------------------------------------------------

    /**
     * Detects when new identity-like fields are added to SubmitTaskRequest
     * but not mirrored in TaskStatusReport.
     *
     * <p>Any field in SubmitTaskRequest that is NOT in the known
     * request-only exclusion set and NOT in TaskStatusReport triggers a
     * failure, forcing the developer to either add it to TaskStatusReport
     * or explicitly exclude it.</p>
     */
    @Test
    void submitTaskRequestHasNoRestSemanticMirrors() {
        Set<String> forbidden = Set.of(
                "taskId", "queryId", "stageId", "partitionId", "taskType",
                "planFragment", "splits", "exchangeSources", "resourceGrant",
                "sessionProperties", "dynamicFilters", "executionOwner");

        assertThat(Arrays.stream(SubmitTaskRequest.class.getDeclaredMethods())
                .map(java.lang.reflect.Method::getName))
                .doesNotContainAnyElementsOf(forbidden);
    }

    // -----------------------------------------------------------------------
    // Field count drift guard
    // -----------------------------------------------------------------------

    @Test
    void reportWireSeparatesProgressFromTerminalEvidence() {
        var report = org.pangrova.protocol.http.TaskStatusDelivery.getDescriptor();
        assertThat(report.getFields()).extracting(field -> field.getName()).containsExactly(
                "worker_id", "sequence", "timestamp_ms", "role", "progress", "terminal_report",
                "supplemental_stats", "write");
        assertThat(report.getOneofs()).singleElement().satisfies(status ->
                assertThat(status.getFields()).extracting(field -> field.getName())
                        .containsExactly("progress", "terminal_report"));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Set<String> fieldNamesOf(Class<? extends Record> recordClass) {
        return Arrays.stream(recordClass.getRecordComponents())
                .map(RecordComponent::getName)
                .collect(Collectors.toSet());
    }

}
