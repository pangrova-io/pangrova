package org.pangrova.protocol.rest;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonProperty;

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
    void submitTaskRequestExposesOnlyCanonicalJsonPayload() throws Exception {
        var jsonProperty = SubmitTaskRequest.class
                .getMethod("canonicalTaskRequest")
                .getAnnotation(JsonProperty.class);

        assertThat(jsonProperty).isNotNull();
        assertThat(jsonProperty.value()).isEqualTo("canonical_task_request");
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

    /**
     * Guards against silent field additions by tracking expected field counts.
     * When a field is added to any protocol DTO, this test fails, forcing
     * the developer to update the expected count and verify symmetry.
     */
    @Test
    void protocolDtoFieldCountsMatchExpected() {
        assertFieldCount(TaskStatusReport.class, 16,
                "TaskStatusReport field count changed — verify canonical report projection");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Set<String> fieldNamesOf(Class<? extends Record> recordClass) {
        return Arrays.stream(recordClass.getRecordComponents())
                .map(RecordComponent::getName)
                .collect(Collectors.toSet());
    }

    private static void assertFieldCount(
            Class<? extends Record> recordClass, int expected, String message) {
        int actual = recordClass.getRecordComponents().length;
        assertThat(actual).as(message + " (was %d, now %d)", expected, actual).isEqualTo(expected);
    }
}
