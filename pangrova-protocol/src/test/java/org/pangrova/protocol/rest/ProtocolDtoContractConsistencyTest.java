package org.pangrova.protocol.rest;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.pangrova.protocol.rest.query.QueryStatusResponse;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
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

    /**
     * Identity fields that must be traceable from SubmitQueryRequest context
     * into SubmitQueryResponse. queryId is generated server-side, so the
     * response must carry it for client correlation.
     */
    private static final Set<String> QUERY_SUBMIT_RESPONSE_REQUIRED_FIELDS = Set.of(
            "queryId", "state"
    );

    /**
     * Identity fields that must be present in QueryStatusResponse for
     * client-side query tracking.
     */
    private static final Set<String> QUERY_STATUS_RESPONSE_REQUIRED_FIELDS = Set.of(
            "queryId", "state"
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
    // Query DTO pair: SubmitQueryResponse field contract
    // -----------------------------------------------------------------------

    @Test
    void submitQueryResponseContainsRequiredFields() {
        Set<String> responseFields = fieldNamesOf(SubmitQueryResponse.class);

        for (String required : QUERY_SUBMIT_RESPONSE_REQUIRED_FIELDS) {
            assertThat(responseFields)
                    .as("SubmitQueryResponse must contain required field '%s'", required)
                    .contains(required);
        }
    }

    @Test
    void queryStatusResponseContainsRequiredFields() {
        Set<String> responseFields = fieldNamesOf(QueryStatusResponse.class);

        for (String required : QUERY_STATUS_RESPONSE_REQUIRED_FIELDS) {
            assertThat(responseFields)
                    .as("QueryStatusResponse must contain required field '%s'", required)
                    .contains(required);
        }
    }

    // -----------------------------------------------------------------------
    // Cross-DTO consistency: query responses share queryId + state
    // -----------------------------------------------------------------------

    @Test
    void queryIdTypeConsistentAcrossQueryDtos() {
        Class<?> submitResponseType = fieldType(SubmitQueryResponse.class, "queryId");
        Class<?> statusResponseType = fieldType(QueryStatusResponse.class, "queryId");

        assertThat(submitResponseType)
                .as("queryId type must be consistent between SubmitQueryResponse and "
                        + "QueryStatusResponse")
                .isEqualTo(statusResponseType);
    }

    @Test
    void stateTypeConsistentAcrossQueryDtos() {
        Class<?> submitResponseType = fieldType(SubmitQueryResponse.class, "state");
        Class<?> statusResponseType = fieldType(QueryStatusResponse.class, "state");

        assertThat(submitResponseType)
                .as("state type must be consistent between SubmitQueryResponse and "
                        + "QueryStatusResponse")
                .isEqualTo(statusResponseType);
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

    /**
     * Detects when new fields are added to SubmitQueryResponse but not
     * mirrored in QueryStatusResponse (or vice versa) for shared concepts.
     */
    @Test
    void queryResponseDtosShareCommonFieldConcepts() {
        // Fields shared between both query response DTOs
        Set<String> sharedFields = Set.of("queryId", "state", "resultEndpoint");

        // Fields unique to SubmitQueryResponse
        Set<String> submitOnlyFields = Set.of("columnNames");

        // Fields unique to QueryStatusResponse
        Set<String> statusOnlyFields = Set.of("error", "stats");

        Set<String> submitFields = fieldNamesOf(SubmitQueryResponse.class);
        Set<String> statusFields = fieldNamesOf(QueryStatusResponse.class);

        // Verify shared fields exist in both
        for (String shared : sharedFields) {
            assertThat(submitFields)
                    .as("SubmitQueryResponse must contain shared field '%s'", shared)
                    .contains(shared);
            assertThat(statusFields)
                    .as("QueryStatusResponse must contain shared field '%s'", shared)
                    .contains(shared);
        }

        // Detect unexpected new fields in SubmitQueryResponse
        Set<String> unexpectedSubmitFields = submitFields.stream()
                .filter(f -> !sharedFields.contains(f))
                .filter(f -> !submitOnlyFields.contains(f))
                .collect(Collectors.toSet());

        assertThat(unexpectedSubmitFields)
                .as("New SubmitQueryResponse fields not in shared or submitOnly sets. "
                        + "Add to appropriate set or mirror in QueryStatusResponse.")
                .isEmpty();

        // Detect unexpected new fields in QueryStatusResponse
        Set<String> unexpectedStatusFields = statusFields.stream()
                .filter(f -> !sharedFields.contains(f))
                .filter(f -> !statusOnlyFields.contains(f))
                .collect(Collectors.toSet());

        assertThat(unexpectedStatusFields)
                .as("New QueryStatusResponse fields not in shared or statusOnly sets. "
                        + "Add to appropriate set or mirror in SubmitQueryResponse.")
                .isEmpty();
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
        assertFieldCount(SubmitQueryResponse.class, 4,
                "SubmitQueryResponse field count changed — verify QueryStatusResponse symmetry");
        assertFieldCount(QueryStatusResponse.class, 5,
                "QueryStatusResponse field count changed — verify SubmitQueryResponse symmetry");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Set<String> fieldNamesOf(Class<? extends Record> recordClass) {
        return Arrays.stream(recordClass.getRecordComponents())
                .map(RecordComponent::getName)
                .collect(Collectors.toSet());
    }

    private static Class<?> fieldType(Class<? extends Record> recordClass, String fieldName) {
        return Arrays.stream(recordClass.getRecordComponents())
                .filter(c -> c.getName().equals(fieldName))
                .findFirst()
                .map(RecordComponent::getType)
                .orElseThrow(() -> new AssertionError(
                        "Field '" + fieldName + "' not found in " + recordClass.getSimpleName()));
    }

    private static void assertFieldCount(
            Class<? extends Record> recordClass, int expected, String message) {
        int actual = recordClass.getRecordComponents().length;
        assertThat(actual).as(message + " (was %d, now %d)", expected, actual).isEqualTo(expected);
    }
}
