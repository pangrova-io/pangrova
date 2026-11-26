package org.pangrova.common.client.query;

import org.pangrova.common.enums.ResultDisposition;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the public query REST contract: field presence, symmetry, and drift.
 */
class ClientQueryProtocolContractTest {

    private static final Set<String> QUERY_SUBMIT_RESPONSE_REQUIRED_FIELDS = Set.of(
            "queryId", "state", "resultDisposition");

    private static final Set<String> QUERY_STATUS_RESPONSE_REQUIRED_FIELDS = Set.of(
            "queryId", "state", "resultDisposition");

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

    @Test
    void resultDispositionTypeConsistentAcrossQueryDtos() {
        assertThat(fieldType(SubmitQueryResponse.class, "resultDisposition"))
                .isEqualTo(ResultDisposition.class);
        assertThat(fieldType(QueryStatusResponse.class, "resultDisposition"))
                .isEqualTo(ResultDisposition.class);
    }

    @Test
    void queryResponseDtosShareCommonFieldConcepts() {
        Set<String> sharedFields = Set.of("queryId", "state", "resultDisposition", "resultEndpoint");
        Set<String> submitOnlyFields = Set.of("columnNames", "branchSelection", "ownerEndpoint");
        Set<String> statusOnlyFields = Set.of("error", "stats", "version", "drained");

        Set<String> submitFields = fieldNamesOf(SubmitQueryResponse.class);
        Set<String> statusFields = fieldNamesOf(QueryStatusResponse.class);

        for (String shared : sharedFields) {
            assertThat(submitFields)
                    .as("SubmitQueryResponse must contain shared field '%s'", shared)
                    .contains(shared);
            assertThat(statusFields)
                    .as("QueryStatusResponse must contain shared field '%s'", shared)
                    .contains(shared);
        }

        Set<String> unexpectedSubmitFields = submitFields.stream()
                .filter(f -> !sharedFields.contains(f))
                .filter(f -> !submitOnlyFields.contains(f))
                .collect(Collectors.toSet());
        assertThat(unexpectedSubmitFields)
                .as("New SubmitQueryResponse fields not in shared or submitOnly sets. "
                        + "Add to appropriate set or mirror in QueryStatusResponse.")
                .isEmpty();

        Set<String> unexpectedStatusFields = statusFields.stream()
                .filter(f -> !sharedFields.contains(f))
                .filter(f -> !statusOnlyFields.contains(f))
                .collect(Collectors.toSet());
        assertThat(unexpectedStatusFields)
                .as("New QueryStatusResponse fields not in shared or statusOnly sets. "
                        + "Add to appropriate set or mirror in SubmitQueryResponse.")
                .isEmpty();
    }

    @Test
    void queryDtoFieldCountsMatchExpected() {
        assertFieldCount(SubmitQueryResponse.class, 7,
                "SubmitQueryResponse field count changed — verify QueryStatusResponse symmetry");
        assertFieldCount(QueryStatusResponse.class, 8,
                "QueryStatusResponse field count changed — verify SubmitQueryResponse symmetry");
    }

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
