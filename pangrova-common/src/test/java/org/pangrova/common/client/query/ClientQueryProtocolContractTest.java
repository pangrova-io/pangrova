package org.pangrova.common.client.query;

import org.pangrova.common.enums.ResultDisposition;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the public query REST contract: exact response fields and shared types.
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
    void submitResponseCarriesAdmissionReceiptFields() {
        assertThat(fieldNamesOf(SubmitQueryResponse.class))
                .containsExactlyInAnyOrder("queryId", "state", "resultDisposition", "resultEndpoint",
                        "columnNames", "ownerEndpoint");
    }

    @Test
    void statusResponseCarriesReadinessAndDrainFields() {
        assertThat(fieldNamesOf(QueryStatusResponse.class))
                .containsExactlyInAnyOrder("queryId", "state", "error", "stats", "resultDisposition",
                        "resultEndpoint", "version", "drained", "branchSelection");
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
}
