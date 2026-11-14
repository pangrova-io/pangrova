package org.pangrova.common.client.query;

import org.pangrova.common.client.result.ResultEndpoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.client.error.ErrorDetail;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract test verifying JSON structure stability for query REST responses.
 *
 * <p>Ensures that {@link QueryStatusResponse} and {@link SubmitQueryResponse}
 * produce backward-compatible JSON field names and types, preventing silent
 * contract breakage for existing clients.
 *
 * <p><b>Validates: Requirements 1.15, 1.16, 2.15, 2.16</b></p>
 */
class QueryResponseJsonContractTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void queryStatusResponse_fullFields_producesExpectedJsonStructure() throws Exception {
        ErrorDetail error = new ErrorDetail("QUERY_TIMEOUT", "TIMEOUT", "Query timed out");
        ResultEndpoint endpoint = new ResultEndpoint("host1", 9090, "ticket-abc", "grpc");
        QueryStats stats = new QueryStats(1500L, 200L, 42L);
        QueryStatusResponse response = new QueryStatusResponse(
                "q-123", "FAILED", error, stats, endpoint);

        JsonNode root = MAPPER.valueToTree(response);

        // Top-level field names
        assertThat(root.has("query_id")).as("must have query_id").isTrue();
        assertThat(root.has("state")).as("must have state").isTrue();
        assertThat(root.has("error")).as("must have error when present").isTrue();
        assertThat(root.has("stats")).as("must have stats when present").isTrue();
        assertThat(root.has("result_endpoint")).as("must have result_endpoint when present").isTrue();

        // Field values
        assertThat(root.get("query_id").asText()).isEqualTo("q-123");
        assertThat(root.get("state").asText()).isEqualTo("FAILED");

        // Error sub-structure
        JsonNode errorNode = root.get("error");
        assertThat(errorNode.has("error_code")).isTrue();
        assertThat(errorNode.has("error_type")).isTrue();
        assertThat(errorNode.has("message")).isTrue();
        assertThat(errorNode.get("error_code").asText()).isEqualTo("QUERY_TIMEOUT");
        assertThat(errorNode.get("error_type").asText()).isEqualTo("TIMEOUT");
        assertThat(errorNode.get("message").asText()).isEqualTo("Query timed out");

        // Result endpoint sub-structure
        JsonNode epNode = root.get("result_endpoint");
        assertThat(epNode.has("host")).isTrue();
        assertThat(epNode.has("port")).isTrue();
        assertThat(epNode.has("ticket")).isTrue();
        assertThat(epNode.has("protocol")).isTrue();
        assertThat(epNode.get("host").asText()).isEqualTo("host1");
        assertThat(epNode.get("port").asInt()).isEqualTo(9090);
        assertThat(epNode.get("ticket").asText()).isEqualTo("ticket-abc");
        assertThat(epNode.get("protocol").asText()).isEqualTo("grpc");
    }

    @Test
    void queryStatusResponse_nullOptionalFields_omitsThemFromJson() throws Exception {
        QueryStatusResponse response = QueryStatusResponse.of(
                "q-456", "RUNNING", null, null);

        JsonNode root = MAPPER.valueToTree(response);

        assertThat(root.has("query_id")).isTrue();
        assertThat(root.has("state")).isTrue();
        assertThat(root.has("error")).as("null error must be omitted").isFalse();
        assertThat(root.has("stats")).as("null stats must be omitted").isFalse();
        assertThat(root.has("result_endpoint")).as("null endpoint must be omitted").isFalse();
    }

    @Test
    void queryStatusResponse_deserializesBackFromJson() throws Exception {
        ErrorDetail error = new ErrorDetail("ERR_01", "TYPE_A", "Something failed");
        ResultEndpoint endpoint = new ResultEndpoint("worker-1", 8081, "tkt-1", "grpc+tls");
        QueryStatusResponse original = QueryStatusResponse.of(
                "q-789", "FINISHED", error, endpoint);

        String json = MAPPER.writeValueAsString(original);
        QueryStatusResponse deserialized = MAPPER.readValue(json, QueryStatusResponse.class);

        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void submitQueryResponse_producesExpectedJsonStructure() throws Exception {
        ResultEndpoint endpoint = new ResultEndpoint("host2", 9091, "ticket-xyz", "grpc");
        SubmitQueryResponse response = new SubmitQueryResponse(
                "q-submit-1", "QUEUED", endpoint, java.util.List.of("col1", "col2"));

        JsonNode root = MAPPER.valueToTree(response);

        assertThat(root.has("query_id")).as("must have query_id").isTrue();
        assertThat(root.has("state")).as("must have state").isTrue();
        assertThat(root.has("result_endpoint")).as("must have result_endpoint").isTrue();
        assertThat(root.has("column_names")).as("must have column_names").isTrue();

        assertThat(root.get("query_id").asText()).isEqualTo("q-submit-1");
        assertThat(root.get("state").asText()).isEqualTo("QUEUED");
        assertThat(root.get("column_names").isArray()).isTrue();
        assertThat(root.get("column_names").size()).isEqualTo(2);
    }

    @Test
    void branchAcknowledgementDistinguishesUnchangedSelectionAndReset() throws Exception {
        SubmitQueryResponse ordinary = new SubmitQueryResponse("q", "FINISHED", null, null);
        JsonNode ordinaryJson = MAPPER.valueToTree(ordinary);
        assertThat(ordinaryJson.has("branch_selection")).isFalse();
        assertThat(MAPPER.treeToValue(ordinaryJson, SubmitQueryResponse.class).branchSelection()).isNull();

        for (BranchSelection change : java.util.List.of(
                new BranchSelection("catalog.db.orders", "dev"), new BranchSelection("catalog.db.orders", null))) {
            SubmitQueryResponse response = new SubmitQueryResponse("q", "FINISHED", null, null, change);
            JsonNode json = MAPPER.valueToTree(response);
            assertThat(json.at("/branch_selection/table").asText()).isEqualTo(change.table());
            assertThat(MAPPER.treeToValue(json, SubmitQueryResponse.class).branchSelection()).isEqualTo(change);
        }
        assertThat(MAPPER.valueToTree(QueryStatusResponse.of("q", "FINISHED", null, null))
                .has("branch_selection")).isFalse();
    }

    @Test
    void errorDetail_nullStackTrace_omittedFromJson() throws Exception {
        ErrorDetail error = new ErrorDetail("CODE_1", "TYPE_1", "msg");

        JsonNode root = MAPPER.valueToTree(error);

        assertThat(root.has("error_code")).isTrue();
        assertThat(root.has("error_type")).isTrue();
        assertThat(root.has("message")).isTrue();
        assertThat(root.has("stack_trace")).as("null stack_trace must be omitted").isFalse();
    }

    @Test
    void errorDetail_withStackTrace_includesItInJson() throws Exception {
        ErrorDetail error = new ErrorDetail("CODE_2", "TYPE_2", "msg", "at Foo.bar(Foo.java:1)");

        JsonNode root = MAPPER.valueToTree(error);

        assertThat(root.has("stack_trace")).isTrue();
        assertThat(root.get("stack_trace").asText()).isEqualTo("at Foo.bar(Foo.java:1)");
    }
}
