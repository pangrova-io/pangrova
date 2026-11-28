package org.pangrova.common.client.query;

import org.pangrova.common.client.result.ResultEndpoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.enums.ResultDisposition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract test verifying JSON structure stability for query REST responses.
 *
 * <p>Ensures that {@link QueryStatusResponse} and {@link SubmitQueryResponse}
 * produce stable JSON field names and types, preventing silent contract drift.
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
                "q-123", "FAILED", error, stats, ResultDisposition.STREAM, endpoint, 17L, false, null);

        JsonNode root = MAPPER.valueToTree(response);

        // Top-level field names
        assertThat(root.has("query_id")).as("must have query_id").isTrue();
        assertThat(root.has("state")).as("must have state").isTrue();
        assertThat(root.has("error")).as("must have error when present").isTrue();
        assertThat(root.has("stats")).as("must have stats when present").isTrue();
        assertThat(root.has("result_endpoint")).as("must have result_endpoint when present").isTrue();
        assertThat(root.get("result_disposition").asText()).isEqualTo("STREAM");

        // Field values
        assertThat(root.get("query_id").asText()).isEqualTo("q-123");
        assertThat(root.get("state").asText()).isEqualTo("FAILED");
        assertThat(root.path("version").isIntegralNumber()).isTrue();
        assertThat(root.path("version").longValue()).isEqualTo(17L);
        assertThat(root.path("drained").isBoolean()).isTrue();
        assertThat(root.path("drained").booleanValue()).isFalse();
        assertThat(MAPPER.treeToValue(root, QueryStatusResponse.class)).isEqualTo(response);

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
                "q-456", "RUNNING", null, ResultDisposition.PENDING, null);

        JsonNode root = MAPPER.valueToTree(response);

        assertThat(root.has("query_id")).isTrue();
        assertThat(root.has("state")).isTrue();
        assertThat(root.has("error")).as("null error must be omitted").isFalse();
        assertThat(root.has("stats")).as("null stats must be omitted").isFalse();
        assertThat(root.has("result_endpoint")).as("null endpoint must be omitted").isFalse();
        assertThat(root.get("result_disposition").asText()).isEqualTo("PENDING");
        assertThat(root.has("version")).as("null version must be omitted").isFalse();
        assertThat(root.has("drained")).as("null drained status must be omitted").isFalse();
    }

    @Test
    void queryStatusResponse_deserializesBackFromJson() throws Exception {
        ErrorDetail error = new ErrorDetail("ERR_01", "TYPE_A", "Something failed");
        ResultEndpoint endpoint = new ResultEndpoint("worker-1", 8081, "tkt-1", "grpc+tls");
        QueryStatusResponse original = QueryStatusResponse.of(
                "q-789", "FINISHED", error, ResultDisposition.STREAM, endpoint);

        String json = MAPPER.writeValueAsString(original);
        QueryStatusResponse deserialized = MAPPER.readValue(json, QueryStatusResponse.class);

        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void submitQueryResponse_producesExpectedJsonStructure() throws Exception {
        ResultEndpoint endpoint = new ResultEndpoint("host2", 9091, "ticket-xyz", "grpc");
        SubmitQueryResponse response = new SubmitQueryResponse(
                "q-submit-1", "QUEUED", ResultDisposition.STREAM, endpoint,
                java.util.List.of("col1", "col2"));

        JsonNode root = MAPPER.valueToTree(response);

        assertThat(root.has("query_id")).as("must have query_id").isTrue();
        assertThat(root.has("state")).as("must have state").isTrue();
        assertThat(root.has("result_endpoint")).as("must have result_endpoint").isTrue();
        assertThat(root.get("result_disposition").asText()).isEqualTo("STREAM");
        assertThat(root.has("column_names")).as("must have column_names").isTrue();

        assertThat(root.get("query_id").asText()).isEqualTo("q-submit-1");
        assertThat(root.get("state").asText()).isEqualTo("QUEUED");
        assertThat(root.get("column_names").isArray()).isTrue();
        assertThat(root.get("column_names").size()).isEqualTo(2);
    }

    @Test
    void branchAcknowledgementDistinguishesUnchangedSelectionAndReset() throws Exception {
        QueryStatusResponse ordinary = QueryStatusResponse.of("q", "FINISHED", null, ResultDisposition.NONE, null);
        JsonNode ordinaryJson = MAPPER.valueToTree(ordinary);
        assertThat(ordinaryJson.has("branch_selection")).isFalse();
        assertThat(MAPPER.treeToValue(ordinaryJson, QueryStatusResponse.class).branchSelection()).isNull();

        for (BranchSelection change : java.util.List.of(
                new BranchSelection("catalog.db.orders", "dev"), new BranchSelection("catalog.db.orders", null))) {
            QueryStatusResponse response = new QueryStatusResponse(
                    "q", "FINISHED", null, null, ResultDisposition.NONE, null, 1L, false, change);
            JsonNode json = MAPPER.valueToTree(response);
            assertThat(json.at("/branch_selection/table").asText()).isEqualTo(change.table());
            assertThat(MAPPER.treeToValue(json, QueryStatusResponse.class).branchSelection()).isEqualTo(change);
            var recovered = QueryStatusResponse.recovered("q", "FINISHED", null, ResultDisposition.NONE, change);
            assertThat(MAPPER.readValue(MAPPER.writeValueAsBytes(recovered), QueryStatusResponse.class))
                    .isEqualTo(recovered);
        }
        assertThat(MAPPER.valueToTree(QueryStatusResponse.of(
                "q", "FINISHED", null, ResultDisposition.NONE, null))
                .has("branch_selection")).isFalse();
    }

    @Test
    void ownerEndpointIsAnOptionalSubmitReceiptField() throws Exception {
        var owner = new QueryOwnerEndpoint("coordinator-1", "incarnation-1", "localhost", 8080);
        var receipt = new SubmitQueryResponse(
                "q", "QUEUED", ResultDisposition.PENDING, null, null, owner);
        JsonNode json = MAPPER.valueToTree(receipt);
        assertThat(json.at("/owner_endpoint/coordinator_id").asText()).isEqualTo("coordinator-1");
        assertThat(json.at("/owner_endpoint/incarnation").asText()).isEqualTo("incarnation-1");
        assertThat(json.at("/owner_endpoint/host").asText()).isEqualTo("localhost");
        assertThat(json.at("/owner_endpoint/port").asInt()).isEqualTo(8080);
        assertThat(MAPPER.treeToValue(json, SubmitQueryResponse.class)).isEqualTo(receipt);

        var legacyReceipt = new SubmitQueryResponse("q", "QUEUED", ResultDisposition.PENDING, null, null);
        JsonNode legacyJson = MAPPER.valueToTree(legacyReceipt);
        assertThat(legacyJson.has("owner_endpoint")).isFalse();
        assertThat(MAPPER.treeToValue(legacyJson, SubmitQueryResponse.class).ownerEndpoint()).isNull();
        assertThat(MAPPER.valueToTree(QueryStatusResponse.of(
                "q", "QUEUED", null, ResultDisposition.PENDING, null)).has("owner_endpoint")).isFalse();
    }

    @Test
    void liveResultDispositionIsRequiredAndMustAgreeWithEndpoint() {
        var endpoint = new ResultEndpoint("worker", 8080, "ticket", "grpc");

        assertThat(ResultDisposition.values())
                .containsExactly(ResultDisposition.PENDING, ResultDisposition.STREAM, ResultDisposition.NONE,
                        ResultDisposition.UNAVAILABLE);
        assertThatThrownBy(() -> new SubmitQueryResponse(
                "q", "FINISHED", ResultDisposition.UNAVAILABLE, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueryStatusResponse.of(
                "q", "FINISHED", null, ResultDisposition.UNAVAILABLE, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(QueryStatusResponse.recovered("q", "FINISHED", null, ResultDisposition.UNAVAILABLE, null)
                .recovered()).isTrue();
        assertThatThrownBy(() -> new SubmitQueryResponse(
                "q", "RUNNING", ResultDisposition.STREAM, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SubmitQueryResponse(
                "q", "RUNNING", ResultDisposition.PENDING, endpoint, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SubmitQueryResponse(
                "q", "FINISHED", ResultDisposition.NONE, endpoint, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SubmitQueryResponse(
                "q", "FAILED", ResultDisposition.PENDING, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SubmitQueryResponse("q", "RUNNING", null, null, null))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> QueryStatusResponse.of(
                "q", "RUNNING", null, ResultDisposition.STREAM, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueryStatusResponse.of(
                "q", "CANCELLED", null, ResultDisposition.PENDING, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueryStatusResponse.of(
                "q", "FINISHED", null, ResultDisposition.NONE, endpoint))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueryStatusResponse.of("q", "RUNNING", null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Live status");
        assertThatThrownBy(() -> QueryStatusResponse.withoutEndpoint(
                "q", "RUNNING", null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Live status");
    }

    @Test
    void liveResponsesRequireCanonicalStatesAndResultProtocols() {
        assertThatThrownBy(() -> new SubmitQueryResponse(
                "q", "finished", ResultDisposition.PENDING, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown query state");
        assertThatThrownBy(() -> QueryStatusResponse.of(
                "q", "finished", null, ResultDisposition.PENDING, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown query state");
        assertThatThrownBy(() -> QueryStatusResponse.historical("q", "finished"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown query state");
        assertThatThrownBy(() -> new ResultEndpoint("worker", 8080, "ticket", "flight"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown result protocol");
        assertThat(new ResultEndpoint("worker", 8080, "ticket", "grpc+tls").protocol())
                .isEqualTo("grpc+tls");
    }

    @Test
    void resultDisposition_isNotInferredFromLegacyEndpointOnlyJson() {
        assertThatThrownBy(() -> MAPPER.readValue(
                "{\"query_id\":\"q\",\"state\":\"RUNNING\","
                        + "\"result_endpoint\":{\"host\":\"worker\",\"port\":8080,"
                        + "\"ticket\":\"ticket\",\"protocol\":\"grpc\"}}",
                SubmitQueryResponse.class))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("resultDisposition");
        assertThatThrownBy(() -> MAPPER.readValue(
                "{\"query_id\":\"q\",\"state\":\"RUNNING\",\"version\":1}",
                QueryStatusResponse.class))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Historical status");
    }

    @Test
    void historicalStatusOmitsLiveResultAndDrainAuthority() throws Exception {
        QueryStatusResponse historical = QueryStatusResponse.historical("q", "FINISHED");
        JsonNode json = MAPPER.valueToTree(historical);
        assertThat(json.has("result_disposition")).isFalse();
        assertThat(json.has("result_endpoint")).isFalse();
        assertThat(json.has("version")).isFalse();
        assertThat(json.has("drained")).isFalse();
        assertThat(MAPPER.treeToValue(json, QueryStatusResponse.class)).isEqualTo(historical);
        assertThatThrownBy(() -> new QueryStatusResponse("q", "FAILED",
                new ErrorDetail("ERR", "TYPE", "message"), null, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only query_id and state");
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
