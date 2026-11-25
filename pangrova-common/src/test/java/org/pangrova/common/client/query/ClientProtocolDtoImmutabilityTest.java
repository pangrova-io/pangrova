package org.pangrova.common.client.query;

import org.pangrova.common.client.result.ResultEndpoint;

import org.junit.jupiter.api.Test;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.enums.ResultDisposition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies public client DTOs perform defensive copying and are records.
 */
class ClientProtocolDtoImmutabilityTest {

    @Test
    void submitQueryRequestSessionPropertiesIsDefensivelyCopied() {
        Map<String, String> mutable = new HashMap<>(Map.of("k", "v"));
        SubmitQueryRequest req = new SubmitQueryRequest("SELECT 1", mutable);

        mutable.put("injected", "bad");

        assertThat(req.sessionProperties()).hasSize(1);
        assertThatThrownBy(() -> req.sessionProperties().put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void submitQueryRequestNullSessionPropertiesBecomesEmptyMap() {
        SubmitQueryRequest req = new SubmitQueryRequest("SELECT 1", null);
        assertThat(req.sessionProperties()).isEmpty();
    }

    @Test
    void submitQueryResponseColumnNamesIsDefensivelyCopied() {
        List<String> mutable = new ArrayList<>(List.of("col1", "col2"));
        SubmitQueryResponse resp = new SubmitQueryResponse(
                "q-1", "RUNNING", ResultDisposition.PENDING, null, mutable);

        mutable.add("injected");

        assertThat(resp.columnNames()).hasSize(2);
        assertThatThrownBy(() -> resp.columnNames().add("x"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void submitQueryResponseNullColumnNamesBecomesEmptyList() {
        SubmitQueryResponse resp = new SubmitQueryResponse(
                "q-1", "RUNNING", ResultDisposition.PENDING, null, null);
        assertThat(resp.columnNames()).isEmpty();
    }

    @Test
    void publicDtosAreRecords() {
        assertThat(ErrorDetail.class.isRecord()).isTrue();
        assertThat(SubmitQueryRequest.class.isRecord()).isTrue();
        assertThat(SubmitQueryResponse.class.isRecord()).isTrue();
        assertThat(QueryStatusResponse.class.isRecord()).isTrue();
        assertThat(QueryStats.class.isRecord()).isTrue();
        assertThat(QueryParameter.class.isRecord()).isTrue();
        assertThat(ResultEndpoint.class.isRecord()).isTrue();
    }
}
