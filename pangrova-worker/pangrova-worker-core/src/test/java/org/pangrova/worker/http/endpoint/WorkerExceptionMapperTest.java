package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.NotAllowedException;
import org.junit.jupiter.api.Test;
import org.pangrova.client.error.ErrorDetail;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerExceptionMapperTest {

    @Test
    void preservesMethodNotAllowedStatusAndAllowHeader() {
        var response = new WorkerExceptionMapper().toResponse(new NotAllowedException("POST"));

        assertThat(response.getStatus()).isEqualTo(405);
        assertThat(response.getHeaderString("Allow")).contains("POST");
        assertThat(response.getEntity()).isInstanceOf(ErrorDetail.class);
        assertThat(((ErrorDetail) response.getEntity()).errorCode()).isEqualTo("METHOD_NOT_ALLOWED");
    }
}
