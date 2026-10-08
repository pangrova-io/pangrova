package org.pangrova.coordinator.http.endpoint;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.security.tenant.quota.TenantQuotaExceededException;

import static org.assertj.core.api.Assertions.assertThat;

/** Keeps expected tenant admission rejections distinct from internal service failures. */
class CoordinatorExceptionMapperTest {
    private final CoordinatorExceptionMapper mapper = new CoordinatorExceptionMapper();

    @Test
    void webErrorsPreserveStatusHeadersAndStructuredClassification() {
        var statuses = java.util.Map.of(400, "BAD_REQUEST", 401, "UNAUTHENTICATED", 403, "ACCESS_DENIED",
                404, "NOT_FOUND", 409, "CONFLICT", 413, "PAYLOAD_TOO_LARGE", 429, "TOO_MANY_REQUESTS",
                503, "UNAVAILABLE");
        statuses.forEach((status, code) -> {
            try (var original = jakarta.ws.rs.core.Response.status(status).header("Retry-After", "7").build();
                 var response = mapper.toResponse(new jakarta.ws.rs.WebApplicationException("classified", original))) {
                assertThat(response.getStatus()).isEqualTo(status);
                assertThat(response.getHeaderString("Retry-After")).isEqualTo("7");
                assertThat(((ErrorDetail) response.getEntity()).errorCode()).isEqualTo(code);
            }
        });
    }

    @Test
    void exceptionCannotReturnSuccessfulHttpStatus() {
        try (var original = jakarta.ws.rs.core.Response.ok().build();
             var response = mapper.toResponse(new jakarta.ws.rs.WebApplicationException("failed", original))) {
            assertThat(response.getStatus()).isEqualTo(500);
            assertThat(((ErrorDetail) response.getEntity()).errorCode()).isEqualTo("INTERNAL_ERROR");
        }
    }

    @Test
    void quotaRejectionPreservesDomainCodeAndReturnsRateLimitStatus() {
        try (var response = mapper.toResponse(new TenantQuotaExceededException("concurrent query quota exceeded"))) {
            assertThat(response.getStatus()).isEqualTo(429);
            assertThat(response.getEntity()).isEqualTo(
                    new ErrorDetail("TENANT-0001", "USER_ERROR", "concurrent query quota exceeded"));
            assertThat(response.getHeaderString("Retry-After")).isNull();
        }
    }

    @Test
    void quotaSubtypesCannotBecomeInternalErrors() {
        var specializedQuota = new TenantQuotaExceededException("runtime quota exceeded") {};
        try (var response = mapper.toResponse(specializedQuota)) {
            assertThat(response.getStatus()).isEqualTo(429);
            assertThat(((ErrorDetail) response.getEntity()).errorCode()).isEqualTo("TENANT-0001");
        }
    }

    @Test
    void unexpectedFailuresRemainInternalErrors() {
        try (var response = mapper.toResponse(new IllegalStateException("storage unavailable"))) {
            assertThat(response.getStatus()).isEqualTo(500);
            assertThat(response.getEntity()).isEqualTo(
                    new ErrorDetail("INTERNAL_ERROR", "INTERNAL_ERROR", "storage unavailable"));
        }
    }

    @Test
    void internalServiceIdentityRejectionRemainsPermissionDenied() {
        try (var response = mapper.toResponse(new jakarta.ws.rs.ForbiddenException(
                "Authenticated internal service identity is required"))) {
            assertThat(response.getStatus()).isEqualTo(403);
            assertThat(response.getEntity()).isEqualTo(new ErrorDetail("ACCESS_DENIED", "USER_ERROR",
                    "Authenticated internal service identity is required"));
        }
    }

    @Test
    void unsupportedSqlPreservesDomainCodeWithoutReportingAServerFailure() {
        var failure = new PangrovaException(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE,
                "Column tag SQL is not connected to the durable policy authority");
        try (var response = mapper.toResponse(failure)) {
            assertThat(response.getStatus()).isEqualTo(400);
            assertThat(response.getEntity()).isEqualTo(new ErrorDetail("PARSER-0002", "USER_ERROR",
                    failure.getMessage()));
        }
    }

    @Test
    void structuredGovernanceStorageFailureRemainsAServerFailure() {
        var failure = new PangrovaException(PangrovaErrorCode.GOVERNANCE_QUALITY_CHECK_FAILED,
                "Quality store unavailable");
        try (var response = mapper.toResponse(failure)) {
            assertThat(response.getStatus()).isEqualTo(500);
            assertThat(((ErrorDetail) response.getEntity()).errorCode())
                    .isEqualTo(PangrovaErrorCode.GOVERNANCE_QUALITY_CHECK_FAILED.getCode());
        }
    }
}
