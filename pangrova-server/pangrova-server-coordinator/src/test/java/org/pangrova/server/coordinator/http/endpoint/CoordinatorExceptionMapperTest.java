package org.pangrova.server.coordinator.http.endpoint;

import org.junit.jupiter.api.Test;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.exception.TenantQuotaExceededException;

import static org.assertj.core.api.Assertions.assertThat;

/** Keeps expected tenant admission rejections distinct from internal service failures. */
class CoordinatorExceptionMapperTest {
    private final CoordinatorExceptionMapper mapper = new CoordinatorExceptionMapper();

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
    void deferredPermissionSubtypeReturnsForbiddenAtHttpBoundary() {
        try (var response = mapper.toResponse(new AuthorizationDeniedException(
                "Current deferred permission was revoked"))) {
            assertThat(response.getStatus()).isEqualTo(403);
            assertThat(response.getEntity()).isEqualTo(new ErrorDetail("SECURITY-0002", "USER_ERROR",
                    "Current deferred permission was revoked"));
        }
    }

    @Test
    void parseAndUnsupportedSqlAreBadRequestsWithTheirDomainCode() {
        for (var code : java.util.List.of(org.pangrova.common.enums.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                org.pangrova.common.enums.PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE)) {
            var failure = new org.pangrova.common.exception.PangrovaException(code, "SQL rejected");
            try (var response = mapper.toResponse(failure)) {
                assertThat(response.getStatus()).isEqualTo(400);
                assertThat(response.getEntity()).isEqualTo(
                        new ErrorDetail(code.getCode(), "USER_ERROR", "SQL rejected"));
            }
        }
    }

    @Test
    void coordinatorAdmissionUnavailabilityUsesServiceUnavailable() {
        var rejected = new org.pangrova.query.api.exception.QueryRejectedException("admission temporarily unavailable");
        try (var response = mapper.toResponse(rejected)) {
            assertThat(response.getStatus()).isEqualTo(503);
            assertThat(((ErrorDetail) response.getEntity()).errorCode()).isEqualTo("COORDINATOR-0002");
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
    void jaxRsMethodRejectionRetainsAllowHeaderAndTypedClientError() {
        try (var response = mapper.toResponse(new jakarta.ws.rs.NotAllowedException("POST"))) {
            assertThat(response.getStatus()).isEqualTo(405);
            assertThat(response.getAllowedMethods()).containsExactly("POST");
            var detail = (ErrorDetail) response.getEntity();
            assertThat(detail.errorCode()).isEqualTo("METHOD_NOT_ALLOWED");
            assertThat(detail.errorType()).isEqualTo("USER_ERROR");
        }
    }

    @Test
    void jaxRsMediaAndNotFoundRejectionsRetainActualStatus() {
        for (var rejection : java.util.List.of(new jakarta.ws.rs.NotSupportedException(),
                new jakarta.ws.rs.NotAcceptableException(), new jakarta.ws.rs.NotFoundException())) {
            try (var response = mapper.toResponse(rejection)) {
                assertThat(response.getStatus()).isEqualTo(rejection.getResponse().getStatus());
                assertThat(((ErrorDetail) response.getEntity()).errorType()).isEqualTo("USER_ERROR");
            }
        }
    }

    @Test
    void jaxRsResponseProtocolHeadersSurviveJsonEntityReplacement() {
        try (var original = jakarta.ws.rs.core.Response.status(429).header("Retry-After", "7")
                .header("Content-Length", "9000").header("Content-Encoding", "gzip").build();
                var response = mapper.toResponse(new jakarta.ws.rs.WebApplicationException(original))) {
            assertThat(response.getStatus()).isEqualTo(429);
            assertThat(response.getHeaderString("Retry-After")).isEqualTo("7");
            assertThat(response.getHeaderString("Content-Length")).isNull();
            assertThat(response.getHeaderString("Content-Encoding")).isNull();
            assertThat(((ErrorDetail) response.getEntity()).errorCode()).isEqualTo("RATE_LIMITED");
        }
        try (var response = mapper.toResponse(new jakarta.ws.rs.NotAuthorizedException("Bearer"))) {
            assertThat(response.getStatus()).isEqualTo(401);
            assertThat(response.getHeaderString("WWW-Authenticate")).isEqualTo("Bearer");
            assertThat(((ErrorDetail) response.getEntity()).errorCode()).isEqualTo("AUTHENTICATION_REQUIRED");
        }
    }

}
