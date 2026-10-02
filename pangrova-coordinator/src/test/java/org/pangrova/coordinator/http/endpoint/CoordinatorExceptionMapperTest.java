package org.pangrova.coordinator.http.endpoint;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.security.tenant.quota.TenantQuotaExceededException;

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
    void unexpectedFailuresRemainInternalErrors() {
        try (var response = mapper.toResponse(new IllegalStateException("storage unavailable"))) {
            assertThat(response.getStatus()).isEqualTo(500);
            assertThat(response.getEntity()).isEqualTo(
                    new ErrorDetail("INTERNAL_ERROR", "INTERNAL_ERROR", "storage unavailable"));
        }
    }
}
