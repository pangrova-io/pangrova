package org.pangrova.planner.throttle;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueryThrottlingServiceTest {

    @Test
    void withinGlobalLimit_acquired() {
        var service = new QueryThrottlingService(2, 100.0, 100.0);

        ThrottleResult result = service.tryAcquire("tenant1", "user1");

        assertThat(result.acquired()).isTrue();
        assertThat(result.reason()).isNull();
        assertThat(result.permit()).isNotNull();
        assertThat(result.permit().tenant()).isEqualTo("tenant1");
        assertThat(result.permit().user()).isEqualTo("user1");
    }

    @Test
    void exceedGlobalLimit_rejectedWithThrottle0001() {
        var service = new QueryThrottlingService(1, 100.0, 100.0);

        ThrottleResult first = service.tryAcquire("t1", "u1");
        assertThat(first.acquired()).isTrue();

        ThrottleResult second = service.tryAcquire("t2", "u2");
        assertThat(second.acquired()).isFalse();
        assertThat(second.reason()).isEqualTo(
                PangrovaErrorCode.THROTTLE_GLOBAL_LIMIT.getDescription());
    }

    @Test
    void tenantRateLimitExceeded_rejectedWithThrottle0002() {
        // Rate of 0.001 permits/sec → first tryAcquire may succeed, second fails
        var service = new QueryThrottlingService(100, 0.001, 100.0);

        // Exhaust the initial stored permit
        service.tryAcquire("tenantA", "user1");

        ThrottleResult result = service.tryAcquire("tenantA", "user2");
        assertThat(result.acquired()).isFalse();
        assertThat(result.reason()).isEqualTo(
                PangrovaErrorCode.THROTTLE_TENANT_LIMIT.getDescription());
    }

    @Test
    void userRateLimitExceeded_rejectedWithThrottle0003() {
        var service = new QueryThrottlingService(100, 100.0, 0.001);

        // Exhaust the initial stored permit for userX
        service.tryAcquire("tenantA", "userX");

        // Use a different tenant so tenant limiter is fresh
        ThrottleResult result = service.tryAcquire("tenantB", "userX");
        assertThat(result.acquired()).isFalse();
        assertThat(result.reason()).isEqualTo(
                PangrovaErrorCode.THROTTLE_USER_LIMIT.getDescription());
    }

    @Test
    void onUserRejection_globalPermitReleased() {
        var service = new QueryThrottlingService(2, 100.0, 0.001);

        int before = service.availableGlobalPermits();

        // First call succeeds, consuming one global permit
        ThrottleResult first = service.tryAcquire("t1", "userZ");
        assertThat(first.acquired()).isTrue();

        // Second call for same user fails at user level
        ThrottleResult second = service.tryAcquire("t1", "userZ");
        assertThat(second.acquired()).isFalse();

        // Global permit should be released back (only first call's permit held)
        assertThat(service.availableGlobalPermits()).isEqualTo(before - 1);
    }
}
