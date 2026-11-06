package org.pangrova.planner.core.throttle;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ThrottlePermitOwnershipTest {

    @Test
    void repeatedReleaseCannotIncreaseConfiguredCapacity() {
        var service = new QueryThrottlingService(1, 1000, 1000);
        var permit = service.tryAcquire("tenant", "user").permit();
        service.release(permit);
        service.release(permit);
        assertThat(service.availableGlobalPermits()).isOne();
    }

    @Test
    void onlyTheIssuedObjectCanReturnItsOwnersCapacity() {
        var service = new QueryThrottlingService(1, 1000, 1000);
        var other = new QueryThrottlingService(1, 1000, 1000);
        var permit = service.tryAcquire("tenant", "user").permit();
        var equalValue = new ThrottlePermit(permit.user(), permit.tenant(), permit.acquiredAt());
        assertThat(equalValue).isEqualTo(permit).isNotSameAs(permit);
        service.release(equalValue);
        service.release(null);
        other.release(permit);
        assertThat(service.availableGlobalPermits()).isZero();
        assertThat(other.availableGlobalPermits()).isOne();
        service.release(permit);
        assertThat(service.availableGlobalPermits()).isOne();
    }

    @Test
    void concurrentDuplicateReleaseReturnsOnlyOneSlot() throws Exception {
        var service = new QueryThrottlingService(1, 1000, 1000);
        var permit = service.tryAcquire("tenant", "user").permit();
        var executor = Executors.newFixedThreadPool(8);
        try {
            var returns = new ArrayList<java.util.concurrent.Future<?>>();
            for (int index = 0; index < 32; index++) {
                returns.add(executor.submit(() -> service.release(permit)));
            }
            for (var returned : returns) returned.get(5, TimeUnit.SECONDS);
            assertThat(service.availableGlobalPermits()).isOne();
            assertThat(service.tryAcquire("next", "next").acquired()).isTrue();
            assertThat(service.tryAcquire("overflow", "overflow").acquired()).isFalse();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void invalidIdentityDoesNotConsumeGlobalCapacity() {
        var service = new QueryThrottlingService(1, 1000, 1000);
        assertThatThrownBy(() -> service.tryAcquire(null, "user"))
                .isInstanceOf(NullPointerException.class);
        assertThat(service.availableGlobalPermits()).isOne();
        assertThatThrownBy(() -> service.tryAcquire("tenant", null))
                .isInstanceOf(NullPointerException.class);
        assertThat(service.availableGlobalPermits()).isOne();
        assertThat(service.tryAcquire("valid", "valid").acquired()).isTrue();
    }

    @Test
    void invalidRatesAreRejectedBeforeUpdatingEitherLayer() {
        var service = new QueryThrottlingService(1, 0.001, 0.001);
        var first = service.tryAcquire("tenant", "user").permit();
        service.release(first);
        for (double invalid : new double[] {0, -1, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThatThrownBy(() -> service.adjustRates(invalid, 1000))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> service.adjustRates(1000, invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(service.tryAcquire("tenant", "other-user").reason())
                .isEqualTo(org.pangrova.common.exception.PangrovaErrorCode.THROTTLE_TENANT_LIMIT.getDescription());
        assertThat(service.tryAcquire("other-tenant", "user").reason())
                .isEqualTo(org.pangrova.common.exception.PangrovaErrorCode.THROTTLE_USER_LIMIT.getDescription());
        assertThat(service.availableGlobalPermits()).isOne();
    }

    @Test
    void invalidConfigurationCannotCreateAnAdmissionService() {
        assertThatThrownBy(() -> new QueryThrottlingService(-1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        for (double invalid : new double[] {0, -1, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThatThrownBy(() -> new QueryThrottlingService(1, invalid, 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new QueryThrottlingService(1, 1, invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(new QueryThrottlingService(0, 1, 1).tryAcquire("tenant", "user").acquired()).isFalse();
    }
}
