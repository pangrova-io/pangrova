package org.pangrova.scheduler.admission;

import org.junit.jupiter.api.Test;
import org.pangrova.scheduler.api.resource.AdmissionDecision;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.resource.AdmissionRequest.TenantLimits;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Direct lifecycle tests for the admission owner, not SQL/Native execution evidence. */
class QueryAdmissionOwnershipTest {
    private static final TenantLimits QUOTA = new TenantLimits(1000, 2);

    @Test
    void failedAdmissionRetainsItsEnteredReservationForCleanup() {
        var ownership = new QueryAdmissionOwnership();
        var returned = new AtomicInteger();
        var failure = new IllegalStateException("entered reservation failed before publication");
        assertThatThrownBy(() -> ownership.admit(request("query"), reservation -> {
            reservation.retain(returned::incrementAndGet);
            throw failure;
        })).isSameAs(failure);

        ownership.release("query", "wrong-tenant");
        assertThat(returned.get()).isZero();
        ownership.release("query", "tenant");
        ownership.release("query", "tenant");
        assertThat(returned.get()).isEqualTo(1);
    }

    @Test
    void failedReleaseRetainsTheSameOwnerForRetry() {
        var ownership = new QueryAdmissionOwnership();
        var attempts = new AtomicInteger();
        var failure = new IllegalStateException("backend release unavailable");
        ownership.admit(request("query"), reservation -> {
            reservation.retain(() -> {
                if (attempts.incrementAndGet() == 1) {
                    throw failure;
                }
            });
            return AdmissionDecision.admit();
        });

        assertThatThrownBy(() -> ownership.release("query", "tenant")).isSameAs(failure);
        ownership.release("query", "tenant");
        ownership.release("query", "tenant");
        assertThat(attempts.get()).isEqualTo(2);
    }

    @Test
    void changedRequestCannotReplaceExistingOwner() {
        var ownership = new QueryAdmissionOwnership();
        var returned = new AtomicInteger();
        ownership.admit(request("query"), reservation -> {
            reservation.retain(returned::incrementAndGet);
            return AdmissionDecision.admit();
        });
        var changed = AdmissionRequest.of("different-tenant", "default", "query", QUOTA);
        assertThat(ownership.admit(changed, reservation -> {
            throw new AssertionError("conflicting admission action must not enter");
        }).isAdmitted()).isFalse();
        ownership.release("query", "different-tenant");
        assertThat(returned.get()).isZero();
        ownership.release("query", "tenant");
        assertThat(returned.get()).isEqualTo(1);
    }

    @Test
    void refusalWithoutReservationCanRetryWithTheSameIdentity() {
        var ownership = new QueryAdmissionOwnership();
        var returned = new AtomicInteger();
        var decision = ownership.admit(request("query"), reservation -> AdmissionDecision.reject("memory limit"));
        assertThat(decision.isAdmitted()).isFalse();
        assertThat(ownership.release("query", "tenant")).isFalse();

        var accepted = ownership.admit(request("query"), reservation -> {
            reservation.retain(returned::incrementAndGet);
            return AdmissionDecision.admit();
        });
        assertThat(accepted.isAdmitted()).isTrue();
        assertThat(ownership.release("query", "tenant")).isTrue();
        assertThat(returned.get()).isEqualTo(1);
    }

    private static AdmissionRequest request(String queryId) {
        return AdmissionRequest.of("tenant", "default", queryId, QUOTA);
    }
}
