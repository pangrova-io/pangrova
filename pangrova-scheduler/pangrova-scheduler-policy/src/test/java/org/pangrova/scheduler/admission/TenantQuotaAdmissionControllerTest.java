package org.pangrova.scheduler.admission;

import org.pangrova.scheduler.api.resource.AdmissionDecision;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.resource.AdmissionRequest.TenantLimits;
import org.pangrova.scheduler.api.enums.AdmissionVerdict;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks tenant admission with real component state; this does not execute SQL or Native tasks.
 */
class TenantQuotaAdmissionControllerTest {

    private static final TenantLimits QUOTA = new TenantLimits(10L << 30, 3, 5);
    private final TenantQuotaAdmissionController controller = new TenantQuotaAdmissionController(null, null);

    @Test
    void admitsQueryWithinConcurrencyLimit() {
        AdmissionRequest req = AdmissionRequest.of("t1", "default", "q1", QUOTA);
        AdmissionDecision decision = controller.admit(req);
        assertThat(decision.isAdmitted()).isTrue();
        assertThat(controller.admittedCount("t1")).isEqualTo(1);
    }

    @Test
    void reservesAgainstTheLatestLimitsSuppliedForEachQuery() {
        assertThat(controller.admit(AdmissionRequest.of("t1", "default", "q1", QUOTA)).isAdmitted()).isTrue();

        var lowered = new TenantLimits(QUOTA.maxMemoryBytes(), 1, 0);
        assertThat(controller.admit(AdmissionRequest.of("t1", "default", "q2", lowered)).verdict())
                .isEqualTo(AdmissionVerdict.REJECT);
        assertThat(controller.admittedCount("t1")).isOne();

        controller.release("q1", "t1");
        assertThat(controller.admit(AdmissionRequest.of("t1", "default", "q2", lowered)).isAdmitted()).isTrue();
    }

    @Test
    void queuesWhenConcurrencyLimitReached() {
        // Admit 3 queries (maxConcurrentQueries = 3)
        controller.admit(AdmissionRequest.of("t1", "default", "q1", QUOTA));
        controller.admit(AdmissionRequest.of("t1", "default", "q2", QUOTA));
        controller.admit(AdmissionRequest.of("t1", "default", "q3", QUOTA));
        assertThat(controller.admittedCount("t1")).isEqualTo(3);

        // 4th query should be queued
        AdmissionDecision decision = controller.admit(AdmissionRequest.of("t1", "default", "q4", QUOTA));
        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.QUEUE);
    }

    @Test
    void releasedSlotAllowsNewAdmission() {
        controller.admit(AdmissionRequest.of("t1", "default", "q1", QUOTA));
        controller.admit(AdmissionRequest.of("t1", "default", "q2", QUOTA));
        controller.admit(AdmissionRequest.of("t1", "default", "q3", QUOTA));
        assertThat(controller.admittedCount("t1")).isEqualTo(3);

        controller.release("q1", "t1");
        assertThat(controller.admittedCount("t1")).isEqualTo(2);

        // Now admission should succeed again
        AdmissionDecision decision = controller.admit(AdmissionRequest.of("t1", "default", "q4", QUOTA));
        assertThat(decision.isAdmitted()).isTrue();
    }

    @Test
    void rejectsQueryExceedingMemoryEstimate() {
        long overLimit = QUOTA.maxMemoryBytes() + 1;
        AdmissionRequest req = new AdmissionRequest("t1", "default", "q1", QUOTA,
                OptionalLong.of(overLimit));
        AdmissionDecision decision = controller.admit(req);
        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(decision.reason()).contains("estimated memory");
    }

    @Test
    void admitsQueryWithMemoryEstimateWithinLimit() {
        AdmissionRequest req = new AdmissionRequest("t1", "default", "q1", QUOTA,
                OptionalLong.of(1024L * 1024 * 1024)); // 1 GB
        AdmissionDecision decision = controller.admit(req);
        assertThat(decision.isAdmitted()).isTrue();
    }

    @Test
    void admittedCountReturnsZeroForUnknownTenant() {
        assertThat(controller.admittedCount("nonexistent")).isEqualTo(0);
    }

    @Test
    void releaseIsIdempotentForUnknownQuery() {
        // Should not throw
        controller.release("unknown-query", "unknown-tenant");
    }
}
