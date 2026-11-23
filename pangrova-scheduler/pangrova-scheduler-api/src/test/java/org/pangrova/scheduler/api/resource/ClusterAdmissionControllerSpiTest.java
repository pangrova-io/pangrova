package org.pangrova.scheduler.api.resource;

import org.pangrova.scheduler.api.enums.AdmissionVerdict;

import org.pangrova.scheduler.api.admission.ClusterAdmissionController;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the cluster admission control SPI contracts (Task 145, G-RES-04).
 */
class ClusterAdmissionControllerSpiTest {
    private static final AdmissionRequest.TenantLimits QUOTA = new AdmissionRequest.TenantLimits(2048, 3, 0);

    // --- AdmissionRequest ---

    @Test
    void admissionRequestStoresFields() {
        var req = new AdmissionRequest("t1", "group-a", "q-123", QUOTA, OptionalLong.of(1024));
        assertThat(req.tenantId()).isEqualTo("t1");
        assertThat(req.resourceGroupId()).isEqualTo("group-a");
        assertThat(req.queryId()).isEqualTo("q-123");
        assertThat(req.estimatedMemoryBytes()).hasValue(1024);
    }

    @Test
    void admissionRequestWithoutMemoryEstimate() {
        var req = AdmissionRequest.of("t1", "default", "q-456", QUOTA);
        assertThat(req.estimatedMemoryBytes()).isEmpty();
    }

    @Test
    void admissionRequestRejectsBlankTenantId() {
        assertThatThrownBy(() -> new AdmissionRequest("", "g", "q", QUOTA, OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tenantId");
    }

    @Test
    void admissionRequestRejectsNullQueryId() {
        assertThatThrownBy(() -> new AdmissionRequest("t", "g", null, QUOTA, OptionalLong.empty()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void admissionRequestRejectsBlankQueryId() {
        assertThatThrownBy(() -> new AdmissionRequest("t", "g", " ", QUOTA, OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queryId");
    }

    @Test
    void admissionRequestRejectsNegativeMemoryEstimate() {
        assertThatThrownBy(() -> new AdmissionRequest("tenant", "group", "query", QUOTA, OptionalLong.of(-1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("estimatedMemoryBytes");
    }

    @Test
    void admissionRequestAcceptsZeroAndMaximumRepresentableEstimate() {
        assertThat(new AdmissionRequest("tenant", "group", "query", QUOTA, OptionalLong.of(0))
                .estimatedMemoryBytes()).hasValue(0);
        assertThat(new AdmissionRequest("tenant", "group", "query", QUOTA, OptionalLong.of(Long.MAX_VALUE))
                .estimatedMemoryBytes()).hasValue(Long.MAX_VALUE);
    }

    // --- AdmissionDecision ---

    @Test
    void admitDecision() {
        AdmissionDecision d = AdmissionDecision.admit();
        assertThat(d.verdict()).isEqualTo(AdmissionVerdict.ADMIT);
        assertThat(d.isAdmitted()).isTrue();
    }

    @Test
    void queueDecision() {
        AdmissionDecision d = AdmissionDecision.queue("group at capacity");
        assertThat(d.verdict()).isEqualTo(AdmissionVerdict.QUEUE);
        assertThat(d.isAdmitted()).isFalse();
        assertThat(d.reason()).isEqualTo("group at capacity");
    }

    @Test
    void rejectDecision() {
        AdmissionDecision d = AdmissionDecision.reject("hard limit");
        assertThat(d.verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(d.isAdmitted()).isFalse();
    }

    @Test
    void decisionRejectsNullVerdict() {
        assertThatThrownBy(() -> new AdmissionDecision(null, "r"))
                .isInstanceOf(NullPointerException.class);
    }

    // --- ClusterAdmissionController interface ---

    @Test
    void controllerInterfaceCanBeImplemented() {
        var counters = new ConcurrentHashMap<String, AtomicInteger>();

        ClusterAdmissionController controller = new ClusterAdmissionController() {
            private static final int MAX_CONCURRENT = 3;

            @Override
            public AdmissionDecision admit(AdmissionRequest request) {
                AtomicInteger counter = counters.computeIfAbsent(
                        request.tenantId(), k -> new AtomicInteger(0));
                int current = counter.incrementAndGet();
                if (current > MAX_CONCURRENT) {
                    counter.decrementAndGet();
                    return AdmissionDecision.reject("concurrent limit reached: " + MAX_CONCURRENT);
                }
                return AdmissionDecision.admit();
            }

            @Override
            public void release(String queryId, String tenantId) {
                AtomicInteger counter = counters.get(tenantId);
                if (counter != null) {
                    counter.decrementAndGet();
                }
            }

            @Override
            public int admittedCount(String tenantId) {
                AtomicInteger counter = counters.get(tenantId);
                return counter == null ? 0 : counter.get();
            }
        };

        // admit 3 queries
        for (int i = 0; i < 3; i++) {
            var req = AdmissionRequest.of("t1", "default", "q-" + i, QUOTA);
            assertThat(controller.admit(req).isAdmitted()).isTrue();
        }

        // 4th query rejected
        var req4 = AdmissionRequest.of("t1", "default", "q-3", QUOTA);
        assertThat(controller.admit(req4).isAdmitted()).isFalse();
        assertThat(controller.admittedCount("t1")).isEqualTo(3);

        // release one, then 4th admitted
        controller.release("q-0", "t1");
        assertThat(controller.admit(req4).isAdmitted()).isTrue();
    }

    @Test
    void releaseForUnknownTenantIsNoOp() {
        ClusterAdmissionController controller = new ClusterAdmissionController() {
            @Override
            public AdmissionDecision admit(AdmissionRequest request) {
                return AdmissionDecision.admit();
            }

            @Override
            public void release(String queryId, String tenantId) {
                // no-op for unknown
            }

            @Override
            public int admittedCount(String tenantId) {
                return 0;
            }
        };

        // should not throw
        controller.release("unknown-query", "unknown-tenant");
        assertThat(controller.admittedCount("unknown-tenant")).isEqualTo(0);
    }
}
