package org.pangrova.coordinator.query.resourcegroup;

import org.pangrova.scheduler.api.resource.AdmissionDecision;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.resource.AdmissionVerdict;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ResourceGroupAdmissionController}.
 */
class ResourceGroupAdmissionControllerTest {

    @Test
    void admitsWithinConcurrencyLimit() {
        var controller = new ResourceGroupAdmissionController(
                Map.of("analytics", ResourceGroupConfig.of("analytics", 2, 5)));

        AdmissionDecision d1 = controller.admit(req("q1", "analytics"));
        AdmissionDecision d2 = controller.admit(req("q2", "analytics"));

        assertThat(d1.verdict()).isEqualTo(AdmissionVerdict.ADMIT);
        assertThat(d2.verdict()).isEqualTo(AdmissionVerdict.ADMIT);
        assertThat(controller.runningCount("analytics")).isEqualTo(2);
    }

    @Test
    void queuesWhenAtConcurrencyLimit() {
        var controller = new ResourceGroupAdmissionController(
                Map.of("etl", ResourceGroupConfig.of("etl", 1, 3)));

        controller.admit(req("q1", "etl"));
        AdmissionDecision d2 = controller.admit(req("q2", "etl"));

        assertThat(d2.verdict()).isEqualTo(AdmissionVerdict.QUEUE);
        assertThat(controller.queuedCount("etl")).isEqualTo(1);
    }

    @Test
    void rejectsWhenQueueFull() {
        var controller = new ResourceGroupAdmissionController(
                Map.of("small", ResourceGroupConfig.of("small", 1, 1)));

        controller.admit(req("q1", "small")); // running
        controller.admit(req("q2", "small")); // queued
        AdmissionDecision d3 = controller.admit(req("q3", "small"));

        assertThat(d3.verdict()).isEqualTo(AdmissionVerdict.REJECT);
    }

    @Test
    void releaseFreesCapacity() {
        var controller = new ResourceGroupAdmissionController(
                Map.of("grp", ResourceGroupConfig.of("grp", 1, 0)));

        controller.admit(req("q1", "grp"));
        assertThat(controller.runningCount("grp")).isEqualTo(1);

        controller.release("q1", "tenant-a");
        assertThat(controller.runningCount("grp")).isZero();

        AdmissionDecision d2 = controller.admit(req("q2", "grp"));
        assertThat(d2.verdict()).isEqualTo(AdmissionVerdict.ADMIT);
    }

    @Test
    void releaseIsIdempotent() {
        var controller = new ResourceGroupAdmissionController(1, 0);
        controller.release("unknown-query", "t1");
        // should not throw
    }

    @Test
    void admittedCountTracksByTenant() {
        var controller = new ResourceGroupAdmissionController(10, 100);

        controller.admit(AdmissionRequest.of("tenant-a", "default", "q1"));
        controller.admit(AdmissionRequest.of("tenant-a", "default", "q2"));
        controller.admit(AdmissionRequest.of("tenant-b", "default", "q3"));

        assertThat(controller.admittedCount("tenant-a")).isEqualTo(2);
        assertThat(controller.admittedCount("tenant-b")).isEqualTo(1);
    }

    @Test
    void promoteFromQueueDecreasesQueueIncreasesRunning() {
        var controller = new ResourceGroupAdmissionController(
                Map.of("grp", ResourceGroupConfig.of("grp", 1, 5)));

        controller.admit(req("q1", "grp")); // running
        controller.admit(req("q2", "grp")); // queued

        assertThat(controller.queuedCount("grp")).isEqualTo(1);
        assertThat(controller.runningCount("grp")).isEqualTo(1);

        controller.promoteFromQueue("q2");
        assertThat(controller.queuedCount("grp")).isZero();
        assertThat(controller.runningCount("grp")).isEqualTo(2);
    }

    @Test
    void memoryAwareAdmission() {
        var controller = new ResourceGroupAdmissionController(
                Map.of("mem", new ResourceGroupConfig("mem", 10, 5, 1_000_000)));

        AdmissionDecision d1 = controller.admit(
                new AdmissionRequest("t1", "mem", "q1", OptionalLong.of(600_000)));
        assertThat(d1.verdict()).isEqualTo(AdmissionVerdict.ADMIT);

        // 600K + 500K > 1M limit -> queued
        AdmissionDecision d2 = controller.admit(
                new AdmissionRequest("t1", "mem", "q2", OptionalLong.of(500_000)));
        assertThat(d2.verdict()).isEqualTo(AdmissionVerdict.QUEUE);
    }

    @Test
    void unknownGroupUsesDefault() {
        var controller = new ResourceGroupAdmissionController(5, 10);

        AdmissionDecision d = controller.admit(req("q1", "unknown-group"));
        assertThat(d.verdict()).isEqualTo(AdmissionVerdict.ADMIT);
    }

    private static AdmissionRequest req(String queryId, String groupId) {
        return AdmissionRequest.of("tenant-a", groupId, queryId);
    }
}
