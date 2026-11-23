package org.pangrova.scheduler.admission;

import org.pangrova.testing.testkit.coordination.admission.InMemoryClusterConcurrencySlots;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.resource.AdmissionRequest.TenantLimits;
import org.pangrova.scheduler.api.enums.AdmissionVerdict;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises real admission ownership; it does not establish distributed SQL execution. */
class TenantAdmissionOwnershipTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unknownReleaseCannotReturnAnotherQueriesSlot(boolean shared) {
        var controller = controller(shared);
        admit(controller, "first");
        admit(controller, "second");

        controller.release("never-admitted", "tenant");

        assertThat(controller.admittedCount("tenant")).isEqualTo(2);
        assertThat(controller.admit(request("third")).verdict()).isEqualTo(AdmissionVerdict.REJECT);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void duplicateReleaseCannotReturnAnotherQueriesSlot(boolean shared) {
        var controller = controller(shared);
        admit(controller, "first");
        admit(controller, "second");

        controller.release("first", "tenant");
        controller.release("first", "tenant");

        assertThat(controller.admittedCount("tenant")).isEqualTo(1);
        admit(controller, "third");
        assertThat(controller.admittedCount("tenant")).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void replayedAdmissionReservesExactlyOnce(boolean shared) {
        var controller = controller(shared);
        admit(controller, "first");

        admit(controller, "first");

        assertThat(controller.admittedCount("tenant")).isEqualTo(1);
        admit(controller, "second");
        assertThat(controller.admittedCount("tenant")).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void wrongTenantReleaseCannotConsumeTheAdmittedOwner(boolean shared) {
        var controller = controller(shared);
        admit(controller, "first");
        admit(controller, "second");
        controller.release("first", "another-tenant");

        assertThat(controller.admittedCount("tenant")).isEqualTo(2);
        assertThat(controller.admit(request("third")).verdict()).isEqualTo(AdmissionVerdict.REJECT);
        controller.release("first", "tenant");
        admit(controller, "third");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void repeatedRefusalsLeaveTheQueryEligibleForLaterAdmission(boolean shared) {
        var controller = controller(shared);
        admit(controller, "first");
        admit(controller, "second");

        assertThat(controller.admit(request("third")).verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(controller.admit(request("third")).verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(controller.admittedCount("tenant")).isEqualTo(2);
        controller.release("first", "tenant");
        admit(controller, "third");
        assertThat(controller.admittedCount("tenant")).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void concurrentDuplicateAdmissionAndReleaseHaveOneOwner(boolean shared) throws Exception {
        var controller = controller(shared);
        concurrent(24, index -> assertThat(controller.admit(request("same-query")).isAdmitted()).isTrue());
        assertThat(controller.admittedCount("tenant")).isEqualTo(1);
        admit(controller, "another-query");

        concurrent(24, index -> controller.release("same-query", "tenant"));

        assertThat(controller.admittedCount("tenant")).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void concurrentAdmissionsAtCapacityNeverAcquireAdditionalSlots(boolean shared) throws Exception {
        var controller = controller(shared);
        admit(controller, "first");
        admit(controller, "second");

        concurrent(24, index -> assertThat(controller.admit(request("query-" + index)).verdict())
                .isEqualTo(AdmissionVerdict.REJECT));
        assertThat(controller.admittedCount("tenant")).isEqualTo(2);
        controller.release("first", "tenant");
        admit(controller, "query-0");
        assertThat(controller.admittedCount("tenant")).isEqualTo(2);
    }

    private static void concurrent(int tasks, java.util.function.IntConsumer action) throws Exception {
        var pool = Executors.newFixedThreadPool(tasks);
        var ready = new CountDownLatch(tasks);
        var start = new CountDownLatch(1);
        var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
        try {
            for (int index = 0; index < tasks; index++) {
                int task = index;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    if (!start.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("concurrent admission start barrier expired");
                    }
                    action.accept(task);
                    return null;
                }));
            }
            assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            for (var future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            start.countDown();
            pool.shutdownNow();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static AdmissionRequest request(String queryId) {
        return AdmissionRequest.of("tenant", "default", queryId, new TenantLimits(1000, 2));
    }

    private static void admit(TenantQuotaAdmissionController controller, String queryId) {
        assertThat(controller.admit(request(queryId)).isAdmitted()).isTrue();
    }

    private static TenantQuotaAdmissionController controller(boolean shared) {
        return new TenantQuotaAdmissionController(null, shared ? new InMemoryClusterConcurrencySlots() : null);
    }
}
