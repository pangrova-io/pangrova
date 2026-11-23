package org.pangrova.scheduler.admission;

import org.pangrova.testing.testkit.coordination.admission.InMemoryClusterConcurrencySlots;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.resource.AdmissionRequest.TenantLimits;
import org.pangrova.scheduler.api.enums.AdmissionVerdict;

import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Independent set-model oracle for real process-local admission/release operations. */
class TenantAdmissionOwnershipPropertyTest {

    @Property(tries = 300, seed = "20261003")
    void reservationsMatchExactLiveQuerySet(
            @ForAll @Size(min = 1, max = 100) List<@IntRange(min = -20, max = 20) Integer> events,
            @ForAll boolean shared) {
        var quota = new TenantLimits(1000, 3, 2);
        var controller = new TenantQuotaAdmissionController(null, shared ? new InMemoryClusterConcurrencySlots() : null);
        var admitted = new HashSet<String>();
        var queued = new HashSet<String>();
        var liveIdentities = new java.util.HashMap<Integer, String>();
        int generation = 0;
        for (int event : events) {
            int slot = Math.abs(event);
            String query = liveIdentities.getOrDefault(slot, "unknown-" + slot);
            if (event < 0) {
                controller.release(query, "wrong-tenant");
                assertThat(controller.admittedCount("t")).isEqualTo(admitted.size());
                controller.release(query, "t");
                admitted.remove(query);
                queued.remove(query);
                liveIdentities.remove(slot);
            } else {
                if (!liveIdentities.containsKey(slot)) {
                    query = "q" + slot + "-" + generation++;
                    liveIdentities.put(slot, query);
                }
                var expected = expectedVerdict(query, admitted, queued);
                var actual = controller.admit(AdmissionRequest.of("t", "default", query, quota));
                assertThat(actual.verdict()).isEqualTo(expected);
                if (expected == AdmissionVerdict.ADMIT) {
                    admitted.add(query);
                }
                if (expected == AdmissionVerdict.QUEUE) {
                    queued.add(query);
                }
            }
            assertThat(controller.admittedCount("t")).isEqualTo(admitted.size());
        }
        admitted.forEach(query -> controller.release(query, "t"));
        queued.forEach(query -> controller.release(query, "t"));
        assertThat(controller.admittedCount("t")).isZero();
    }

    private static AdmissionVerdict expectedVerdict(String query, java.util.Set<String> admitted,
                                                    java.util.Set<String> queued) {
        return admitted.contains(query) ? AdmissionVerdict.ADMIT
                : queued.contains(query) ? AdmissionVerdict.QUEUE
                : admitted.size() < 3 ? AdmissionVerdict.ADMIT
                : queued.size() < 2 ? AdmissionVerdict.QUEUE : AdmissionVerdict.REJECT;
    }
}
