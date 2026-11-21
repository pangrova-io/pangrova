package org.pangrova.planner.property.throttle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;
import org.pangrova.planner.throttle.QueryThrottlingService;
import org.pangrova.planner.throttle.ThrottlePermit;

import static org.assertj.core.api.Assertions.assertThat;

class ThrottlePermitOwnershipPropertyTest {

    @Property(tries = 300, seed = "20261003")
    void onlyOutstandingIssuedObjectsChangeCapacity(
            @ForAll @Size(min = 1, max = 100) List<@IntRange(min = -10, max = 10) Integer> operations) {
        var service = new QueryThrottlingService(2, 1_000_000, 1_000_000);
        var history = new ArrayList<ThrottlePermit>();
        var outstanding = Collections.newSetFromMap(new IdentityHashMap<ThrottlePermit, Boolean>());
        int identity = 0;
        for (int operation : operations) {
            if (operation >= 0) {
                var result = service.tryAcquire("tenant-" + identity, "user-" + identity++);
                assertThat(result.acquired()).isEqualTo(outstanding.size() < 2);
                if (result.acquired()) {
                    history.add(result.permit());
                    outstanding.add(result.permit());
                }
            } else if (!history.isEmpty()) {
                var permit = history.get(Math.floorMod(operation, history.size()));
                service.release(new ThrottlePermit(permit.user(), permit.tenant(), permit.acquiredAt()));
                assertThat(service.availableGlobalPermits()).isEqualTo(2 - outstanding.size());
                service.release(permit);
                outstanding.remove(permit);
            } else {
                service.release(null);
            }
            assertThat(service.availableGlobalPermits()).isEqualTo(2 - outstanding.size());
        }
        outstanding.forEach(service::release);
        history.forEach(service::release);
        assertThat(service.availableGlobalPermits()).isEqualTo(2);
    }
}
