package org.pangrova.coordinator.scheduler.placement;

import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.scheduler.placement.WorkerCapabilityMatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

/** Compares placement work on already admitted Workers before applying cache or scheduler preferences. */
public final class WorkerPlacementWork {
    private WorkerPlacementWork() {}

    /** Keeps only the least loaded Workers that support the exact task bindings. */
    public static List<WorkerInfo> leastLoaded(List<WorkerInfo> eligible, List<OperatorBinding> bindings,
            long work, Map<WorkerExecutionIdentity, Double> windowWork, ToIntFunction<WorkerInfo> localLoad) {
        var leastLoaded = new ArrayList<WorkerInfo>();
        double minimum = Double.POSITIVE_INFINITY;
        for (var worker : eligible) {
            if (!bindings.isEmpty() && !WorkerCapabilityMatcher.supports(worker.capabilityInventory(), bindings)) {
                continue;
            }
            int running = Math.max(worker.runningTaskCount(), localLoad.applyAsInt(worker));
            double projected = running * (double) work
                    + windowWork.getOrDefault(worker.workerExecutionIdentity(), 0.0);
            if (projected < minimum) {
                leastLoaded.clear();
                minimum = projected;
            }
            if (projected == minimum) {
                leastLoaded.add(worker);
            }
        }
        return leastLoaded;
    }
}
