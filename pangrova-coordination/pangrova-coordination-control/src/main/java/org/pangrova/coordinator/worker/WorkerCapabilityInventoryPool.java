package org.pangrova.coordinator.worker;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec;
import org.pangrova.protocol.v2.CapabilityInventory;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.WeakHashMap;
import javax.annotation.concurrent.GuardedBy;

/**
 * Shares exactly equal, already validated immutable inventories within one WorkerManager.
 * Worker identities, metrics and authorization remain separate. Weak keys and weak values
 * cannot keep a retired inventory alive; this pool is not capability or permission authority.
 */
@ThreadSafe
final class WorkerCapabilityInventoryPool {
    @GuardedBy("this")
    private final Map<CapabilityInventory, Payload> inventories = new WeakHashMap<>();

    synchronized WorkerInfo share(WorkerInfo worker) {
        Shared shared = resolve(worker.admittedInventory());
        return shared.inventory() == worker.admittedInventory()
                ? worker : worker.withCapabilityInventory(shared.inventory());
    }

    synchronized String encode(WorkerInfo.AdmittedInventory inventory) {
        return resolve(inventory).encoded();
    }

    private Shared resolve(WorkerInfo.AdmittedInventory inventory) {
        var message = inventory.inventory();
        Payload existing = inventories.get(message);
        WorkerInfo.AdmittedInventory canonical = existing == null ? null : existing.inventory().get();
        if (canonical != null) {
            return new Shared(canonical, existing.encoded());
        }
        String encoded = CapabilityInventoryCodec.encode(message);
        // Replace an equal but collected key as well as its value; never create a strong referent cycle.
        inventories.remove(message);
        inventories.put(message, new Payload(new WeakReference<>(inventory), encoded));
        return new Shared(inventory, encoded);
    }

    private record Payload(WeakReference<WorkerInfo.AdmittedInventory> inventory, String encoded) {}

    private record Shared(WorkerInfo.AdmittedInventory inventory, String encoded) {}
}
