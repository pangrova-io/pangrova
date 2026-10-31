package org.pangrova.coordinator.worker;

import org.pangrova.common.concurrent.ThreadSafe;
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
        Shared shared = resolve(worker.capabilityInventory());
        return shared.inventory() == worker.capabilityInventory()
                ? worker : worker.withCapabilityInventory(shared.inventory());
    }

    synchronized String encode(CapabilityInventory inventory) {
        return resolve(inventory).encoded();
    }

    private Shared resolve(CapabilityInventory inventory) {
        Payload existing = inventories.get(inventory);
        CapabilityInventory canonical = existing == null ? null : existing.inventory().get();
        if (canonical != null) {
            return new Shared(canonical, existing.encoded());
        }
        String encoded = CapabilityInventoryCodec.encode(inventory);
        // Replace an equal but collected key as well as its value; never create a strong referent cycle.
        inventories.remove(inventory);
        inventories.put(inventory, new Payload(new WeakReference<>(inventory), encoded));
        return new Shared(inventory, encoded);
    }

    private record Payload(WeakReference<CapabilityInventory> inventory, String encoded) {}

    private record Shared(CapabilityInventory inventory, String encoded) {}
}
