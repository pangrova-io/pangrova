package org.pangrova.testing.e2e;

import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.CapabilityInventoryCodec;
import org.pangrova.protocol.v2.CapabilityInventory;

/** Canonical capability inventory for E2E workers that advertise no optional capability. */
public final class CanonicalWorkerInventory {

    private CanonicalWorkerInventory() {}

    public static CapabilityInventory empty() {
        return CapabilityInventory.newBuilder()
                .setProtocol(CanonicalTaskRequests.protocolVersion())
                .setResultEnvelopeVersion(1)
                .setNativeEngineVersion("e2e-native")
                .build();
    }

    public static String encodedEmpty() {
        return CapabilityInventoryCodec.encode(empty());
    }
}
