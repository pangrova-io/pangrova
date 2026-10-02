package org.pangrova.protocol;

import org.pangrova.protocol.v2.CapabilityInventory;

/** Test fixture for an empty but fully identified canonical v2 inventory. */
public final class CanonicalCapabilityInventoryTestFixture {

    private CanonicalCapabilityInventoryTestFixture() {}

    public static CapabilityInventory inventory() {
        return CapabilityInventory.newBuilder()
                .setProtocol(CanonicalTaskRequests.protocolVersion())
                .setResultEnvelopeVersion(1)
                .setNativeEngineVersion("test-native")
                .build();
    }

    public static String encodedInventory() {
        return CapabilityInventoryCodec.encode(inventory());
    }
}
