package org.pangrova.testing.e2e;

import org.pangrova.protocol.CapabilityInventoryCodec;
import org.pangrova.protocol.v2.CapabilityInventory;

import java.io.IOException;
import java.util.Objects;

/** Complete Native-exported inventory for registry tests without an executing Worker. */
public final class CanonicalWorkerInventory {

    private CanonicalWorkerInventory() {}

    public static CapabilityInventory actual() {
        try (var input = Objects.requireNonNull(CanonicalWorkerInventory.class
                .getResourceAsStream("/function-contracts/actual-native-catalogue.bin"),
                "Actual Native catalogue resource is required")) {
            return CapabilityInventory.parseFrom(input);
        } catch (IOException failure) {
            throw new IllegalStateException("Actual Native catalogue resource could not be read", failure);
        }
    }

    public static String encodedActual() {
        return CapabilityInventoryCodec.encode(actual());
    }
}
