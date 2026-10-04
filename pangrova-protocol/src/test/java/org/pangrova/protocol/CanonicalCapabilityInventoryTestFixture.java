package org.pangrova.protocol;

import org.pangrova.protocol.v2.CapabilityInventory;

import java.io.IOException;
import java.util.Objects;

/** Unmodified complete inventory emitted by the actual Native catalogue exporter. */
public final class CanonicalCapabilityInventoryTestFixture {

    private CanonicalCapabilityInventoryTestFixture() {}

    public static CapabilityInventory inventory() {
        try (var input = Objects.requireNonNull(CanonicalCapabilityInventoryTestFixture.class
                .getResourceAsStream("/function-contracts/actual-native-catalogue.bin"),
                "Actual Native catalogue resource is required")) {
            return CapabilityInventory.parseFrom(input);
        } catch (IOException failure) {
            throw new IllegalStateException("Actual Native catalogue resource could not be read", failure);
        }
    }

    public static String encodedInventory() {
        return CapabilityInventoryCodec.encode(inventory());
    }
}
