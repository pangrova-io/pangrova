package org.pangrova.testing.testkit.function;

import org.pangrova.testing.testkit.TestProjectPaths;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;

/** Reads the sole actual exporter artifact for checkout-local contract tests. */
public final class ActualNativeCatalogue {
    private ActualNativeCatalogue() {}

    /** Returns all original bytes; unavailable artifacts fail rather than inventing an empty catalogue. */
    public static byte[] bytes() {
        var artifact = TestProjectPaths.findProjectRoot().resolve(
                "pangrova-protocol/src/test/resources/function-contracts/actual-native-catalogue.bin");
        if (!Files.isRegularFile(artifact)) {
            throw new IllegalStateException("Actual Native catalogue exporter artifact is required: " + artifact);
        }
        try {
            byte[] bytes = Files.readAllBytes(artifact);
            if (bytes.length == 0) {
                throw new IllegalStateException("Actual Native catalogue exporter artifact is empty");
            }
            return bytes;
        } catch (IOException failure) {
            throw new UncheckedIOException("Actual Native catalogue exporter artifact could not be read", failure);
        }
    }
}
