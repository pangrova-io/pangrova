package org.pangrova.coordinator.transport;

import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

/** Immutable mutual-TLS material for Coordinator-to-shuffle control RPCs. */
public record ShuffleTlsConfig(
        Path certificateChain,
        Path privateKey,
        Path rootCa,
        String serverIdentity) {

    public ShuffleTlsConfig {
        requireReadable(certificateChain, "certificateChain");
        requireReadable(privateKey, "privateKey");
        requireReadable(rootCa, "rootCa");
        requireNonNull(serverIdentity, "serverIdentity must not be null");
        if (serverIdentity.isBlank()) {
            throw new IllegalArgumentException("serverIdentity must not be blank");
        }
    }

    private static void requireReadable(Path path, String name) {
        requireNonNull(path, name + " must not be null");
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException(name + " must be a readable file");
        }
    }
}
