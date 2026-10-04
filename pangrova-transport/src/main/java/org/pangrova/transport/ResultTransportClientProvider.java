package org.pangrova.transport;

import org.apache.arrow.flight.grpc.NettyClientBuilder;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.pangrova.transport.security.ResultTransportSecurity;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

/** Builds the sole result transport client while its PEM input streams are owned. */
final class ResultTransportClientProvider {
    private ResultTransportClientProvider() {}

    static ResultTransportClientOwner open(BufferAllocator allocator, Location location, ResultTransportSecurity security,
            Consumer<ResultTransportClientOwner> registerOwner) {
        try (InputStream roots = openMaterial(security.rootCaPath());
             InputStream certificate = openMaterial(security.certificateChainPath());
             InputStream key = openMaterial(security.privateKeyPath())) {
            var builder = new NettyClientBuilder(allocator, location);
            if (roots != null) builder.trustedCertificates(roots);
            if (certificate != null) builder.clientCertificate(certificate, key);
            var owner = new ResultTransportClientOwner(builder.build().build());
            registerOwner.accept(owner);
            owner.initialize(allocator);
            return owner;
        } catch (IOException | RuntimeException failure) {
            throw new TransportConnectionException("TRANSPORT_TLS_CONFIGURATION_INVALID",
                    "Cannot initialize the configured result transport security", failure);
        }
    }

    private static InputStream openMaterial(Path path) throws IOException {
        return path == null ? null : Files.newInputStream(path);
    }

}
