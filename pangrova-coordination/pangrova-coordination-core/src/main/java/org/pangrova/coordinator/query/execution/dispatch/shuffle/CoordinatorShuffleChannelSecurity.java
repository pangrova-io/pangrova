package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.io.IOException;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import io.grpc.ChannelCredentials;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import org.pangrova.coordinator.transport.ShuffleTlsConfig;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShuffleServiceAddress;

/** Keeps Coordinator control-channel TLS policy consistent with advertised endpoints. */
final class CoordinatorShuffleChannelSecurity {
    private static final long CHANNEL_SHUTDOWN_SECONDS = 5;

    private CoordinatorShuffleChannelSecurity() {}

    static void closeChannels(Collection<ManagedChannel> channels) {
        for (ManagedChannel channel : channels) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(CHANNEL_SHUTDOWN_SECONDS, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }

    static void validateTransportSecurity(
            List<ShuffleServiceAddress> endpoints,
            ShuffleTlsConfig tlsConfig) {
        boolean anyTls = endpoints.stream()
                .anyMatch(endpoint -> !endpoint.tlsIdentity().isBlank());
        boolean allTls = endpoints.stream()
                .allMatch(endpoint -> !endpoint.tlsIdentity().isBlank());
        if (anyTls != allTls) {
            throw new IllegalArgumentException(
                    "shuffle endpoints cannot mix TLS and plaintext");
        }
        if (allTls != (tlsConfig != null)) {
            throw new IllegalArgumentException(
                    "shuffle endpoint TLS policy requires matching trust and client material");
        }
    }

    static ChannelCredentials channelCredentials(ShuffleTlsConfig tlsConfig) {
        if (tlsConfig == null) {
            return InsecureChannelCredentials.create();
        }
        try {
            return TlsChannelCredentials.newBuilder()
                    .trustManager(tlsConfig.rootCa().toFile())
                    .keyManager(
                            tlsConfig.certificateChain().toFile(),
                            tlsConfig.privateKey().toFile())
                    .build();
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "failed to load Coordinator shuffle TLS material", failure);
        }
    }

    static void validateHandleTransportSecurity(
            ShuffleHandle handle, ShuffleTlsConfig tlsConfig) {
        boolean tlsRequired = tlsConfig != null;
        boolean commitTls = !handle.commitAddress().tlsIdentity().isBlank();
        boolean endpointsMatchPolicy = handle.endpoints().stream()
                .allMatch(endpoint -> !endpoint.tlsIdentity().isBlank() == tlsRequired);
        if (commitTls != tlsRequired || !endpointsMatchPolicy) {
            throw new IllegalStateException(
                    "shuffle service returned endpoints outside Coordinator TLS policy");
        }
    }
}
