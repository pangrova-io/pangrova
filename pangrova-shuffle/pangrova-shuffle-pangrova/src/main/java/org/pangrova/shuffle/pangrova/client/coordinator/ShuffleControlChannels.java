package org.pangrova.shuffle.pangrova.client.coordinator;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.grpc.ChannelCredentials;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleClientPool.PemTlsConfig;

/** Keeps Coordinator control-channel TLS policy consistent with advertised endpoints. */
final class ShuffleControlChannels {
    private static final long CHANNEL_SHUTDOWN_SECONDS = 5;

    private ShuffleControlChannels() {}

    static void closeChannels(Collection<ManagedChannel> channels) {
        channels.forEach(ManagedChannel::shutdown);
        try {
            await(channels);
            for (ManagedChannel channel : channels) {
                if (!channel.isTerminated()) channel.shutdownNow();
            }
            await(channels);
        } catch (InterruptedException interrupted) {
            channels.forEach(ManagedChannel::shutdownNow);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while draining shuffle control channels", interrupted);
        }
        if (channels.stream().anyMatch(channel -> !channel.isTerminated())) {
            throw new IllegalStateException("Shuffle control channel termination is incomplete");
        }
    }

    private static void await(Collection<ManagedChannel> channels) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(CHANNEL_SHUTDOWN_SECONDS);
        for (ManagedChannel channel : channels) {
            if (!channel.isTerminated()) {
                channel.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            }
        }
    }

    static void validateTransportSecurity(
            List<ShuffleServiceAddress> endpoints,
            PemTlsConfig tlsConfig) {
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

    static ChannelCredentials channelCredentials(PemTlsConfig tlsConfig) {
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
            ShuffleHandle handle, PemTlsConfig tlsConfig) {
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
