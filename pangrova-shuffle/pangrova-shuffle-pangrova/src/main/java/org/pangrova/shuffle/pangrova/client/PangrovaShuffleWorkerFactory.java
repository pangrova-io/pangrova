package org.pangrova.shuffle.pangrova.client;

import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.shuffle.api.capability.ShuffleCapabilityDescriptor;
import org.pangrova.shuffle.api.enums.ShuffleCapability;
import org.pangrova.shuffle.api.worker.ShuffleProducerSession;
import org.pangrova.shuffle.nativewire.NativeShuffleReaders;
import org.pangrova.shuffle.nativewire.NativeShuffleWorkerFactory;

/** Creates fenced Pangrova egress and partition readers sharing one Worker-wide channel budget. */
public final class PangrovaShuffleWorkerFactory implements NativeShuffleWorkerFactory {
    private static final long DEFAULT_RPC_TIMEOUT_MILLIS = 30_000;
    private static final ShuffleCapabilityDescriptor CAPABILITIES =
            ShuffleCapabilityDescriptor.of(ShuffleCapability.BLOCKING_READ, ShuffleCapability.PARTITION_READ);

    @Override public String providerId() { return "pangrova"; }
    @Override public int apiVersion() { return API_VERSION; }
    @Override public ShuffleCapabilityDescriptor capabilities() {
        return CAPABILITIES;
    }

    @Override
    public Session open(Configuration configuration) {
        var properties = configuration.properties();
        for (String key : properties.keySet()) {
            if (!"rpc-timeout-ms".equals(key)) throw new IllegalArgumentException(
                    "Unknown Pangrova Worker shuffle property: " + key);
        }
        long timeout;
        try {
            timeout = Long.parseLong(properties.getOrDefault("rpc-timeout-ms", Long.toString(
                    DEFAULT_RPC_TIMEOUT_MILLIS)));
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("Invalid Pangrova Worker shuffle rpc-timeout-ms");
        }
        if (timeout <= 0) throw new IllegalArgumentException("Pangrova Worker shuffle rpc-timeout-ms must be positive");
        var tls = configuration.tls();
        var options = tls == null ? RemoteShuffleClientPool.PoolOptions.defaults()
                : RemoteShuffleClientPool.PoolOptions.mutualTls(new RemoteShuffleClientPool.PemTlsConfig(
                        tls.certificate(), tls.privateKey(), tls.rootCa()));
        return new WorkerSession(new RemoteShuffleChannelAdmission(configuration.limits()), options, timeout);
    }

    /** Admission outlives both pools so failed drains cannot free credits in a replacement Worker. */
    record WorkerSession(RemoteShuffleChannelAdmission admission, RemoteShuffleClientPool.PoolOptions options,
            long rpcTimeoutMillis)
            implements Session {
        @Override public ShuffleCapabilityDescriptor capabilities() { return CAPABILITIES; }

        private RemoteShuffleClientPool pool() {
            admission.requireOpen();
            return new RemoteShuffleClientPool(rpcTimeoutMillis, rpcTimeoutMillis, options, admission);
        }
        @Override public NativeShuffleReaders readers() { return pool(); }
        @Override public ShuffleProducerSession<TaskRequest> producers() { return new RemoteShuffleByteEgress(pool()); }
        @Override public void seal() { admission.seal(); }
        @Override public void requireDrained() { admission.requireDrained(); }
    }
}
