package org.pangrova.shuffle.nativewire;

import java.nio.file.Path;
import java.util.Map;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.shuffle.api.capability.ShuffleCapabilityDescriptor;
import org.pangrova.shuffle.api.worker.ShuffleChannelLimits;
import org.pangrova.shuffle.api.worker.ShuffleProducerSession;

/** Provider entry for Workers executing the canonical Native task protocol. */
public interface NativeShuffleWorkerFactory {
    int API_VERSION = 1;

    String providerId();
    int apiVersion();
    ShuffleCapabilityDescriptor capabilities();

    /** Opens a Worker-generation session. Both data directions must share the same admission budget. */
    Session open(Configuration configuration);

    /** Properties are scoped to the selected provider and validated before any channels are opened. */
    record Configuration(ShuffleChannelLimits limits, Tls tls, Map<String, String> properties) {
        public Configuration {
            java.util.Objects.requireNonNull(limits, "limits");
            properties = Map.copyOf(properties);
        }

        @Override public String toString() {
            return "Configuration[limits=" + limits + ", tls=" + (tls != null)
                    + ", configuredProperties=" + properties.keySet() + "]";
        }
    }

    /** Already-validated deployment paths. Null TLS config denotes explicitly plaintext transport. */
    record Tls(Path certificate, Path privateKey, Path rootCa) {
        public Tls {
            java.util.Objects.requireNonNull(certificate);
            java.util.Objects.requireNonNull(privateKey);
            java.util.Objects.requireNonNull(rootCa);
        }
    }

    interface Session {
        /** Guarantees actually enabled for this Worker generation. */
        ShuffleCapabilityDescriptor capabilities();
        /** Transfers a producer session to the caller, which closes it on executor drain. */
        ShuffleProducerSession<TaskRequest> producers();
        /** Transfers a reader pool to the caller, which closes it after all input pumps drain. */
        NativeShuffleReaders readers();
        /** Stops granting resources, including pool creations racing shutdown. */
        void seal();
        /** Throws if any actual construction, channel or failed close is still owned. */
        void requireDrained();
    }
}
