package org.pangrova.protocol.execution;

import java.util.List;

/**
 * Describes where a task's output should be routed.
 *
 * <p>Baked into the {@link TaskDescriptor} so the native engine knows its
 * output destination at creation time — no Java data threads needed for
 * intermediate stages.</p>
 *
 * <p>Result routing is implicit: when a plan contains a PartitionedOutputNode,
 * the native engine's OutputBufferManager handles result output and the native result
 * server serves it to the coordinator. No explicit result routing variant
 * is needed.</p>
 */
public sealed interface OutputRouting {

    /**
     * Shuffle output to downstream workers via native transport.
     *
     * @param endpoints      downstream worker transport endpoints
     * @param partitionCount number of shuffle partitions
     */
    record ShuffleOutput(List<NativeEndpoint> endpoints, int partitionCount)
            implements OutputRouting {
        public ShuffleOutput {
            endpoints = List.copyOf(endpoints);
        }
    }

    /**
     * Native endpoint for shuffle/exchange communication.
     *
     * @param host worker hostname
     * @param port transport server port
     */
    record NativeEndpoint(String host, int port) {
    }
}
