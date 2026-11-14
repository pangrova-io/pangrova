package org.pangrova.shuffle.api.coordinator;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.api.descriptor.ShuffleProviderDescriptor;

import java.util.List;

/** Provider projection of a live endpoint-based task binding for comparison with locked allocation evidence. */
@Experimental(reason = "Durable external exchange allocation evidence")
@FunctionalInterface
public interface ExternalShuffleDescriptorEncoder {
    /** Encodes only credential-free route identity from the current external task transport. */
    ShuffleProviderDescriptor describe(String allocationId, ShuffleServiceAddress commitAddress,
            List<ShuffleServerEndpoint> endpoints, int partitionCount);
}
