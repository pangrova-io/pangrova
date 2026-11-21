package org.pangrova.shuffle.api.coordinator;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.shuffle.api.ShuffleExecutionOwnerResolver;
import org.pangrova.shuffle.api.capability.ShuffleCapabilityDescriptor;

import java.util.Map;

/** Factory for a Coordinator process. Settings are scoped to the selected backend and carry no authority. */
@Experimental(reason = "External exchange provider discovery")
public interface ShuffleCoordinatorFactory {
    int API_VERSION = 1;

    String providerId();

    int apiVersion();

    /** Upper bound before applying deployment configuration and publication evidence. */
    ShuffleCapabilityDescriptor capabilities();

    /** Checks configuration without allocating clients, opening channels or starting threads. */
    void validate(Map<String, String> settings);

    /** Validates settings and opens one session. The caller closes it after draining queries and cleanup. */
    ShuffleCoordinatorSession open(Map<String, String> settings, ShuffleExecutionOwnerResolver owners);
}
