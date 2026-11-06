package org.pangrova.scheduler.spi.placement;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Creates a ranking strategy for one Coordinator scheduler. Factories are immutable
 * and thread-safe; each create call returns an independent instance. Only local,
 * resource-free ranking is supported by API version 1. There is no hot replacement.
 * Server validates the version and identity before construction and owns assembly.
 * No external configuration or SDK types are part of this contract.
 */
@Experimental
public interface WorkerSelectionStrategyFactory {
    int API_VERSION = 1;

    String providerId();

    int apiVersion();

    WorkerSelectionStrategy create();
}
