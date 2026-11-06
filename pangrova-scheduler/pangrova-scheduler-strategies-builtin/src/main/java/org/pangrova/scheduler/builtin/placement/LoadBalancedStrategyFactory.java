package org.pangrova.scheduler.builtin.placement;

import org.pangrova.scheduler.spi.placement.WorkerSelectionStrategy;
import org.pangrova.scheduler.spi.placement.WorkerSelectionStrategyFactory;

/** Built-in, resource-free placement ranking; constructed explicitly by Server. */
public final class LoadBalancedStrategyFactory implements WorkerSelectionStrategyFactory {
    @Override
    public String providerId() { return "load-balanced"; }

    @Override
    public int apiVersion() { return API_VERSION; }

    @Override
    public WorkerSelectionStrategy create() { return new LoadBalancedStrategy(); }
}
