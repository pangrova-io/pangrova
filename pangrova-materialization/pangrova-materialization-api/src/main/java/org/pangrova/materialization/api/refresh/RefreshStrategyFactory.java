package org.pangrova.materialization.api.refresh;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.materialization.api.enums.RefreshKind;

/**
 * Coordinator extension point for a refresh algorithm. One factory advertises one persisted kind.
 * Each registry creates one thread-safe strategy for that kind and shares it across refresh attempts.
 * Discovery and construction are resource-free. Pins, transactions and executors belong to the kernel.
 */
@Experimental
public interface RefreshStrategyFactory {
    int API_VERSION = 1;

    /** Factory contract version implemented by this provider. */
    int apiVersion();

    /** Supplies the kind, including its stable strategy ID, without constructing a strategy. */
    RefreshKind kind();

    /** Creates a strategy whose kind and ID match the advertised kind. */
    RefreshStrategy create();
}
