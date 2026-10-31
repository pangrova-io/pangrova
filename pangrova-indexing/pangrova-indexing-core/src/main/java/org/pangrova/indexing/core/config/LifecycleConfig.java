package org.pangrova.indexing.core.config;

/**
 * Groups lifecycle management configuration: GC, consistency, and commit listener.
 *
 * @param gc             garbage collection configuration
 * @param consistency    consistency checking configuration
 * @param commitListener commit listener configuration
 */
public record LifecycleConfig(
        GcConfig gc,
        ConsistencyConfig consistency,
        CommitListenerConfig commitListener
) {

    public static final LifecycleConfig DEFAULT =
            new LifecycleConfig(GcConfig.DEFAULT, ConsistencyConfig.DEFAULT, CommitListenerConfig.DEFAULT);

    public LifecycleConfig {
        if (gc == null) gc = GcConfig.DEFAULT;
        if (consistency == null) consistency = ConsistencyConfig.DEFAULT;
        if (commitListener == null) commitListener = CommitListenerConfig.DEFAULT;
    }
}
