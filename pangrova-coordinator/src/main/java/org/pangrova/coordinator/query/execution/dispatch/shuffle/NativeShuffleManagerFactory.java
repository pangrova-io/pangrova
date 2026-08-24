package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import org.pangrova.shuffle.ShuffleCapabilities;
import org.pangrova.shuffle.ShuffleContext;
import org.pangrova.shuffle.ShuffleDurability;
import org.pangrova.shuffle.ShuffleManagerFactory;
import org.pangrova.shuffle.ShuffleProvider;

/**
 * In-tree {@code native} shuffle factory (ADR-002 D1/D4): in-process data exchange.
 *
 * <p>The coordinator has no external shuffle lifecycle to manage for the native path — workers
 * shuffle in-process — so {@link ShuffleProvider#lifecycleManager()} is {@code null}.
 * Durability is {@link ShuffleDurability#EPHEMERAL}: a lost worker forces a full recompute, so this
 * provider does not back task-level retry.
 */
public final class NativeShuffleManagerFactory implements ShuffleManagerFactory {

    public static final String NAME = "native";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ShuffleProvider create(ShuffleContext context) {
        return new DefaultShuffleProvider(
                null, null, ShuffleCapabilities.of(ShuffleDurability.EPHEMERAL));
    }
}
