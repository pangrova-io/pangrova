package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.util.ServiceLoader;

import org.pangrova.shuffle.api.capability.ShuffleCapabilities;
import org.pangrova.shuffle.api.contract.ShuffleContext;
import org.pangrova.shuffle.api.capability.ShuffleDurability;
import org.pangrova.shuffle.api.contract.ShuffleManagerFactory;
import org.pangrova.shuffle.api.contract.ShuffleProvider;
import org.pangrova.shuffle.core.exchange.SpoolBackedExchangeManager;
import org.pangrova.shuffle.api.spool.SpoolStore;
import org.pangrova.shuffle.api.spool.SpoolStoreContext;
import org.pangrova.shuffle.api.spool.SpoolStoreFactory;

/**
 * In-tree {@code local-spill} shuffle factory (ADR-002 D4).
 *
 * <p>Committed partitions are spooled to the producing worker's local disk via the
 * {@code "local-fs"} {@link SpoolStoreFactory} backend (D6). Durability is
 * {@link ShuffleDurability#LOCAL_SPILL}: data survives task failure but not worker loss.
 * This is the in-tree FTE default.
 *
 * <p>Write path: native engine -> D2-egress JNI -> DefaultShuffleByteEgress -> SpoolStore.
 * Read path: ExchangeSource -> SpoolBackedExchangeManager -> SpoolReader -> downstream task.
 *
 * <p>Configuration properties:
 * <ul>
 *   <li>{@code spool.local-fs.base-dir} — base directory for spool files
 *       (default: java.io.tmpdir/pangrova-spool)</li>
 * </ul>
 */
public final class LocalSpillShuffleManagerFactory implements ShuffleManagerFactory {

    public static final String NAME = "local-spill";
    private static final String SPOOL_BACKEND = "local-fs";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ShuffleProvider create(ShuffleContext context) {
        SpoolStore spoolStore = resolveSpoolStore(context);
        if (spoolStore == null) {
            throw new IllegalArgumentException(
                    "No SpoolStoreFactory registered for local-spill backend: " + SPOOL_BACKEND);
        }
        SpoolBackedExchangeManager exchangeManager = new SpoolBackedExchangeManager(spoolStore);

        // Local-spill has no remote lifecycle — lifecycle is local to the worker.
        // The coordinator's lifecycle manager is null (no external RSS to manage).
        LocalSpillLifecycleManager lifecycleManager = new LocalSpillLifecycleManager(spoolStore);

        return new DefaultShuffleProvider(
                lifecycleManager,
                exchangeManager,
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL));
    }

    private static SpoolStore resolveSpoolStore(ShuffleContext context) {
        for (SpoolStoreFactory factory : ServiceLoader.load(SpoolStoreFactory.class)) {
            if (factory.name().equals(SPOOL_BACKEND)) {
                return factory.create(new SpoolStoreContext(SPOOL_BACKEND, context.properties()));
            }
        }
        return null;
    }
}
