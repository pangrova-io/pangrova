package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import org.pangrova.shuffle.api.capability.ShuffleCapabilities;
import org.pangrova.shuffle.api.contract.ShuffleContext;
import org.pangrova.shuffle.api.capability.ShuffleDurability;
import org.pangrova.shuffle.api.contract.ShuffleManagerFactory;
import org.pangrova.shuffle.api.contract.ShuffleProvider;
import org.pangrova.shuffle.api.capability.ShuffleRetentionPolicy;
import org.pangrova.shuffle.core.exchange.SpoolBackedExchangeManager;
import org.pangrova.shuffle.api.spool.SpoolStore;
import org.pangrova.shuffle.api.spool.SpoolStoreContext;
import org.pangrova.shuffle.api.spool.SpoolStoreFactory;

import java.util.ServiceLoader;

/**
 * In-tree {@code object-store} shuffle factory (ADR-002 D4).
 *
 * <p>Committed partitions are spooled to S3/OSS/HDFS via a durable {@link SpoolStore}
 * backend. Durability is {@link ShuffleDurability#DURABLE}: data survives worker crash.
 * No external shuffle service required — the object store acts as the durability layer.
 *
 * <p>Write path: native engine -> D2-egress JNI -> DefaultShuffleByteEgress -> SpoolStore(durable).
 * Read path: ExchangeSource -> SpoolBackedExchangeManager -> SpoolReader -> downstream task.
 *
 * <p>Configuration properties:
 * <ul>
 *   <li>{@code shuffle.object-store.backend} — required durable SpoolStoreFactory name</li>
 * </ul>
 *
 * <p>A local filesystem does not survive node loss and must never be advertised
 * as durable. Missing or weaker backends fail provider construction.
 */
public final class ObjectStoreShuffleManagerFactory implements ShuffleManagerFactory {

    public static final String NAME = "object-store";
    private static final String PROP_BACKEND = "shuffle.object-store.backend";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ShuffleProvider create(ShuffleContext context) {
        String backend = context.getOrDefault(PROP_BACKEND, "");
        if (backend.isBlank()) {
            throw new IllegalArgumentException(
                    "object-store shuffle requires shuffle.object-store.backend");
        }
        SpoolStore spoolStore = resolveSpoolStore(backend, context);
        if (spoolStore == null) {
            throw new IllegalArgumentException(
                    "No SpoolStoreFactory registered for object-store backend: " + backend);
        }
        if (spoolStore.durability() != ShuffleDurability.DURABLE) {
            IllegalArgumentException failure = new IllegalArgumentException(
                    "object-store backend does not guarantee DURABLE shuffle: " + backend);
            try {
                spoolStore.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }

        SpoolBackedExchangeManager exchangeManager = new SpoolBackedExchangeManager(spoolStore);
        LocalSpillLifecycleManager lifecycleManager = new LocalSpillLifecycleManager(spoolStore);

        return new DefaultShuffleProvider(
                lifecycleManager,
                exchangeManager,
                ShuffleCapabilities.of(ShuffleDurability.DURABLE, ShuffleRetentionPolicy.PERSISTENT));
    }

    private static SpoolStore resolveSpoolStore(String backend, ShuffleContext context) {
        for (SpoolStoreFactory factory : ServiceLoader.load(SpoolStoreFactory.class)) {
            if (factory.name().equals(backend)) {
                return factory.create(new SpoolStoreContext(backend, context.properties()));
            }
        }
        return null;
    }
}
