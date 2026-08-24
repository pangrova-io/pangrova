package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.nio.file.Path;

import org.pangrova.shuffle.ShuffleCapabilities;
import org.pangrova.shuffle.ShuffleContext;
import org.pangrova.shuffle.ShuffleDurability;
import org.pangrova.shuffle.ShuffleManagerFactory;
import org.pangrova.shuffle.ShuffleProvider;
import org.pangrova.shuffle.ShuffleRetentionPolicy;
import org.pangrova.shuffle.exchange.SpoolBackedExchangeManager;
import org.pangrova.shuffle.spool.LocalFsSpoolStore;
import org.pangrova.shuffle.spool.SpoolStore;
import org.pangrova.shuffle.spool.SpoolStoreContext;
import org.pangrova.shuffle.spool.SpoolStoreFactory;

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
 *   <li>{@code shuffle.object-store.backend} — SpoolStoreFactory name to use (default: "local-fs" for dev)</li>
 *   <li>{@code shuffle.object-store.dir} — base path for spool data (bucket path or local dir)</li>
 * </ul>
 *
 * <p><strong>Note:</strong> For production S3/HDFS, a corresponding {@code SpoolStoreFactory}
 * implementation must be on the classpath. For development and testing, falls back to
 * {@code LocalFsSpoolStore} with DURABLE semantics (simulating object-store durability on local disk).
 */
public final class ObjectStoreShuffleManagerFactory implements ShuffleManagerFactory {

    public static final String NAME = "object-store";
    private static final String PROP_BACKEND = "shuffle.object-store.backend";
    private static final String PROP_DIR = "shuffle.object-store.dir";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ShuffleProvider create(ShuffleContext context) {
        String backend = context.getOrDefault(PROP_BACKEND, "local-fs");
        String baseDir = context.getOrDefault(PROP_DIR,
                System.getProperty("java.io.tmpdir") + "/pangrova-shuffle-durable");

        // Resolve SpoolStore via ServiceLoader or fall back to LocalFsSpoolStore.
        SpoolStore spoolStore = resolveSpoolStore(backend, context);
        if (spoolStore == null) {
            // Fallback: use local-fs with durable semantics for dev/test.
            spoolStore = new LocalFsSpoolStore(Path.of(baseDir));
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
        return null; // No matching factory found — caller uses fallback.
    }
}
