package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.nio.file.Path;

import org.pangrova.shuffle.ShuffleCapabilities;
import org.pangrova.shuffle.ShuffleContext;
import org.pangrova.shuffle.ShuffleDurability;
import org.pangrova.shuffle.ShuffleManagerFactory;
import org.pangrova.shuffle.ShuffleProvider;
import org.pangrova.shuffle.exchange.SpoolBackedExchangeManager;
import org.pangrova.shuffle.spool.LocalFsSpoolStore;
import org.pangrova.shuffle.spool.SpoolStore;

/**
 * In-tree {@code local-spill} shuffle factory (ADR-002 D4).
 *
 * <p>Committed partitions are spooled to the producing worker's local disk via
 * {@code LocalFsSpoolStore} (D6). Durability is {@link ShuffleDurability#LOCAL_SPILL}:
 * data survives task failure but not worker loss. This is the in-tree FTE default.
 *
 * <p>Write path: native engine -> D2-egress JNI -> DefaultShuffleByteEgress -> LocalFsSpoolStore.
 * Read path: ExchangeSource -> SpoolBackedExchangeManager -> SpoolReader -> downstream task.
 *
 * <p>Configuration properties:
 * <ul>
 *   <li>{@code shuffle.local-spill.dir} — base directory for spool files
 *       (default: java.io.tmpdir/pangrova-shuffle)</li>
 * </ul>
 */
public final class LocalSpillShuffleManagerFactory implements ShuffleManagerFactory {

    public static final String NAME = "local-spill";
    private static final String PROP_SPOOL_DIR = "shuffle.local-spill.dir";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ShuffleProvider create(ShuffleContext context) {
        String baseDirStr = context.getOrDefault(PROP_SPOOL_DIR,
                System.getProperty("java.io.tmpdir") + "/pangrova-shuffle");
        Path baseDir = Path.of(baseDirStr);
        SpoolStore spoolStore = new LocalFsSpoolStore(baseDir);
        SpoolBackedExchangeManager exchangeManager = new SpoolBackedExchangeManager(spoolStore);

        // Local-spill has no remote lifecycle — lifecycle is local to the worker.
        // The coordinator's lifecycle manager is null (no external RSS to manage).
        LocalSpillLifecycleManager lifecycleManager = new LocalSpillLifecycleManager(spoolStore);

        return new DefaultShuffleProvider(
                lifecycleManager,
                exchangeManager,
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL));
    }
}
