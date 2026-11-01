package org.pangrova.shuffle.storage.local;

import java.nio.file.Path;

import org.pangrova.shuffle.api.storage.ShuffleObjectStorage;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorageContext;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorageFactory;

/**
 * {@code ServiceLoader} factory for the local-filesystem accepted-batch storage backend.
 *
 * <p>Configuration properties:
 * <ul>
 *   <li>{@code dir} — base directory (default: {@code java.io.tmpdir}/pangrova-shuffle-objects)</li>
 *   <li>{@code prefix} — object key prefix (default: {@code shuffle})</li>
 * </ul>
 */
public final class LocalFileShuffleObjectStorageFactory implements ShuffleObjectStorageFactory {

    public static final String NAME = "local";
    private static final String PROP_DIR = "dir";
    private static final String PROP_PREFIX = "prefix";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ShuffleObjectStorage create(ShuffleObjectStorageContext context) {
        String dir = context.getOrDefault(PROP_DIR,
                System.getProperty("java.io.tmpdir") + "/pangrova-shuffle-objects");
        String prefix = context.getOrDefault(PROP_PREFIX, "shuffle");
        return new LocalFileShuffleObjectStorage(Path.of(dir), prefix);
    }
}
