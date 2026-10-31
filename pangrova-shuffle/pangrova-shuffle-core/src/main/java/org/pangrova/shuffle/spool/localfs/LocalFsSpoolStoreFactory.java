package org.pangrova.shuffle.spool.localfs;

import org.pangrova.shuffle.spool.SpoolStore;
import org.pangrova.shuffle.spool.SpoolStoreContext;
import org.pangrova.shuffle.spool.SpoolStoreFactory;

import java.nio.file.Path;

/**
 * {@link SpoolStoreFactory} for the local-filesystem backend (ADR-002 D6).
 *
 * <p>Registered via {@code META-INF/services} under name {@code "local-fs"}. Reads
 * {@code spool.local-fs.base-dir} from the context properties (default:
 * {@code ${java.io.tmpdir}/pangrova-spool}).
 */
public final class LocalFsSpoolStoreFactory implements SpoolStoreFactory {

    public static final String NAME = "local-fs";
    static final String BASE_DIR_KEY = "spool.local-fs.base-dir";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public SpoolStore create(SpoolStoreContext context) {
        String baseDirStr = context.getOrDefault(BASE_DIR_KEY,
                System.getProperty("java.io.tmpdir") + "/pangrova-spool");
        return new LocalFsSpoolStore(Path.of(baseDirStr));
    }
}
