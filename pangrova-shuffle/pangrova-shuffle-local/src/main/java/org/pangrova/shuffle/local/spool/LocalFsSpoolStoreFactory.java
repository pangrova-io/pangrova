package org.pangrova.shuffle.local.spool;

import org.pangrova.shuffle.api.spool.SpoolStore;
import org.pangrova.shuffle.api.spool.SpoolStoreContext;
import org.pangrova.shuffle.api.spool.SpoolStoreFactory;

import java.nio.file.Path;

/**
 * {@link SpoolStoreFactory} for the local-filesystem backend (ADR-002 D6).
 *
 * <p>Registered via {@code META-INF/services} under name {@code "local-fs"}. Reads
 * {@code spool.local-fs.base-dir} from the context properties (default:
 * {@code ${java.io.tmpdir}/pangrova-spool}).
 * {@code spool.local-fs.max-open-files} bounds all selected read segments and
 * writers in one store (default 1024), excluding its directory lock.
 */
public final class LocalFsSpoolStoreFactory implements SpoolStoreFactory {

    public static final String NAME = "local-fs";
    static final String MAX_OPEN_FILES_KEY = "spool.local-fs.max-open-files";
    static final String BASE_DIR_KEY = "spool.local-fs.base-dir";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public SpoolStore create(SpoolStoreContext context) {
        java.util.Objects.requireNonNull(context, "context");
        if (!NAME.equals(context.storeName())) {
            throw new IllegalArgumentException("Local spool factory cannot create another backend");
        }
        String baseDirStr = context.getOrDefault(BASE_DIR_KEY,
                System.getProperty("java.io.tmpdir") + "/pangrova-spool");
        if (baseDirStr.isBlank()) throw new IllegalArgumentException("Local spool base directory must not be blank");
        int maxOpenFiles = Integer.parseInt(context.getOrDefault(MAX_OPEN_FILES_KEY,
                Integer.toString(LocalFsSpoolStore.DEFAULT_MAX_OPEN_FILES)));
        return new LocalFsSpoolStore(Path.of(baseDirStr), maxOpenFiles);
    }
}
