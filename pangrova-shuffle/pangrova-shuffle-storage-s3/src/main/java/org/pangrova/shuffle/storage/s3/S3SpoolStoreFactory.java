package org.pangrova.shuffle.storage.s3;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.pangrova.shuffle.api.spool.SpoolStore;
import org.pangrova.shuffle.api.spool.SpoolStoreContext;
import org.pangrova.shuffle.api.spool.SpoolStoreFactory;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorageContext;
import org.pangrova.shuffle.core.spool.ObjectStoreSpoolStore;

/**
 * {@link SpoolStoreFactory} for the S3 backend (ADR-002 D6).
 *
 * <p>Registered via {@code META-INF/services} under name {@code "s3"}. Reads
 * {@code spool.s3.*} properties: {@code bucket} (required), {@code prefix} (default
 * {@code spool}), {@code region}, {@code endpoint}, {@code path-style-access},
 * {@code api-call-timeout-seconds}, {@code api-call-attempt-timeout-seconds},
 * {@code temp-dir} (default {@code ${java.io.tmpdir}/pangrova-s3-spool}).
 */
public final class S3SpoolStoreFactory implements SpoolStoreFactory {

    public static final String NAME = "s3";
    private static final String KEY_PREFIX = "spool.s3.";
    private static final String DEFAULT_TEMP_DIR =
            System.getProperty("java.io.tmpdir") + "/pangrova-s3-spool";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public SpoolStore create(SpoolStoreContext context) {
        Map<String, String> storageProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : context.properties().entrySet()) {
            if (entry.getKey().startsWith(KEY_PREFIX)) {
                storageProperties.put(entry.getKey().substring(KEY_PREFIX.length()),
                        entry.getValue());
            }
        }
        storageProperties.putIfAbsent("prefix", "spool");
        S3ShuffleObjectStorage objects = S3ShuffleObjectStorage.create(
                new ShuffleObjectStorageContext(NAME, storageProperties));
        String tempDir = context.getOrDefault("spool.s3.temp-dir", DEFAULT_TEMP_DIR);
        return new ObjectStoreSpoolStore(objects, Path.of(tempDir));
    }
}
