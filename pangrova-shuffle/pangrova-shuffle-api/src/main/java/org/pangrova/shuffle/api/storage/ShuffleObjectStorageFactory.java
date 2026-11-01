package org.pangrova.shuffle.api.storage;

import org.pangrova.spi.annotation.Experimental;

/**
 * {@code ServiceLoader} plugin point for accepted-batch shuffle object storage backends
 * (ADR-002 D4/D6).
 *
 * <p>Implementations register via {@code META-INF/services} and are selected by a stable name
 * string ({@code "local"}, {@code "s3"}, {@code "oss"}, ...), aligning the accepted-batch storage
 * plane with {@link org.pangrova.shuffle.api.spool.SpoolStoreFactory} and
 * {@link org.pangrova.shuffle.api.contract.ShuffleManagerFactory}. Cloud SDKs are confined to the
 * backend modules — this API must never leak them transitively.
 */
@Experimental(reason = "ADR-002 D4/D6 accepted-batch storage SPI; evolving alongside storage backends")
public interface ShuffleObjectStorageFactory {

    /** Stable id used in configuration, e.g. {@code "local"} / {@code "s3"} / {@code "oss"}. */
    String name();

    /** Builds the object storage backend from its resolved {@code object-storage.*} configuration. */
    ShuffleObjectStorage create(ShuffleObjectStorageContext context);
}
