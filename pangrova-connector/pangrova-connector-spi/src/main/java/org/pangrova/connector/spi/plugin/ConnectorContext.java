package org.pangrova.connector.spi.plugin;

import org.pangrova.common.api.annotation.Stable;
import org.pangrova.connector.spi.metadata.NativeDatasetControlPlane;

import java.util.Optional;

/**
 * Provides initialization context to a connector during creation.
 *
 * <p>Workers do not expose coordinator-owned metadata control services. Their
 * connector instances may decode immutable handles but may not publish datasets.</p>
 */
@Stable(since = "1.0")
public interface ConnectorContext {
    /** Metadata-only Native control port, present only in the catalog owner process. */
    Optional<NativeDatasetControlPlane> nativeDatasetControlPlane();

    /** Sole durable source authority, available only in the catalog owner process. */
    default Optional<org.pangrova.connector.spi.metadata.ManagedSourceCatalog> managedSourceCatalog() {
        return Optional.empty();
    }
}
