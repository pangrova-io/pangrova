package org.pangrova.spi.connector;

import org.pangrova.spi.annotation.Stable;
import org.pangrova.spi.connector.metadata.NativeDatasetControlPlane;

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
}
