package org.pangrova.spi.connector;

import org.pangrova.spi.annotation.Stable;

/**
 * Provides initialization context to a connector during creation.
 *
 * <p>This interface is passed to {@link ConnectorFactory#create} and may be
 * extended in the future to expose engine-level services to connectors.</p>
 */
@Stable(since = "1.0")
public interface ConnectorContext {
}
