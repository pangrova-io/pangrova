package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Internal;

/**
 * Marker interface for a data split.
 *
 * <p>A split represents a unit of data that can be processed by a single
 * task. Connector implementations provide their own split types that carry
 * the information needed to locate and read a portion of a table's data.</p>
 */
@Internal
public interface Split {
}
