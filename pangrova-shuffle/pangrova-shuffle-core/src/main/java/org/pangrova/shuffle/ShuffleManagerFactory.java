package org.pangrova.shuffle;

import org.pangrova.spi.annotation.Experimental;

/**
 * {@code ServiceLoader} plugin point for shuffle implementations (ADR-002 D1).
 *
 * <p>Implementations are discovered at runtime and selected by a stable name string
 * ({@code "native"}, {@code "local-spill"}, {@code "object-store"}, ...),
 * aligning shuffle with the connector and index-engine plugin points. The bootstrap resolves a
 * factory by name rather than branching on an enum.
 */
@Experimental(reason = "ADR-002 D1 pluggable shuffle SPI; evolving alongside D4 providers")
public interface ShuffleManagerFactory {

    /** Stable id used in configuration, e.g. {@code "native"}, {@code "local-spill"}, or a plugin id. */
    String name();

    /** Builds this implementation's lifecycle + data-plane provider. */
    ShuffleProvider create(ShuffleContext context);
}
