package org.pangrova.spi.connector.classloader;

import org.pangrova.spi.annotation.Experimental;

import java.net.URL;

/**
 * Factory for creating isolated classloaders for connector plugins.
 *
 * <p>Implementations control the class-visibility boundary between the engine
 * and connector code. The engine provides a default implementation that
 * honours the {@link SharedPackagePolicy} and the
 * {@code connector.classloader.shared-packages} configuration property.
 *
 * <p>Custom implementations can override this SPI to add advanced isolation
 * strategies such as per-connector bytecode transformation, security-manager
 * sandboxing, or OSGi-style module wiring.
 *
 * @see SharedPackagePolicy
 */
@Experimental(reason = "G-SPI-04: classloader isolation SPI is evolving")
public interface ConnectorClassLoaderFactory {

    /**
     * Creates an isolated classloader for a connector plugin.
     *
     * @param pluginName     the human-readable name of the plugin (for diagnostics)
     * @param pluginUrls     the URLs of the plugin JARs
     * @param spiClassLoader the parent classloader that provides the SPI contract
     *                       classes; shared-package classes are also resolved via
     *                       this loader
     * @return a new classloader whose delegation strategy is governed by the
     *         factory's configured {@link SharedPackagePolicy}
     */
    ClassLoader createClassLoader(String pluginName, URL[] pluginUrls, ClassLoader spiClassLoader);
}
