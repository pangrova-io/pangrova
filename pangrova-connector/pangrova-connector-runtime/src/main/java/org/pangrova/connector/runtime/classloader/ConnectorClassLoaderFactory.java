package org.pangrova.connector.runtime.classloader;

import org.pangrova.connector.runtime.enums.SharedPackagePolicy;

import java.net.URL;

/**
 * Factory for creating isolated classloaders for connector plugins.
 *
 * <p>Implementations control the class-visibility boundary between the engine
 * and connector code. The default implementation honours
 * {@link SharedPackagePolicy} and its explicit set of shared packages.
 *
 * <p>Custom implementations can add advanced isolation strategies such as
 * per-connector bytecode transformation or OSGi-style module wiring.
 *
 * @see SharedPackagePolicy
 */
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
    PluginClassLoader createClassLoader(String pluginName, URL[] pluginUrls, ClassLoader spiClassLoader);
}
