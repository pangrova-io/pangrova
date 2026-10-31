package org.pangrova.connector.runtime.classloader;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Objects;
import java.util.Set;

/**
 * Isolated classloader for connector plugins with parent-first SPI delegation.
 *
 * <p>Delegation strategy:
 * <ul>
 *   <li>SPI packages: always delegate to the SPI classloader (mandatory)</li>
 *   <li>Shared packages: try the SPI classloader first, fall back to the plugin</li>
 *   <li>All others: plugin-first, fall back to the SPI classloader</li>
 * </ul>
 *
 * <p>The shared-package set is supplied by the active {@link SharedPackagePolicy}
 * so operators can trade compatibility against version independence.
 */
public class PluginClassLoader extends URLClassLoader {

    private static final Set<String> SPI_PACKAGES = SharedPackagePolicy.SPI_PACKAGES;

    private final String pluginName;
    private final ClassLoader spiClassLoader;
    private final Set<String> sharedPackages;

    /**
     * Creates a plugin class loader using the broad shared package policy.
     *
     * @param pluginName name of the plugin owning this loader
     * @param urls plugin classpath URLs
     * @param spiClassLoader class loader exposing the connector SPI
     */
    public PluginClassLoader(String pluginName, URL[] urls, ClassLoader spiClassLoader) {
        this(pluginName, urls, spiClassLoader,
                SharedPackagePolicy.BROAD.resolveSharedPackages(null));
    }

    /**
     * Creates a plugin class loader restricted to an explicit shared package set.
     *
     * @param pluginName name of the plugin owning this loader
     * @param urls plugin classpath URLs
     * @param spiClassLoader class loader exposing the connector SPI
     * @param sharedPackages packages delegated to the SPI class loader
     */
    public PluginClassLoader(String pluginName, URL[] urls, ClassLoader spiClassLoader,
                             Set<String> sharedPackages) {
        super(urls, null);
        this.pluginName = pluginName;
        this.spiClassLoader = Objects.requireNonNull(spiClassLoader, "spiClassLoader");
        this.sharedPackages = Set.copyOf(Objects.requireNonNull(sharedPackages, "sharedPackages"));
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loaded = findLoadedClass(name);
            if (loaded != null) return loaded;
            if (isSpiPackage(name)) return spiClassLoader.loadClass(name);
            if (isSharedPackage(name)) return loadSharedClass(name);
            return loadPluginFirst(name, resolve);
        }
    }

    /**
     * Returns the name of the plugin owning this class loader.
     *
     * @return plugin directory name
     */
    public String getPluginName() {
        return pluginName;
    }

    private Class<?> loadSharedClass(String name) throws ClassNotFoundException {
        try {
            return spiClassLoader.loadClass(name);
        } catch (ClassNotFoundException e) {
            return findClass(name);
        }
    }

    private Class<?> loadPluginFirst(String name, boolean resolve) throws ClassNotFoundException {
        try {
            Class<?> c = findClass(name);
            if (resolve) resolveClass(c);
            return c;
        } catch (ClassNotFoundException e) {
            return spiClassLoader.loadClass(name);
        }
    }

    private boolean isSpiPackage(String name) {
        return SPI_PACKAGES.stream().anyMatch(name::startsWith);
    }

    private boolean isSharedPackage(String name) {
        return sharedPackages.stream().anyMatch(name::startsWith);
    }
}
