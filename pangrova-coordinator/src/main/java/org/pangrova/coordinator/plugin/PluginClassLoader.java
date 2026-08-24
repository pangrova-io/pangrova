package org.pangrova.coordinator.plugin;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Set;

/**
 * Isolated classloader for connector plugins with parent-first SPI delegation.
 *
 * <p>Delegation strategy:
 * <ul>
 *   <li>SPI packages: always delegate to SPI classloader (mandatory)</li>
 *   <li>Shared packages: try SPI classloader first, fallback to plugin</li>
 *   <li>All others: plugin-first, fallback to SPI classloader</li>
 * </ul>
 *
 * <p>This class was migrated from pangrova-spi to pangrova-coordinator because SPI modules
 * should only contain contract interfaces and value objects, not implementation logic.
 */
public class PluginClassLoader extends URLClassLoader {

    private static final Set<String> SPI_PACKAGES = Set.of(
            "org.pangrova.spi", "org.pangrova.common", "org.pangrova.protocol");

    private static final Set<String> SHARED_PACKAGES = Set.of(
            "org.apache.arrow", "io.substrait", "org.slf4j",
            "com.google.common", "org.apache.hadoop", "org.apache.parquet",
            "org.apache.orc", "com.fasterxml.jackson");

    private final String pluginName;
    private final ClassLoader spiClassLoader;

    public PluginClassLoader(String pluginName, URL[] urls, ClassLoader spiClassLoader) {
        super(urls, null);
        this.pluginName = pluginName;
        this.spiClassLoader = spiClassLoader;
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
        return SHARED_PACKAGES.stream().anyMatch(name::startsWith);
    }
}
