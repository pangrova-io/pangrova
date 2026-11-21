package org.pangrova.connector.runtime.classloader;

import org.pangrova.connector.runtime.enums.SharedPackagePolicy;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Isolated classloader for connector plugins with parent-first SPI delegation.
 *
 * <p>Delegation strategy:
 * <ul>
 *   <li>SPI packages: always delegate to the SPI classloader (mandatory)</li>
 *   <li>Shared packages: try the SPI classloader first, fall back to the plugin</li>
 *   <li>All others: plugin-first, with only JDK platform classes as fallback</li>
 * </ul>
 *
 * <p>The shared-package set is supplied by the active {@link SharedPackagePolicy}
 * so operators can trade compatibility against version independence.
 */
public class PluginClassLoader extends URLClassLoader {

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

    /** Scope SDK service discovery during factory startup and catalog construction. */
    public <T> T withContext(Supplier<T> operation) {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(this);
        try {
            return operation.get();
        } finally {
            thread.setContextClassLoader(previous);
        }
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
            return ClassLoader.getPlatformClassLoader().loadClass(name);
        }
    }

    private boolean isSpiPackage(String name) {
        int nested = name.indexOf('$');
        String topLevel = nested >= 0 ? name.substring(0, nested) : name;
        return SharedPackagePolicy.SPI_TYPES.contains(topLevel) || belongsTo(name, SharedPackagePolicy.SPI_PACKAGES);
    }

    private boolean isSharedPackage(String name) {
        return belongsTo(name, sharedPackages);
    }

    private static boolean belongsTo(String className, Set<String> packages) {
        return packages.stream().anyMatch(prefix -> className.startsWith(prefix.endsWith(".") ? prefix : prefix + "."));
    }
}
