package org.pangrova.connector.runtime.lifecycle;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.runtime.classloader.ConnectorClassLoaderFactory;
import org.pangrova.connector.runtime.classloader.DefaultConnectorClassLoaderFactory;
import org.pangrova.connector.runtime.discovery.ConnectorFactoryRegistry;
import org.pangrova.connector.api.plugin.ConnectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages connector plugin lifecycle: discovery, classloading, and factory registration.
 *
 * <p>Each immediate child directory of the plugin directory is one plugin; its
 * JARs are loaded through an isolated {@code PluginClassLoader} and its
 * {@link ConnectorFactory} providers are registered by connector name.
 *
 * <p>Concurrency strategy: ConcurrentHashMap for classloader and factory
 * registries. Plugin load/unload operations are expected to be called from the
 * bootstrap thread.
 */
@ThreadSafe
public class PluginManager {

    private static final Logger LOG = LoggerFactory.getLogger(PluginManager.class);

    private final Path pluginDir;
    private final ClassLoader spiClassLoader;
    private final ConnectorClassLoaderFactory classLoaderFactory;
    private final Map<String, ClassLoader> pluginClassLoaders = new ConcurrentHashMap<>();
    private final Map<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();
    private final Map<String, Map<String, ConnectorFactory>> factoriesByPlugin =
            new ConcurrentHashMap<>();

    /**
     * Creates a plugin manager using the default connector class loader factory.
     *
     * @param pluginDir directory containing installed plugin directories
     * @param spiClassLoader class loader exposing the connector SPI to plugins
     */
    public PluginManager(Path pluginDir, ClassLoader spiClassLoader) {
        this(pluginDir, spiClassLoader, new DefaultConnectorClassLoaderFactory());
    }

    /**
     * Creates a plugin manager with an explicit class loader factory.
     *
     * @param pluginDir directory containing installed plugin directories
     * @param spiClassLoader class loader exposing the connector SPI to plugins
     * @param classLoaderFactory factory creating the class loader of each plugin
     */
    public PluginManager(Path pluginDir, ClassLoader spiClassLoader,
                         ConnectorClassLoaderFactory classLoaderFactory) {
        this.pluginDir = Objects.requireNonNull(pluginDir, "pluginDir must not be null");
        this.spiClassLoader = Objects.requireNonNull(spiClassLoader, "spiClassLoader must not be null");
        this.classLoaderFactory =
                Objects.requireNonNull(classLoaderFactory, "classLoaderFactory must not be null");
    }

    /**
     * Loads every plugin directory found under the configured plugin directory.
     *
     * @throws ConfigurationException when the plugin directory cannot be scanned
     */
    public void loadPlugins() {
        validatePluginDir();
        try (DirectoryStream<Path> dirs = Files.newDirectoryStream(pluginDir, Files::isDirectory)) {
            for (Path dir : dirs) {
                loadPlugin(dir);
            }
        } catch (IOException e) {
            throw new ConfigurationException(
                    PangrovaErrorCode.CONFIG_INVALID_VALUE,
                    "Failed to scan plugin directory: " + pluginDir, e);
        }
    }

    /**
     * Returns the factory registered for a connector name.
     *
     * @param name connector name
     * @return the registered factory, or {@code null} when no plugin registered it
     */
    public ConnectorFactory getConnectorFactory(String name) {
        return connectorFactories.get(name);
    }

    /**
     * Unloads a plugin and removes the connector factories it registered.
     *
     * @param pluginName name of the plugin to unload
     */
    public void unloadPlugin(String pluginName) {
        ClassLoader cl = pluginClassLoaders.remove(pluginName);
        if (cl == null) return;
        Map<String, ConnectorFactory> owned = factoriesByPlugin.remove(pluginName);
        if (owned != null) {
            owned.keySet().forEach(connectorFactories::remove);
        }
        closeClassLoader(pluginName, cl);
    }

    /**
     * Returns an immutable snapshot of all registered connector factories.
     *
     * @return connector name to factory mapping
     */
    public Map<String, ConnectorFactory> getAllFactories() {
        return Map.copyOf(connectorFactories);
    }

    private void validatePluginDir() {
        if (!Files.isDirectory(pluginDir)) {
            throw new ConfigurationException(
                    PangrovaErrorCode.CONFIG_INVALID_VALUE,
                    "Plugin directory does not exist: " + pluginDir);
        }
    }

    private void loadPlugin(Path dir) {
        Path fileName = dir.getFileName();
        if (fileName == null) {
            throw new IllegalArgumentException("Plugin directory has no name: " + dir);
        }
        String name = fileName.toString();
        URL[] urls = collectJarUrls(dir);
        ClassLoader cl = classLoaderFactory.createClassLoader(name, urls, spiClassLoader);
        pluginClassLoaders.put(name, cl);
        Map<String, ConnectorFactory> discovered = discoverFactories(cl);
        factoriesByPlugin.put(name, discovered);
        LOG.info("event=plugin_loaded plugin={} jarCount={} factories={}",
                name, urls.length, discovered.keySet());
    }

    private URL[] collectJarUrls(Path dir) {
        List<URL> urls = new ArrayList<>();
        try (DirectoryStream<Path> jars = Files.newDirectoryStream(dir, "*.jar")) {
            for (Path jar : jars) {
                urls.add(jar.toUri().toURL());
            }
        } catch (IOException e) {
            throw new ConfigurationException(
                    PangrovaErrorCode.CONFIG_INVALID_VALUE,
                    "Failed to collect JARs from: " + dir, e);
        }
        return urls.toArray(URL[]::new);
    }

    private Map<String, ConnectorFactory> discoverFactories(ClassLoader cl) {
        ConnectorFactoryRegistry registry = ConnectorFactoryRegistry.discover(cl);
        Map<String, ConnectorFactory> registered = new HashMap<>();
        registry.factories().forEach((name, factory) -> {
            ConnectorFactory previous = connectorFactories.putIfAbsent(name, factory);
            if (previous != null) {
                throw new ConfigurationException(
                        PangrovaErrorCode.CONFIG_INVALID_VALUE,
                        "Duplicate ConnectorFactory name '" + name + "' across plugins: "
                                + previous.getClass().getName() + " and "
                                + factory.getClass().getName());
            }
            registered.put(name, factory);
        });
        return Map.copyOf(registered);
    }

    private void closeClassLoader(String pluginName, ClassLoader cl) {
        if (!(cl instanceof AutoCloseable closeable)) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            LOG.warn("event=classloader_close_failed plugin={} error={}", pluginName, e.getMessage(), e);
        }
    }
}
