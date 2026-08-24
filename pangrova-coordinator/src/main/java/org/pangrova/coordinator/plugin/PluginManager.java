package org.pangrova.coordinator.plugin;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.spi.connector.ConnectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Manages connector plugin lifecycle: discovery, classloading, and factory registration.
 *
 * <p>This class was migrated from pangrova-spi to pangrova-coordinator because SPI modules
 * should only contain contract interfaces and value objects, not implementation logic.</p>
 *
 * <p>Concurrency strategy: ConcurrentHashMap for classloader and factory registries.
 * Plugin load/unload operations are expected to be called from the bootstrap thread.</p>
 */
@ThreadSafe
public class PluginManager {

    private static final Logger LOG = LoggerFactory.getLogger(PluginManager.class);

    private final Path pluginDir;
    private final ClassLoader spiClassLoader;
    private final Map<String, PluginClassLoader> pluginClassLoaders = new ConcurrentHashMap<>();
    private final Map<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();

    public PluginManager(Path pluginDir, ClassLoader spiClassLoader) {
        this.pluginDir = Objects.requireNonNull(pluginDir, "pluginDir must not be null");
        this.spiClassLoader = Objects.requireNonNull(spiClassLoader, "spiClassLoader must not be null");
    }

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

    public ConnectorFactory getConnectorFactory(String name) {
        return connectorFactories.get(name);
    }

    public void unloadPlugin(String pluginName) {
        PluginClassLoader cl = pluginClassLoaders.remove(pluginName);
        if (cl == null) return;
        removeFactoriesForPlugin(pluginName);
        closeClassLoader(cl);
    }

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
        String name = dir.getFileName().toString();
        URL[] urls = collectJarUrls(dir);
        PluginClassLoader cl = new PluginClassLoader(name, urls, spiClassLoader);
        pluginClassLoaders.put(name, cl);
        discoverFactories(cl);
        LOG.info("event=plugin_loaded plugin={} jarCount={}", name, urls.length);
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

    private void discoverFactories(PluginClassLoader cl) {
        ServiceLoader<ConnectorFactory> loader = ServiceLoader.load(ConnectorFactory.class, cl);
        for (ConnectorFactory factory : loader) {
            connectorFactories.put(factory.getName(), factory);
            LOG.info("event=connector_factory_discovered factory={}", factory.getName());
        }
    }

    private void removeFactoriesForPlugin(String pluginName) {
        connectorFactories.entrySet().removeIf(entry -> {
            ClassLoader cl = entry.getValue().getClass().getClassLoader();
            return cl instanceof PluginClassLoader pcl && pcl.getPluginName().equals(pluginName);
        });
    }

    private void closeClassLoader(PluginClassLoader cl) {
        try {
            cl.close();
        } catch (IOException e) {
            LOG.warn("event=classloader_close_failed plugin={} error={}", cl.getPluginName(), e.getMessage(), e);
        }
    }
}
