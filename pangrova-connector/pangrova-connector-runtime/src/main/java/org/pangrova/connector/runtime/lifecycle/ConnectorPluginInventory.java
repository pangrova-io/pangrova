package org.pangrova.connector.runtime.lifecycle;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.connector.api.enums.ConnectorRole;
import org.pangrova.connector.api.plugin.ConnectorFactory;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.plugin.ConnectorContext;
import org.pangrova.connector.runtime.classloader.DefaultConnectorClassLoaderFactory;
import org.pangrova.connector.runtime.classloader.ConnectorClassLoaderFactory;
import org.pangrova.connector.runtime.classloader.PluginClassLoader;
import org.pangrova.connector.runtime.discovery.ConnectorFactoryRegistry;
import org.pangrova.connector.runtime.enums.SharedPackagePolicy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

/** Immutable role inventory. The process closes it after all catalog connectors have drained. */
public final class ConnectorPluginInventory implements AutoCloseable {
    private final List<PluginClassLoader> loaders;
    private ConnectorFactoryRegistry registry;

    private ConnectorPluginInventory(ConnectorFactoryRegistry registry, List<PluginClassLoader> loaders) {
        this.registry = registry;
        this.loaders = loaders;
    }

    /** Discovers bundled factories and every configured plugin before publishing any factories. */
    public static ConnectorPluginInventory open(ClassLoader parent, ConnectorRole role, Optional<Path> root) {
        return open(parent, role, root, new DefaultConnectorClassLoaderFactory(SharedPackagePolicy.NARROW, Set.of()));
    }

    static ConnectorPluginInventory open(ClassLoader parent, ConnectorRole role, Optional<Path> root,
            ConnectorClassLoaderFactory isolation) {
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(parent, "parent");
        var factories = new ArrayList<ConnectorFactory>();
        ServiceLoader.load(ConnectorFactory.class, parent).forEach(factories::add);
        List<PluginClassLoader> loaders = new ArrayList<>();
        try {
            if (root.isPresent()) {
                try (var entries = Files.list(root.get())) {
                    for (Path directory : entries.filter(Files::isDirectory).sorted().toList()) {
                        Path pluginName = directory.getFileName();
                        if (pluginName == null) {
                            throw new IllegalStateException("Connector plugin directory has no name: " + directory);
                        }
                        PluginClassLoader loader = isolation.createClassLoader(
                                pluginName.toString(), jarUrls(directory), parent);
                        loaders.add(loader);
                        loadFactories(directory, loader, factories);
                    }
                }
            }
            return new ConnectorPluginInventory(ConnectorFactoryRegistry.of(factories, role), loaders);
        } catch (IOException failure) {
            var invalid = new ConfigurationException(PangrovaErrorCode.CONFIG_INVALID_VALUE,
                    "Cannot scan connector plugin directory " + root, failure);
            release(loaders, invalid);
            throw invalid;
        } catch (RuntimeException | Error failure) {
            release(loaders, failure);
            throw failure;
        }
    }

    private static URL[] jarUrls(Path directory) throws IOException {
        List<URL> urls = new ArrayList<>();
        try (var entries = Files.list(directory)) {
            for (Path file : entries.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".jar")).sorted().toList()) {
                urls.add(file.toUri().toURL());
            }
        }
        return urls.toArray(URL[]::new);
    }

    private static void loadFactories(Path directory, PluginClassLoader loader, List<ConnectorFactory> factories) {
        int previous = factories.size();
        for (var provider : ServiceLoader.load(ConnectorFactory.class, loader).stream().toList()) {
            if (provider.type().getClassLoader() != loader) {
                throw new ConfigurationException(PangrovaErrorCode.CONFIG_INVALID_VALUE,
                        "Plugin " + directory + " does not contain its declared factory " + provider.type().getName());
            }
            var factory = loader.withContext(provider::get);
            factories.add(new IsolatedFactory(loader, factory));
        }
        if (previous == factories.size()) {
            throw new ConfigurationException(PangrovaErrorCode.CONFIG_INVALID_VALUE,
                    "No connector factory declared in plugin directory " + directory);
        }
    }

    /** Each entry point into a plugin factory sees its own SDK services and resources. */
    private record IsolatedFactory(PluginClassLoader loader, ConnectorFactory delegate) implements ConnectorFactory {
        @Override public String getName() { return loader.withContext(delegate::getName); }
        @Override public int apiVersion() { return loader.withContext(delegate::apiVersion); }
        @Override public Set<ConnectorRole> roles() { return loader.withContext(delegate::roles); }

        @Override
        public void validateConfiguration(Map<String, String> config) {
            loader.withContext(() -> {
                delegate.validateConfiguration(config);
                return null;
            });
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
            return loader.withContext(() -> delegate.create(catalogName, config, context));
        }
    }

    /** Returns the published factories while the role still owns this inventory. */
    public synchronized ConnectorFactoryRegistry registry() {
        if (registry == null) throw new IllegalStateException("Connector plugin inventory is closed");
        return registry;
    }

    @Override
    public synchronized void close() {
        registry = null;
        release(loaders, null);
    }

    private static void release(List<PluginClassLoader> loaders, Throwable primary) {
        var releases = new ArrayList<Runnable>(loaders.size());
        for (int index = loaders.size() - 1; index >= 0; index--) {
            PluginClassLoader loader = loaders.get(index);
            releases.add(() -> {
                try {
                    loader.close();
                    loaders.remove(loader);
                } catch (IOException failure) {
                    throw new UncheckedIOException("Failed closing connector plugin " + loader.getPluginName(),
                            failure);
                }
            });
        }
        ResourceCloser.closeAll(primary, releases.toArray(Runnable[]::new));
    }
}
