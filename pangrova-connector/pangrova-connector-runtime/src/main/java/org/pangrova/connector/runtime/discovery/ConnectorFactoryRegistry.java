package org.pangrova.connector.runtime.discovery;

import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.plugin.ConnectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Immutable registry of connector factories discovered from a classloader.
 *
 * <p>Discovery is fail-closed: a factory with a blank name, a duplicate name,
 * or an {@code org.pangrova} implementation loaded outside the connector SPI
 * classloader is rejected instead of silently overwriting another provider.
 */
public final class ConnectorFactoryRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorFactoryRegistry.class);

    private final Map<String, ConnectorFactory> factories;

    private ConnectorFactoryRegistry(Map<String, ConnectorFactory> factories) {
        this.factories = Map.copyOf(factories);
    }

    /** Discovers every {@link ConnectorFactory} visible to the given classloader. */
    public static ConnectorFactoryRegistry discover(ClassLoader classLoader) {
        return of(ServiceLoader.load(ConnectorFactory.class, classLoader));
    }

    /** Builds a registry from an explicit factory inventory. */
    public static ConnectorFactoryRegistry of(Iterable<ConnectorFactory> discovered) {
        Map<String, ConnectorFactory> byName = new LinkedHashMap<>();
        for (ConnectorFactory factory : discovered) {
            if (factory == null) {
                throw invalid("ConnectorFactory must not be null");
            }
            String name = factory.getName();
            if (name == null || name.isBlank()) {
                throw invalid("ConnectorFactory name must not be blank: "
                        + factory.getClass().getName());
            }
            ConnectorFactory previous = byName.putIfAbsent(name, factory);
            if (previous != null) {
                throw invalid("Duplicate ConnectorFactory name '" + name + "': "
                        + previous.getClass().getName() + " and " + factory.getClass().getName());
            }
            LOG.info("event=connector_factory_discovered factory={} class={}",
                    name, factory.getClass().getName());
        }
        return new ConnectorFactoryRegistry(byName);
    }

    /** Returns the factory registered under {@code name}, if any. */
    public Optional<ConnectorFactory> find(String name) {
        return Optional.ofNullable(factories.get(name));
    }

    /**
     * Returns the factory registered under {@code name}.
     *
     * @throws ConfigurationException when no factory is registered
     */
    public ConnectorFactory require(String name) {
        ConnectorFactory factory = factories.get(name);
        if (factory == null) {
            throw new ConfigurationException(
                    PangrovaErrorCode.CONFIG_INVALID_VALUE,
                    "No ConnectorFactory found for provider '" + name + "'");
        }
        return factory;
    }

    /** Returns the discovered factories keyed by connector name. */
    public Map<String, ConnectorFactory> factories() {
        return factories;
    }

    /** Returns the discovered connector names. */
    public Set<String> names() {
        return factories.keySet();
    }

    private static ConfigurationException invalid(String message) {
        return new ConfigurationException(PangrovaErrorCode.CONFIG_INVALID_VALUE, message);
    }
}
