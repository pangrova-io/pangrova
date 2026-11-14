package org.pangrova.connector.runtime.discovery;

import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.plugin.ConnectorFactory;
import org.pangrova.connector.api.constants.ConnectorFactoryContract;
import org.pangrova.connector.api.enums.ConnectorRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Immutable registry of connector factories discovered from a classloader.
 *
 * <p>Discovery is fail-closed: null factories, blank names, duplicate names,
 * incompatible API versions and empty role declarations are rejected. A
 * role-scoped discovery excludes factories not declaring that process role.</p>
 */
public final class ConnectorFactoryRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorFactoryRegistry.class);

    private final Map<String, ConnectorFactory> factories;

    private ConnectorFactoryRegistry(Map<String, ConnectorFactory> factories) {
        this.factories = Map.copyOf(factories);
    }

    /** Discovers every {@link ConnectorFactory} visible to the given classloader. */
    public static ConnectorFactoryRegistry discover(ClassLoader classLoader) {
        return discover(classLoader, null);
    }

    /** Discovers factories applicable to {@code role}; null keeps all roles. */
    public static ConnectorFactoryRegistry discover(ClassLoader classLoader, ConnectorRole role) {
        return of(ServiceLoader.load(ConnectorFactory.class, classLoader), role);
    }

    /** Builds a registry from an explicit factory inventory. */
    public static ConnectorFactoryRegistry of(Iterable<ConnectorFactory> discovered) {
        return of(discovered, null);
    }

    /** Builds a registry from an explicit inventory filtered by process role. */
    public static ConnectorFactoryRegistry of(
            Iterable<ConnectorFactory> discovered, ConnectorRole role) {
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
            if (factory.apiVersion() != ConnectorFactoryContract.CURRENT_API_VERSION) {
                throw invalid("ConnectorFactory '" + name + "' declares API version "
                        + factory.apiVersion() + " but runtime requires "
                        + ConnectorFactoryContract.CURRENT_API_VERSION);
            }
            Set<ConnectorRole> roles = factory.roles();
            if (roles == null || roles.isEmpty()) {
                throw invalid("ConnectorFactory '" + name + "' must declare at least one role");
            }
            if (role != null && !roles.contains(role)) {
                LOG.info("event=connector_factory_role_excluded factory={} role={}",
                        name, role);
                continue;
            }
            ConnectorFactory previous = byName.putIfAbsent(name, factory);
            if (previous != null) {
                throw invalid("Duplicate ConnectorFactory name '" + name + "': "
                        + previous.getClass().getName() + " and " + factory.getClass().getName());
            }
            LOG.info("event=connector_factory_discovered factory={} class={} role={}",
                    name, factory.getClass().getName(), role);
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

    /** Validates provider configuration before any connector instance is created. */
    public void validateConfiguration(String name, Map<String, String> config) {
        Objects.requireNonNull(config, "config must not be null");
        require(name).validateConfiguration(Map.copyOf(config));
    }

    private static ConfigurationException invalid(String message) {
        return new ConfigurationException(PangrovaErrorCode.CONFIG_INVALID_VALUE, message);
    }
}
