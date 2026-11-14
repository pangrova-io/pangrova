package org.pangrova.connector.runtime.lifecycle;

import org.pangrova.connector.api.constants.ConnectorFactoryContract;

import org.pangrova.connector.api.enums.ConnectorRole;

import java.util.Set;

import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.plugin.ConnectorContext;
import org.pangrova.connector.api.plugin.ConnectorFactory;

import java.util.Map;

/**
 * Stand-in provider packaged into a temporary plugin JAR by {@link ConnectorPluginInventoryTest}.
 */
public final class TestConnectorFactory implements ConnectorFactory {

    @Override
    public int apiVersion() {
        return ConnectorFactoryContract.CURRENT_API_VERSION;
    }

    @Override
    public Set<ConnectorRole> roles() {
        return Set.of(ConnectorRole.COORDINATOR);
    }

    @Override
    public void validateConfiguration(Map<String, String> config) {
        if (Thread.currentThread().getContextClassLoader().getResource("plugin-test-resource") == null) {
            throw new IllegalStateException("Missing plugin resource");
        }
    }

    @Override
    public String getName() {
        return "test-plugin";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        validateConfiguration(config);
        throw new IllegalArgumentException("plugin-local-" + catalogName);
    }
}
