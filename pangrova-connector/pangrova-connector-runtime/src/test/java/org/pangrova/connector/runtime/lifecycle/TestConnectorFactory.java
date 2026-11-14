package org.pangrova.connector.runtime.lifecycle;

import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.plugin.ConnectorContext;
import org.pangrova.connector.api.plugin.ConnectorFactory;

import java.util.Map;

/**
 * Stand-in provider packaged into a temporary plugin JAR by {@link PluginManagerTest}.
 */
public final class TestConnectorFactory implements ConnectorFactory {

    @Override
    public String getName() {
        return "test-plugin";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        throw new UnsupportedOperationException("not used");
    }
}
