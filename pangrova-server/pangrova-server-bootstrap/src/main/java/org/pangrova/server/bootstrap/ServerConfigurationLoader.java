package org.pangrova.server.bootstrap;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.YamlConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves command-line YAML, role defaults and environment overrides for the process entry points. */
public final class ServerConfigurationLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ServerConfigurationLoader.class);

    private ServerConfigurationLoader() {}

    /** Loads the role's configuration before constructing any services. */
    public static PangrovaConfiguration load(String[] args, String defaultResource) throws IOException {
        String configPath = parseConfigPath(args);
        Map<String, String> flatMap;
        if (configPath != null) {
            LOG.info("event=config_loading source=cli_yaml path={}",
                    configPath.replace('\r', '_').replace('\n', '_'));
            flatMap = YamlConfigLoader.loadWithDefaults(Path.of(configPath), defaultResource);
        } else {
            LOG.info("event=config_loading source=classpath_default resource={}", defaultResource);
            flatMap = YamlConfigLoader.loadFromClasspath(defaultResource);
        }
        return PangrovaConfiguration.fromYamlWithEnvOverride(flatMap);
    }

    /**
     * Scans CLI args for {@code --config <path>}. Returns the path or {@code null} if absent.
     */
    static String parseConfigPath(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--config".equals(args[i])) {
                return args[i + 1];
            }
        }
        return null;
    }
}
