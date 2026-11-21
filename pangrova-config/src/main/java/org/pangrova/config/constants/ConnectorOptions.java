package org.pangrova.config.constants;

import org.pangrova.config.ConfigOption;
import org.pangrova.config.ConfigOptions;
import org.pangrova.config.PangrovaConfiguration;

import java.nio.file.Path;
import java.util.Optional;

/** Deployment-local root of trusted connector plugin installations for both server roles. */
public final class ConnectorOptions {
    public static final ConfigOption<String> PLUGIN_DIRECTORY = ConfigOptions.key("connector.plugin-directory")
            .stringType().withDescription("Optional root containing one directory per connector plugin").build();

    private ConnectorOptions() {}

    /** Resolves the optional trusted root when mapping role configuration. */
    public static Optional<Path> pluginDirectory(PangrovaConfiguration config) {
        return config.getOptional(PLUGIN_DIRECTORY).map(value -> {
            if (value.isBlank()) throw new IllegalArgumentException("connector.plugin-directory is blank");
            return Path.of(value).toAbsolutePath().normalize();
        });
    }
}
