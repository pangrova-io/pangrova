package org.pangrova.config.constants;

import org.pangrova.config.ConfigOption;
import org.pangrova.config.ConfigOptions;

/**
 * Configuration options shared by both the coordinator and worker processes.
 *
 * <p>These options use the same YAML key in both deployment modes, so callers
 * read them via the same key regardless of which options class is in scope.</p>
 */
public final class CommonOptions {

    private CommonOptions() {
        // constants only
    }

    public static final ConfigOption<Boolean> OBSERVABILITY_ENABLED =
            ConfigOptions.key("observability.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable the observability module")
                    .build();

    // ---- Observability ----

    public static final ConfigOption<String> OBSERVABILITY_SERVICE_NAME =
            ConfigOptions.key("observability.service-name")
            .stringType()
            .withDescription("OTel service name for this process")
            .build();

    public static final ConfigOption<String> OBSERVABILITY_OTLP_ENDPOINT =
            ConfigOptions.key("observability.otlp-endpoint")
            .stringType()
            .withDescription("OTel OTLP exporter endpoint URL")
            .build();

    public static final ConfigOption<Boolean> SECURITY_ENABLED =
            ConfigOptions.key("security.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable the security module (defaults to false)")
                    .build();
}
