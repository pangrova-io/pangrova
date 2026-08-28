package org.pangrova.operations.config;

import java.util.Set;

/**
 * Defines which configuration key prefixes are allowed for hot reload per §6.1
 * of the operations guide (docs/operations/day-2.md).
 *
 * <p>Classification from §6.1:
 * <ul>
 *   <li><b>Hot (reloadable)</b>: Runtime Config, Resource Limits, Feature Flags, Security rules</li>
 *   <li><b>Cold (requires restart)</b>: Connector Config, Cluster Config, structural config</li>
 * </ul>
 *
 * <p>Cold prefixes are those that require a process restart. Any key matching a cold prefix
 * will be rejected by {@link HotReloadableConfig#reload} to prevent unsafe runtime changes
 * to structural configuration.
 */
public final class ConfigReloadPolicy {

    /**
     * Key prefixes classified as cold (requires restart) per §6.1 and §12 of the config reference.
     * These prefixes encompass Connector Config and Cluster Config categories.
     */
    private static final Set<String> COLD_PREFIXES = Set.of(
            // Cluster Config — node identity, ports, discovery, HA topology
            "coordinator.node-id",
            "coordinator.http.port",
            "coordinator.http.max-threads",
            "coordinator.cluster.",
            "worker.node-id",
            "worker.rpc.port",
            "worker.flight.port",

            // Connector Config — data source connections, catalog configuration
            "connector.",
            "catalog.",

            // Security structural config — auth type, TLS, secret provider (not rules)
            "security.authentication.type",
            "security.authorization.type",
            "security.encryption.",
            "security.secret.provider",
            "security.audit.async",

            // Optimizer structural config — case sensitivity, null collation
            "optimizer.calcite.default-null-collation",
            "optimizer.calcite.case-sensitive",

            // Operations structural config — the reload interval itself is cold
            "operations.config.hot-reload-interval"
    );

    private ConfigReloadPolicy() {
        // utility class
    }

    /**
     * Returns the set of cold key prefixes. A config key matching any of these prefixes
     * is not eligible for hot reload.
     */
    public static Set<String> coldPrefixes() {
        return COLD_PREFIXES;
    }

    /**
     * Checks whether a config key is classified as cold (requires restart).
     *
     * @param key the configuration key
     * @return true if the key matches a cold prefix and must not be hot-reloaded
     */
    public static boolean isColdKey(String key) {
        if (key == null) {
            return false;
        }
        for (String prefix : COLD_PREFIXES) {
            if (key.equals(prefix) || key.startsWith(prefix + ".") || key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
