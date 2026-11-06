package org.pangrova.table.core.config;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Deployment switches for the table control plane.
 *
 * <p>{@code enabled} turns on registry shadow writes (control records are created
 * but permissions do not change). {@code externalReadOnly} turns on enforcement:
 * OBSERVED and LINKED tables reject DML and destructive DDL before reaching the
 * connector. This split follows the staged rollout: shadow first, enforcement later.</p>
 */
public record TableControlSettings(
        boolean enabled,
        boolean externalReadOnly,
        Set<String> legacyWriteAllowlist) {

    private static final String ENABLED_PROPERTY = "pangrova.table-control.enabled";
    private static final String READ_ONLY_PROPERTY = "pangrova.table-control.external-readonly";
    private static final String ALLOWLIST_PROPERTY =
            "pangrova.table-control.legacy-write-allowlist";

    public TableControlSettings {
        legacyWriteAllowlist = legacyWriteAllowlist == null
                ? Set.of() : Set.copyOf(legacyWriteAllowlist);
    }

    /** Defaults for staged rollout: registry shadow writes on, enforcement off. */
    public static TableControlSettings defaults() {
        return new TableControlSettings(true, false, Set.of());
    }

    /** Reads the switches from system properties, defaulting to shadow mode. */
    public static TableControlSettings fromSystemProperties() {
        return new TableControlSettings(
                Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "true")),
                Boolean.parseBoolean(System.getProperty(READ_ONLY_PROPERTY, "false")),
                parseAllowlist(System.getProperty(ALLOWLIST_PROPERTY, "")));
    }

    private static Set<String> parseAllowlist(String value) {
        Set<String> names = new LinkedHashSet<>();
        Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(entry -> !entry.isEmpty())
                .forEach(names::add);
        return Set.copyOf(names);
    }

    public boolean isLegacyWriteAllowlisted(String qualifiedName) {
        return legacyWriteAllowlist.contains(qualifiedName);
    }
}
