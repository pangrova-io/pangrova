package org.pangrova.worker.jni;

import java.util.Map;

/**
 * Configuration for native function registration at worker startup.
 *
 * <p>Controls whether UDF/UDAF auto-registration is enabled and provides
 * per-plugin configuration properties.</p>
 *
 * @param autoRegister  when true, discovers {@link org.pangrova.spi.function.FunctionFactory}
 *                      implementations via ServiceLoader at startup and registers them
 *                      with the native engine
 * @param pluginConfigs per-plugin configuration properties keyed by plugin name
 *                      (from {@link org.pangrova.spi.function.FunctionFactory#getName()})
 */
public record NativeFunctionConfig(
        boolean autoRegister,
        Map<String, Map<String, String>> pluginConfigs) {

    /** Default configuration with auto-registration enabled and no plugin configs. */
    public static final NativeFunctionConfig DEFAULT =
            new NativeFunctionConfig(true, Map.of());

    /** Configuration with auto-registration disabled. */
    public static final NativeFunctionConfig DISABLED =
            new NativeFunctionConfig(false, Map.of());

    public NativeFunctionConfig {
        if (pluginConfigs == null || pluginConfigs.isEmpty()) {
            pluginConfigs = Map.of();
        } else {
            java.util.Map<String, Map<String, String>> copy = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, Map<String, String>> entry : pluginConfigs.entrySet()) {
                if (entry.getKey() == null || entry.getKey().isBlank()) {
                    throw new IllegalArgumentException("plugin name must not be blank");
                }
                Map<String, String> values = entry.getValue() == null
                        ? Map.of()
                        : Map.copyOf(entry.getValue());
                copy.put(entry.getKey(), values);
            }
            pluginConfigs = Map.copyOf(copy);
        }
    }

    /**
     * Returns the configuration properties for the given plugin name.
     * Returns an empty map if no plugin-specific config is defined.
     *
     * @param pluginName the plugin name from {@link org.pangrova.spi.function.FunctionFactory#getName()}
     * @return plugin configuration properties, never null
     */
    public Map<String, String> configForPlugin(String pluginName) {
        return pluginConfigs.getOrDefault(pluginName, Map.of());
    }
}
