package org.pangrova.worker.nativebridge.jni;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NativeFunctionConfig}.
 */
class NativeFunctionConfigTest {

    @Test
    void defaultConfigHasAutoRegisterEnabled() {
        assertThat(NativeFunctionConfig.DEFAULT.autoRegister()).isTrue();
        assertThat(NativeFunctionConfig.DEFAULT.pluginConfigs()).isEmpty();
    }

    @Test
    void disabledConfigHasAutoRegisterDisabled() {
        assertThat(NativeFunctionConfig.DISABLED.autoRegister()).isFalse();
        assertThat(NativeFunctionConfig.DISABLED.pluginConfigs()).isEmpty();
    }

    @Test
    void configForPluginReturnsProvidedConfig() {
        var pluginConfigs = Map.of(
                "geo", Map.of("srid", "4326"),
                "ml", Map.of("model_path", "/tmp/model.bin")
        );
        var config = new NativeFunctionConfig(true, pluginConfigs);

        assertThat(config.configForPlugin("geo")).containsEntry("srid", "4326");
        assertThat(config.configForPlugin("ml")).containsEntry("model_path", "/tmp/model.bin");
    }

    @Test
    void configForPluginReturnsEmptyForUnknownPlugin() {
        var config = new NativeFunctionConfig(true, Map.of("known", Map.of("k", "v")));
        assertThat(config.configForPlugin("unknown")).isEmpty();
    }

    @Test
    void nullPluginConfigsDefaultsToEmptyMap() {
        var config = new NativeFunctionConfig(true, null);
        assertThat(config.pluginConfigs()).isEmpty();
        assertThat(config.configForPlugin("anything")).isEmpty();
    }

    @Test
    void pluginConfigsAreDefensivelyCopied() {
        var mutable = new java.util.HashMap<String, Map<String, String>>();
        var nested = new java.util.HashMap<String, String>();
        nested.put("k", "v");
        mutable.put("plugin", nested);
        var config = new NativeFunctionConfig(true, mutable);

        mutable.put("new_plugin", Map.of("x", "y"));
        nested.put("new", "value");
        assertThat(config.pluginConfigs()).hasSize(1);
        assertThat(config.configForPlugin("plugin")).containsOnlyKeys("k");
    }
}
