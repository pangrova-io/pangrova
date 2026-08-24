package org.pangrova.operations.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HotReloadableConfigTest {

    @Test
    void initialConfigIsAccessible() {
        HotReloadableConfig config = new HotReloadableConfig(
                Map.of("timeout", "30", "retries", "3"));

        assertThat(config.get("timeout")).isEqualTo("30");
        assertThat(config.get("retries")).isEqualTo("3");
        assertThat(config.get("missing")).isNull();
    }

    @Test
    void getOrDefaultReturnsDefaultWhenKeyAbsent() {
        HotReloadableConfig config = new HotReloadableConfig(Map.of("a", "1"));

        assertThat(config.getOrDefault("a", "x")).isEqualTo("1");
        assertThat(config.getOrDefault("b", "x")).isEqualTo("x");
    }

    @Test
    void reloadUpdatesConfigAtomically() {
        HotReloadableConfig config = new HotReloadableConfig(Map.of("timeout", "30"));

        boolean changed = config.reload(Map.of("timeout", "60", "newKey", "value"));

        assertThat(changed).isTrue();
        assertThat(config.get("timeout")).isEqualTo("60");
        assertThat(config.get("newKey")).isEqualTo("value");
    }

    @Test
    void reloadReturnsFalseWhenNoChange() {
        Map<String, String> props = Map.of("a", "1", "b", "2");
        HotReloadableConfig config = new HotReloadableConfig(props);

        boolean changed = config.reload(Map.of("a", "1", "b", "2"));

        assertThat(changed).isFalse();
    }

    @Test
    void listenersAreNotifiedOnReload() {
        HotReloadableConfig config = new HotReloadableConfig(Map.of("k", "v1"));

        List<Map<String, String>> notifications = new ArrayList<>();
        config.addChangeListener(notifications::add);

        config.reload(Map.of("k", "v2"));

        assertThat(notifications).hasSize(1);
        assertThat(notifications.get(0)).containsEntry("k", "v2");
    }

    @Test
    void listenerFailureDoesNotPreventOtherListeners() {
        HotReloadableConfig config = new HotReloadableConfig(Map.of("k", "v1"));

        List<String> received = new ArrayList<>();
        config.addChangeListener(m -> { throw new RuntimeException("boom"); });
        config.addChangeListener(m -> received.add(m.get("k")));

        config.reload(Map.of("k", "v2"));

        assertThat(received).containsExactly("v2");
    }

    @Test
    void nullInitialPropertiesThrowsException() {
        assertThatThrownBy(() -> new HotReloadableConfig(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nullReloadPropertiesThrowsException() {
        HotReloadableConfig config = new HotReloadableConfig(Map.of("k", "v"));
        assertThatThrownBy(() -> config.reload(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getAllReturnsImmutableSnapshot() {
        HotReloadableConfig config = new HotReloadableConfig(Map.of("a", "1"));
        Map<String, String> snapshot = config.getAll();

        assertThat(snapshot).containsEntry("a", "1");
        assertThatThrownBy(() -> snapshot.put("b", "2"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
