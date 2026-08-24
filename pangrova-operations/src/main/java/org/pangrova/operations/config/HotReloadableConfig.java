package org.pangrova.operations.config;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Supports hot-reloading of non-structural configuration changes without process restart.
 *
 * <p>Holds an atomic reference to the current configuration snapshot. When {@link #reload}
 * is called with new properties, the snapshot is atomically replaced and all registered
 * listeners are notified of the change.
 *
 * <p>Thread-safe: reads are lock-free via AtomicReference; listeners are stored in a
 * CopyOnWriteArrayList for safe concurrent iteration during notification.
 */
public final class HotReloadableConfig {

    private static final Logger LOG = LoggerFactory.getLogger(HotReloadableConfig.class);

    private final AtomicReference<Map<String, String>> currentConfig;
    private final List<Consumer<Map<String, String>>> listeners;

    /**
     * Creates a hot-reloadable config with the given initial properties.
     *
     * @param initialProperties the initial configuration key-value pairs
     */
    public HotReloadableConfig(Map<String, String> initialProperties) {
        if (initialProperties == null) {
            throw new IllegalArgumentException("initialProperties must not be null");
        }
        this.currentConfig = new AtomicReference<>(Map.copyOf(initialProperties));
        this.listeners = new CopyOnWriteArrayList<>();
    }

    /**
     * Returns the current configuration value for the given key, or null if absent.
     */
    public String get(String key) {
        return currentConfig.get().get(key);
    }

    /**
     * Returns the current configuration value for the given key, or the default if absent.
     */
    public String getOrDefault(String key, String defaultValue) {
        return currentConfig.get().getOrDefault(key, defaultValue);
    }

    /**
     * Returns an unmodifiable snapshot of all current configuration properties.
     */
    public Map<String, String> getAll() {
        return currentConfig.get();
    }

    /**
     * Atomically replaces the configuration with new properties and notifies all listeners.
     *
     * @param newProperties the new configuration key-value pairs
     * @return true if the configuration changed, false if identical to current
     */
    public boolean reload(Map<String, String> newProperties) {
        if (newProperties == null) {
            throw new IllegalArgumentException("newProperties must not be null");
        }
        Map<String, String> immutableNew = Map.copyOf(newProperties);
        Map<String, String> previous = currentConfig.getAndSet(immutableNew);

        if (previous.equals(immutableNew)) {
            LOG.debug("Configuration reload skipped: no changes detected");
            return false;
        }

        LOG.info("Configuration reloaded: changedKeys={}",
                countChangedKeys(previous, immutableNew));
        notifyListeners(immutableNew);
        return true;
    }

    /**
     * Registers a listener that is called whenever configuration is reloaded.
     * The listener receives the new configuration snapshot.
     *
     * @param listener the change listener
     */
    public void addChangeListener(Consumer<Map<String, String>> listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        listeners.add(listener);
    }

    private void notifyListeners(Map<String, String> newConfig) {
        for (Consumer<Map<String, String>> listener : listeners) {
            try {
                listener.accept(newConfig);
            } catch (RuntimeException e) {
                LOG.error("Configuration change listener failed: listener={} error={}",
                        listener.getClass().getSimpleName(), e.getMessage(), e);
            }
        }
    }

    private static int countChangedKeys(Map<String, String> previous, Map<String, String> current) {
        int changed = 0;
        for (Map.Entry<String, String> entry : current.entrySet()) {
            String oldValue = previous.get(entry.getKey());
            if (!entry.getValue().equals(oldValue)) {
                changed++;
            }
        }
        for (String key : previous.keySet()) {
            if (!current.containsKey(key)) {
                changed++;
            }
        }
        return changed;
    }
}
