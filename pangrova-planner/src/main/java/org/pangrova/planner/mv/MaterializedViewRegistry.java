package org.pangrova.planner.mv;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for materialized view definitions used in Dynamic Table query rewrite.
 * Uses an in-memory registry backed by ConcurrentHashMap.
 *
 * <p>Renamed from {@code MaterializedViewRegistry} per C3 naming rules
 * (ban on *Manager suffix). This class is a pure registry/lookup, not an
 * orchestrating manager.</p>
 */
@org.pangrova.common.concurrent.ThreadSafe
public class MaterializedViewRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewRegistry.class);

    private final Map<String, MaterializedViewDefinition> registry = new ConcurrentHashMap<>();
    private final MaterializedViewLifecycleListener listener;

    public MaterializedViewRegistry() {
        this(MaterializedViewLifecycleListener.NO_OP);
    }

    public MaterializedViewRegistry(MaterializedViewLifecycleListener listener) {
        this.listener = listener;
    }

    /**
     * Drop a materialized view, removing it from the registry.
     */
    public void dropMaterializedView(String mvName) {
        if (registry.remove(mvName) == null) {
            throw new MaterializedViewException(PangrovaErrorCode.MATERIALIZED_VIEW_CREATION_FAILURE,
                    "Materialized view not found: " + mvName);
        }
        listener.onDropped(mvName);
        LOG.info("action=mv_dropped mvName={}", mvName);
    }

    /**
     * Returns the current materialized view definition, if registered.
     */
    public Optional<MaterializedViewDefinition> getDefinition(String mvName) {
        return Optional.ofNullable(registry.get(mvName));
    }

    /**
     * Returns all registered materialized view definitions.
     */
    public Collection<MaterializedViewDefinition> getAllDefinitions() {
        return List.copyOf(registry.values());
    }

    /**
     * Register a materialized view definition directly — used for recovery and testing.
     */
    public void register(MaterializedViewDefinition definition) {
        registry.put(definition.name(), definition);
    }

    /**
     * Bulk-register materialized view definitions — used during recovery from persistent store.
     * Does not fire lifecycle events (the data is already persisted).
     */
    public void registerAll(Collection<MaterializedViewDefinition> definitions) {
        for (MaterializedViewDefinition def : definitions) {
            registry.put(def.name(), def);
        }
        LOG.info("action=mv_bulk_register count={}", definitions.size());
    }

    /**
     * Listener for materialized view lifecycle events, enabling persistence delegation.
     * Default NO_OP does nothing — pure in-memory mode.
     */
    public interface MaterializedViewLifecycleListener {

        MaterializedViewLifecycleListener NO_OP = new MaterializedViewLifecycleListener() {};

        default void onCreated(MaterializedViewDefinition definition) { }

        default void onRefreshed(MaterializedViewDefinition definition) { }

        default void onDropped(String mvName) { }
    }
}
