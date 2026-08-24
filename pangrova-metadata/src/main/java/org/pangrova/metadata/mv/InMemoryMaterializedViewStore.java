package org.pangrova.metadata.mv;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link MaterializedViewStore} backed by ConcurrentHashMap.
 * <p>
 * Supports an optional {@link PersistenceHook} for durable storage delegation.
 * When a hook is provided, all mutations are forwarded to the hook after updating
 * the in-memory state. On construction, the hook's {@code loadAll()} is called
 * to hydrate the in-memory map from the durable store.
 *
 * All operations are safe for concurrent access.
 */
@ThreadSafe
public final class InMemoryMaterializedViewStore implements MaterializedViewStore {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryMaterializedViewStore.class);

    private final Map<String, MaterializedViewMetadata> store = new ConcurrentHashMap<>();
    private final PersistenceHook hook;

    /**
     * Create a purely in-memory store (no durable backing).
     */
    public InMemoryMaterializedViewStore() {
        this(PersistenceHook.NO_OP);
    }

    /**
     * Create an in-memory store backed by a durable persistence hook.
     * Hydrates from the hook on construction.
     */
    public InMemoryMaterializedViewStore(PersistenceHook hook) {
        this.hook = hook;
        hydrate();
    }

    @Override
    public void save(MaterializedViewMetadata metadata) {
        store.put(metadata.mvName(), metadata);
        hook.onSave(metadata);
        LOG.info("action=mv_save mvName={} state={}", metadata.mvName(), metadata.state());
    }

    @Override
    public Optional<MaterializedViewMetadata> load(String mvName) {
        return Optional.ofNullable(store.get(mvName));
    }

    @Override
    public boolean delete(String mvName) {
        MaterializedViewMetadata removed = store.remove(mvName);
        if (removed != null) {
            hook.onDelete(mvName);
            LOG.info("action=mv_delete mvName={}", mvName);
            return true;
        }
        return false;
    }

    @Override
    public List<MaterializedViewMetadata> listActive() {
        return store.values().stream()
                .filter(m -> m.state() != MaterializedViewState.DROPPED)
                .toList();
    }

    @Override
    public List<MaterializedViewMetadata> listAll() {
        return List.copyOf(store.values());
    }

    private void hydrate() {
        List<MaterializedViewMetadata> persisted = hook.loadAll();
        for (MaterializedViewMetadata m : persisted) {
            store.put(m.mvName(), m);
        }
        if (!persisted.isEmpty()) {
            LOG.info("action=mv_hydrate count={}", persisted.size());
        }
    }

    /**
     * Hook for delegating persistence to a durable store (JDBC, file, etc.).
     * Default NO_OP implementation does nothing — pure in-memory mode.
     */
    public interface PersistenceHook {

        PersistenceHook NO_OP = new PersistenceHook() {};

        default void onSave(MaterializedViewMetadata metadata) { }

        default void onDelete(String mvName) { }

        default List<MaterializedViewMetadata> loadAll() {
            return List.of();
        }
    }
}
