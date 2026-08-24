package org.pangrova.metadata.dynamictable;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.StaleEpochException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link DynamicTableStateStore} backed by
 * {@link ConcurrentHashMap}.
 *
 * <p>Concurrency strategy: all operations delegate to ConcurrentHashMap
 * which provides thread-safe reads and writes without external locking.</p>
 */
@ThreadSafe
public final class InMemoryDynamicTableStateStore implements DynamicTableStateStore {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryDynamicTableStateStore.class);

    private final Map<String, DynamicTableState> store = new ConcurrentHashMap<>();

    @Override
    public void saveState(DynamicTableState state) {
        store.put(state.dtName(), state);
        LOG.info("action=dt_state_save dtName={} state={} tenantId={}",
                state.dtName(), state.state(), state.tenantId());
    }

    @Override
    public Optional<DynamicTableState> loadState(String dtName) {
        return Optional.ofNullable(store.get(dtName));
    }

    @Override
    public List<DynamicTableState> listActiveByTenant(String tenantId) {
        return store.values().stream()
                .filter(s -> tenantId == null || s.tenantId().equals(tenantId))
                .filter(s -> s.state() == DynamicTableLifecycleState.ACTIVE)
                .toList();
    }

    @Override
    public List<DynamicTableState> listAll() {
        return List.copyOf(store.values());
    }

    @Override
    public boolean delete(String dtName) {
        DynamicTableState removed = store.remove(dtName);
        if (removed != null) {
            LOG.info("action=dt_state_delete dtName={}", dtName);
            return true;
        }
        return false;
    }

    @Override
    public void saveStateIfEpoch(DynamicTableState state, long expectedEpoch) {
        store.compute(state.dtName(), (key, current) -> {
            if (current == null || current.epoch() != expectedEpoch) {
                throw new StaleEpochException(state.dtName(), expectedEpoch);
            }
            return state;
        });
        LOG.info("action=dt_state_cas_save dtName={} state={} expectedEpoch={}",
                state.dtName(), state.state(), expectedEpoch);
    }
}
