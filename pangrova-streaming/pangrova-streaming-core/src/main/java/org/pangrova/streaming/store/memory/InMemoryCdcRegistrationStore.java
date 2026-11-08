package org.pangrova.streaming.store.memory;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.streaming.api.cdc.CdcRegistration;
import org.pangrova.streaming.api.cdc.CdcRegistrationStore;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link CdcRegistrationStore} backed by a {@link ConcurrentHashMap}.
 * Suitable for single-node deployments and testing.
 */
@ThreadSafe
public final class InMemoryCdcRegistrationStore implements CdcRegistrationStore {

    private final Map<String, CdcRegistration> registrations = new ConcurrentHashMap<>();

    @Override
    public void register(CdcRegistration registration) {
        registrations.put(registration.streamId(), registration);
    }

    @Override
    public boolean unregister(String streamId) {
        return registrations.remove(streamId) != null;
    }

    @Override
    public Optional<CdcRegistration> getRegistration(String streamId) {
        return Optional.ofNullable(registrations.get(streamId));
    }

    @Override
    public List<CdcRegistration> listRegistrations() {
        return Collections.unmodifiableList(List.copyOf(registrations.values()));
    }
}
