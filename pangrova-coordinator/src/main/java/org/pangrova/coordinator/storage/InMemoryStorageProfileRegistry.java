/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.storage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageProfileRegistry;

/**
 * Thread-safe in-memory implementation of {@link StorageProfileRegistry}.
 *
 * <p>Profiles registered via {@code CREATE STORAGE PROFILE} are stored in a
 * {@link ConcurrentHashMap} and survive for the lifetime of the coordinator JVM.
 * A persistent implementation backed by the metadata store can replace this
 * when cross-restart persistence is required.</p>
 */
public final class InMemoryStorageProfileRegistry implements StorageProfileRegistry {

    private final ConcurrentHashMap<String, StorageProfile> profiles = new ConcurrentHashMap<>();

    @Override
    public void register(StorageProfile profile) {
        profiles.put(profile.name(), profile);
    }

    @Override
    public boolean unregister(String name) {
        return profiles.remove(name) != null;
    }

    @Override
    public Optional<StorageProfile> get(String name) {
        return Optional.ofNullable(profiles.get(name));
    }

    @Override
    public List<StorageProfile> list() {
        return List.copyOf(profiles.values());
    }

    @Override
    public Optional<StorageProfile> resolveByUrl(String url) {
        StorageProfile bestMatch = null;
        int longestPrefix = -1;
        for (StorageProfile p : profiles.values()) {
            if (p.matchesUrl(url) && p.prefix().length() > longestPrefix) {
                bestMatch = p;
                longestPrefix = p.prefix().length();
            }
        }
        return Optional.ofNullable(bestMatch);
    }
}
