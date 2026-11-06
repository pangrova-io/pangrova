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
package org.pangrova.testing.testkit.file;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.file.api.error.FileStorageException;
import org.pangrova.persistence.file.api.profile.StorageProfileRegistry;
import org.pangrova.persistence.file.api.profile.StorageProfileRevision;

/**
 * Thread-safe in-memory implementation of {@link StorageProfileRegistry}.
 *
 * <p>Used by focused tests only. Production SQL uses the PostgreSQL authority;
 * this map is not a coordinator bootstrap fallback.</p>
 */
public final class InMemoryStorageProfileRegistry implements StorageProfileRegistry {

    private final ConcurrentHashMap<String, StorageProfileRevision> current = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RevisionKey, StorageProfileRevision> history = new ConcurrentHashMap<>();

    @Override
    public void register(StorageProfile profile) {
        StorageProfileRevision snapshot = new StorageProfileRevision(profile, UUID.randomUUID().toString(), 1);
        current.put(profile.name(), snapshot);
        history.put(new RevisionKey(profile.name(), snapshot.incarnationId(), 1), snapshot);
    }

    @Override
    public boolean createIfAbsent(StorageProfile profile) {
        StorageProfileRevision snapshot = new StorageProfileRevision(profile, UUID.randomUUID().toString(), 1);
        if (current.putIfAbsent(profile.name(), snapshot) != null) {
            return false;
        }
        history.put(new RevisionKey(profile.name(), snapshot.incarnationId(), 1), snapshot);
        return true;
    }

    @Override
    public boolean unregister(String name) {
        return current.remove(name) != null;
    }

    @Override
    public Optional<StorageProfile> get(String name) {
        return getCurrentRevision(name).map(StorageProfileRevision::profile);
    }

    @Override
    public Optional<StorageProfileRevision> getCurrentRevision(String name) {
        return Optional.ofNullable(current.get(name));
    }

    @Override
    public Optional<StorageProfileRevision> findRevision(
            String name, String incarnationId, long revision) {
        return Optional.ofNullable(history.get(new RevisionKey(name, incarnationId, revision)));
    }

    @Override
    public List<StorageProfile> list() {
        return current.values().stream().map(StorageProfileRevision::profile).toList();
    }

    @Override
    public Optional<StorageProfile> resolveByUrl(String url) {
        StorageProfile bestMatch = null;
        int longestPrefix = -1;
        boolean ambiguous = false;
        for (StorageProfileRevision snapshot : current.values()) {
            StorageProfile profile = snapshot.profile();
            if (profile.matchesUrl(url)) {
                int length = profile.prefix().length();
                if (length > longestPrefix) {
                    bestMatch = profile;
                    longestPrefix = length;
                    ambiguous = false;
                } else if (length == longestPrefix) {
                    ambiguous = true;
                }
            }
        }
        if (ambiguous) {
            throw new FileStorageException("Ambiguous storage profiles match source URL");
        }
        return Optional.ofNullable(bestMatch);
    }

    private record RevisionKey(String name, String incarnationId, long revision) {}
}
