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
package org.pangrova.metadata.tag;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory {@link ColumnTagStore} for local/dev use.
 *
 * <p>Tags are lost on restart; this is acceptable only outside production. A
 * warning is logged on construction so the ephemeral nature is never silent.</p>
 */
public final class InMemoryColumnTagStore implements ColumnTagStore {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryColumnTagStore.class);

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, String>> tags =
            new ConcurrentHashMap<>();

    public InMemoryColumnTagStore() {
        LOG.warn("Column tag store is in-memory only; all column tags will be lost on restart. "
                + "Configure a JDBC-backed tag store (metadata datasource) for production use.");
    }

    @Override
    public String put(String storeKey, String tagKey, String value) {
        return tags.computeIfAbsent(storeKey, k -> new ConcurrentHashMap<>()).put(tagKey, value);
    }

    @Override
    public String remove(String storeKey, String tagKey) {
        ConcurrentHashMap<String, String> forKey = tags.get(storeKey);
        return forKey == null ? null : forKey.remove(tagKey);
    }

    @Override
    public Map<String, String> list(String storeKey) {
        ConcurrentHashMap<String, String> forKey = tags.get(storeKey);
        return forKey == null ? Map.of() : Map.copyOf(forKey);
    }

    @Override
    public Map<String, Map<String, String>> listAll() {
        Map<String, Map<String, String>> snapshot = new java.util.LinkedHashMap<>();
        tags.forEach((key, value) -> snapshot.put(key, Map.copyOf(value)));
        return Map.copyOf(snapshot);
    }
}
