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

/**
 * Persistence boundary for column-level governance tags.
 *
 * <p>Extracted so the metadata service can either keep tags in memory (dev) or
 * durably persist them (production). Previously tags lived only in a
 * {@code ConcurrentHashMap} inside the metadata service and were silently lost
 * on restart — a governance hazard, since masking/classification policies keyed
 * on tags would change after a bounce.</p>
 *
 * <p>The {@code storeKey} is an opaque composite identifier for a specific
 * (catalog, schema, table, column); implementations treat it as a flat key.
 * Implementations must be safe for concurrent use.</p>
 */
public interface ColumnTagStore {

    /**
     * Sets a tag value, returning the previous value (or null if none).
     */
    String put(String storeKey, String tagKey, String value);

    /**
     * Removes a tag, returning the removed value (or null if it was absent).
     */
    String remove(String storeKey, String tagKey);

    /**
     * Returns an immutable snapshot of all tags for the given key
     * (empty if none).
     */
    Map<String, String> list(String storeKey);
}
