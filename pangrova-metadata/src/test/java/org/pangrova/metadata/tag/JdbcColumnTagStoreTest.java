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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Validates P2-3: {@link JdbcColumnTagStore} durably persists column tags so
 * they survive restarts, closing the in-memory-only tag-store gap. Uses in-memory
 * H2 so no external infrastructure is required.
 */
class JdbcColumnTagStoreTest {

    private static final String KEY = "cat|sch|orders|email";

    private DataSource ds;
    private JdbcColumnTagStore store;

    @BeforeEach
    void setUp() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setUrl("jdbc:h2:mem:tags_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        h2.setUser("sa");
        h2.setPassword("");
        ds = h2;
        store = new JdbcColumnTagStore(ds);
    }

    @Test
    void putReturnsPreviousValueAndUpsertsInPlace() {
        assertThat(store.put(KEY, "pii", "true")).isNull();
        assertThat(store.put(KEY, "pii", "false")).isEqualTo("true");
        assertThat(store.list(KEY)).containsExactly(Map.entry("pii", "false"));
    }

    @Test
    void removeReturnsRemovedValueThenNull() {
        store.put(KEY, "classification", "restricted");
        assertThat(store.remove(KEY, "classification")).isEqualTo("restricted");
        assertThat(store.remove(KEY, "classification")).isNull();
        assertThat(store.list(KEY)).isEmpty();
    }

    @Test
    void listIsIsolatedPerStoreKey() {
        store.put(KEY, "pii", "true");
        store.put("cat|sch|orders|name", "pii", "false");
        assertThat(store.list(KEY)).containsExactly(Map.entry("pii", "true"));
        assertThat(store.list("cat|sch|orders|name")).containsExactly(Map.entry("pii", "false"));
    }

    @Test
    void tagsSurviveNewStoreInstanceOnSameDatabase() {
        store.put(KEY, "owner", "data-eng");
        store.put(KEY, "pii", "true");

        // Simulate a coordinator restart: a fresh store over the same database
        // must still observe the previously persisted tags.
        JdbcColumnTagStore restarted = new JdbcColumnTagStore(ds);
        assertThat(restarted.list(KEY))
                .containsOnly(Map.entry("owner", "data-eng"), Map.entry("pii", "true"));
    }

    @Test
    void unknownKeyListsEmpty() {
        assertThat(store.list("cat|sch|nope|col")).isEmpty();
    }
}
