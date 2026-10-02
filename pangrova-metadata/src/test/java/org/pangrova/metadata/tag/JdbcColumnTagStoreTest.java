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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

/**
 * Validates P2-3: {@link JdbcColumnTagStore} durably persists column tags so
 * they survive restarts, closing the in-memory-only tag-store gap.
 */
class JdbcColumnTagStoreTest {

    private static final String KEY = "cat|sch|orders|email";

    private DataSource ds;
    private JdbcColumnTagStore store;

    @BeforeEach
    void setUp() throws Exception {
        ds = SharedPostgresContainer.isolatedDataSource("column_tags");
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

    @Test
    void concurrentCreationIsSerializedAcrossStoreInstances() throws Exception {
        JdbcColumnTagStore peer = new JdbcColumnTagStore(ds);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> {
                start.await();
                return store.put(KEY, "owner", "alpha");
            });
            var second = executor.submit(() -> {
                start.await();
                return peer.put(KEY, "owner", "beta");
            });
            start.countDown();

            String firstPrevious = first.get();
            String secondPrevious = second.get();
            assertThat((firstPrevious == null) ^ (secondPrevious == null)).isTrue();
            assertThat(firstPrevious == null ? secondPrevious : firstPrevious)
                    .isIn("alpha", "beta");
            assertThat(store.list(KEY).get("owner")).isIn("alpha", "beta");
        } finally {
            executor.shutdownNow();
        }
    }
}
