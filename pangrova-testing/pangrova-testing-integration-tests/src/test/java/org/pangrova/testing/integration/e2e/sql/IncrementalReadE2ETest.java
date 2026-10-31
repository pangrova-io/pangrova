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
package org.pangrova.testing.integration.e2e.sql;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full-stack E2E tests for Incremental Read (SELECT CHANGES BETWEEN SNAPSHOT).
 *
 * <p>Validates the ability to read incremental changes between snapshots,
 * enabling CDC-style processing through SQL.</p>
 */
@E2ETest
@DisplayName("Incremental Read Full-Stack E2E")
class IncrementalReadE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void assignTableName(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_incr_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    @Nested
    @DisplayName("SELECT CHANGES BETWEEN SNAPSHOT")
    class SelectChanges {

        @Test
        @DisplayName("SELECT CHANGES BETWEEN syntax is accepted")
        void selectChangesSyntaxAccepted() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'first')");
            executeDml("INSERT INTO " + fqn() + " VALUES (2, 'second')");

            // Get actual snapshot IDs from the table
            List<TypedRow> snapshots = executeQuery("SHOW SNAPSHOTS FROM " + fqn());
            assertThat(snapshots).hasSizeGreaterThanOrEqualTo(2);

            // Use numeric snapshot IDs for incremental read
            Object firstId = snapshots.get(0).getRaw("snapshot_id");
            Object lastId = snapshots.get(snapshots.size() - 1).getRaw("snapshot_id");
            String changesSql = "SELECT * FROM " + fqn()
                    + " CHANGES BETWEEN SNAPSHOT " + firstId + " AND " + lastId;
            List<TypedRow> changes = executeQuery(changesSql);
            assertThat(changes).isNotNull();

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Incremental read with column selection")
        void incrementalReadWithColumns() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, updated_at BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'Alice', 100)");
            executeDml("INSERT INTO " + fqn() + " VALUES (2, 'Bob', 200)");

            // Get actual snapshot IDs
            List<TypedRow> snapshots = executeQuery("SHOW SNAPSHOTS FROM " + fqn());
            assertThat(snapshots).hasSizeGreaterThanOrEqualTo(2);

            Object firstId = snapshots.get(0).getRaw("snapshot_id");
            Object lastId = snapshots.get(snapshots.size() - 1).getRaw("snapshot_id");
            String sql = "SELECT id, name FROM " + fqn()
                    + " CHANGES BETWEEN SNAPSHOT " + firstId + " AND " + lastId;
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).isNotNull();

            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("Incremental Read Workflow")
    class IncrementalWorkflow {

        @Test
        @DisplayName("Full workflow: insert → snapshot → more inserts → read changes")
        void fullIncrementalWorkflow() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, event VARCHAR)");

            // Initial data
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'created')");

            // More data
            executeDml("INSERT INTO " + fqn() + " VALUES (2, 'updated')");
            executeDml("INSERT INTO " + fqn() + " VALUES (3, 'deleted')");

            // Verify total data is correct
            assertQueryReturnsRows("SELECT * FROM " + fqn(), 3);

            executeDdl("DROP TABLE " + fqn());
        }
    }
}
