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
 * Full-stack E2E tests for metadata query statements.
 *
 * <p>Validates SHOW SNAPSHOTS, SHOW FILES, SHOW MANIFESTS,
 * SHOW PARTITIONS through the full pipeline.</p>
 */
@E2ETest
@DisplayName("Metadata Queries Full-Stack E2E")
class MetadataQueriesE2ETest extends PangrovaE2ETestBase {

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
        tableName = "t_meta_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    @Nested
    @DisplayName("SHOW SNAPSHOTS")
    class ShowSnapshots {

        @Test
        @DisplayName("SHOW SNAPSHOTS returns snapshot history")
        void showSnapshots() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'first')");
            executeDml("INSERT INTO " + fqn() + " VALUES (2, 'second')");

            String sql = "SHOW SNAPSHOTS FROM " + fqn();
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).isNotNull();
            // At least one snapshot exists after data inserts
            assertThat(rows.size()).isGreaterThanOrEqualTo(1);

            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("SHOW FILES")
    class ShowFiles {

        @Test
        @DisplayName("SHOW FILES returns data file list")
        void showFiles() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'data')");

            String sql = "SHOW FILES FROM " + fqn();
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).isNotNull();
            assertThat(rows).isNotEmpty();

            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("SHOW MANIFESTS")
    class ShowManifests {

        @Test
        @DisplayName("SHOW MANIFESTS returns manifest list")
        void showManifests() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'data')");

            String sql = "SHOW MANIFESTS FROM " + fqn();
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).isNotNull();

            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("SHOW PARTITIONS")
    class ShowPartitions {

        @Test
        @DisplayName("SHOW PARTITIONS on partitioned table")
        void showPartitions() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, val DOUBLE)"
                    + " PARTITIONED BY (category)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'A', 10.0), (2, 'B', 20.0), (3, 'A', 30.0)");

            String sql = "SHOW PARTITIONS FROM " + fqn();
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).isNotNull();

            executeDdl("DROP TABLE " + fqn());
        }
    }
}
